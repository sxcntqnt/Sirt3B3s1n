// mqtt-consumer/internal.go
//
// All types, client wrappers, and helper functions for the mqtt-consumer.
// Same package as main.go — every symbol is directly accessible.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ─────────────────────────────────────────────────────────────────────────────
// StripedLock — per-vehicle enrichment serialization without O(n) locks
//
// Problem: multiple workers can concurrently process events for the same
// vehicle. Both read the same "last state" from Redis, both compute the same
// (stale) distance, and both overwrite state — leaving non-deterministic
// movement filtering.
//
// Fix: a fixed array of mutexes indexed by hash(vehicleID) % stripes.
// At 100 000 vehicles and 256 stripes, each stripe covers ~400 vehicles —
// low enough collision rate to prevent the race without a per-vehicle
// sync.Map (which would grow without bound).
// ─────────────────────────────────────────────────────────────────────────────

type StripedLock struct {
	locks []sync.Mutex
}

func NewStripedLock(stripes int) *StripedLock {
	return &StripedLock{locks: make([]sync.Mutex, stripes)}
}

func (s *StripedLock) stripe(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % uint32(len(s.locks))
}

func (s *StripedLock) Lock(key string)   { s.locks[s.stripe(key)].Lock() }
func (s *StripedLock) Unlock(key string) { s.locks[s.stripe(key)].Unlock() }

// ─────────────────────────────────────────────────────────────────────────────
// Tracing
// ─────────────────────────────────────────────────────────────────────────────

func setupTracing(serviceName string) (func(context.Context) error, error) {
	endpoint := envOr("JAEGER_ENDPOINT", "http://localhost:4318/v1/traces")

	exp, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", envOr("ENVIRONMENT", "production")),
			attribute.String("instance_id", envOr("INSTANCE_ID", "unknown")),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// MQTT client
// ─────────────────────────────────────────────────────────────────────────────

type MQTTMessage struct {
	Topic   string
	Payload []byte
}

type MQTTClient interface {
	Receive() (MQTTMessage, error)
	Disconnect(quiesce uint)
}

type pahoClient struct {
	inner  mqtt.Client
	msgCh  chan MQTTMessage
	closed bool
	mu     sync.Mutex
}

func initMQTTClient(cfg MQTTConfig) MQTTClient {
	// Buffer sized to absorb broker bursts without blocking paho's goroutines.
	msgCh := make(chan MQTTMessage, 10000)

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://"+cfg.Broker).
		SetClientID("mqtt-consumer-"+envOr("INSTANCE_ID", uuid.NewString())).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30*time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Printf("[mqtt-consumer] connected to broker %s", cfg.Broker)
			for _, topic := range cfg.Topics {
				tok := c.Subscribe(topic, cfg.QoS, func(_ mqtt.Client, m mqtt.Message) {
					// Non-blocking push — drop here rather than blocking paho's goroutine.
					select {
					case msgCh <- MQTTMessage{Topic: m.Topic(), Payload: m.Payload()}:
					default:
						log.Printf("[mqtt-consumer] paho msgCh full — dropping topic=%s", m.Topic())
					}
				})
				if tok.Wait() && tok.Error() != nil {
					log.Printf("[mqtt-consumer] subscribe %s error: %v", topic, tok.Error())
				}
			}
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("[mqtt-consumer] connection lost: %v", err)
		})

	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Printf("[mqtt-consumer] initial connect error: %v (will retry)", tok.Error())
	}

	return &pahoClient{inner: client, msgCh: msgCh}
}

func (p *pahoClient) Receive() (MQTTMessage, error) {
	msg, ok := <-p.msgCh
	if !ok {
		return MQTTMessage{}, fmt.Errorf("mqtt channel closed")
	}
	return msg, nil
}

func (p *pahoClient) Disconnect(quiesce uint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.inner.Disconnect(quiesce)
		close(p.msgCh)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Domain types
// ─────────────────────────────────────────────────────────────────────────────

type RawGPSEvent struct {
	VehicleID         string  `json:"vehicle_id"`
	OrgID             string  `json:"org_id"`
	Latitude          float64 `json:"lat"`
	Longitude         float64 `json:"lon"`
	Altitude          int16   `json:"alt"`
	Speed             int16   `json:"speed"`
	Heading           int16   `json:"heading"`
	HDOP              float32 `json:"hdop"`
	Satellites        int8    `json:"sats"`
	FixStatus         string  `json:"fix"`
	Rain              bool    `json:"rain"`
	EventType         string  `json:"event_type"`
	DeviceTimestampMs int64   `json:"ts"`
	SchemaVersion     int8    `json:"schema_v"`
}

type EnrichedEvent struct {
	// Device fields
	VehicleID       string
	OrgID           string
	Latitude        float64
	Longitude       float64
	Altitude        int16
	Speed           int16
	Heading         int16
	HDOP            float32
	Satellites      int8
	FixStatus       string
	Rain            bool
	EventType       string
	DeviceTimestamp time.Time
	ReceivedAt      time.Time
	SchemaVersion   int8

	// Enriched from Redis
	VehiclePlate string
	RouteID      string
	DriverID     string
	ConductorID  string
	Capacity     int8

	// Movement filter
	MovementFiltered bool
	DistanceFromLast float32
	TimeSinceLast    int32

	// Pipeline identifiers
	TraceID    string
	EventID    string
	RawMessage string
}

// ToMap serialises the event to a Redis stream entry value.
func (e *EnrichedEvent) ToMap() map[string]interface{} {
	payload, _ := json.Marshal(map[string]interface{}{
		"event_id":             e.EventID,
		"trace_id":             e.TraceID,
		"vehicle_id":           e.VehicleID,
		"org_id":               e.OrgID,
		"latitude":             e.Latitude,
		"longitude":            e.Longitude,
		"altitude":             e.Altitude,
		"speed":                e.Speed,
		"heading":              e.Heading,
		"hdop":                 e.HDOP,
		"satellites":           e.Satellites,
		"fix_status":           e.FixStatus,
		"rain":                 e.Rain,
		"event_type":           e.EventType,
		"movement_filtered":    e.MovementFiltered,
		"distance_from_last":   e.DistanceFromLast,
		"time_since_last":      e.TimeSinceLast,
		"vehicle_plate":        e.VehiclePlate,
		"route_id":             e.RouteID,
		"driver_id":            e.DriverID,
		"conductor_id":         e.ConductorID,
		"capacity":             e.Capacity,
		"raw_message":          e.RawMessage,
		"schema_version":       e.SchemaVersion,
		"device_timestamp_ms":  e.DeviceTimestamp.UnixMilli(),
		"received_at_ms":       e.ReceivedAt.UnixMilli(),
	})
	return map[string]interface{}{"payload": string(payload)}
}

// ─────────────────────────────────────────────────────────────────────────────
// Redis
// ─────────────────────────────────────────────────────────────────────────────

func initRedisClusterClient(cfg RedisConfig) *redis.ClusterClient {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    cfg.Nodes,
		Password: cfg.Password,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[mqtt-consumer] redis ping warning: %v", err)
	}
	return client
}

// ─────────────────────────────────────────────────────────────────────────────
// ClickHouse (direct write — critical events only)
//
// FIX: was returning *driver.Conn (pointer to interface = double indirection).
// driver.Conn is an interface; return it directly. Callers hold driver.Conn
// and test for nil interface to check availability.
// ─────────────────────────────────────────────────────────────────────────────

func initClickHouseClient(cfg ClickHouseConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Host},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		MaxOpenConns: 5,
		MaxIdleConns: 2,
		DialTimeout:  10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return conn, nil
}

// writeToClickhouse does a single-row insert for critical events.
// FIX: accepts driver.Conn (not *driver.Conn) and table name from config.
func writeToClickhouse(event EnrichedEvent, conn driver.Conn, table string) error {
	q := fmt.Sprintf(`INSERT INTO %s
		(event_id,trace_id,vehicle_id,organization_id,latitude,longitude,
		 altitude,speed,heading,hdop,satellites,fix_status,rain,event_type,
		 movement_filtered,distance_from_last,time_since_last,vehicle_plate,
		 route_id,driver_id,conductor_id,capacity,raw_message,schema_version,
		 device_timestamp,received_at,processed_at,recorded_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, table)

	now := time.Now().UTC()
	return conn.Exec(context.Background(), q,
		event.EventID, event.TraceID, event.VehicleID, event.OrgID,
		event.Latitude, event.Longitude, event.Altitude, event.Speed,
		event.Heading, event.HDOP, event.Satellites, event.FixStatus,
		event.Rain, event.EventType, event.MovementFiltered,
		event.DistanceFromLast, event.TimeSinceLast, event.VehiclePlate,
		event.RouteID, event.DriverID, event.ConductorID, event.Capacity,
		event.RawMessage, event.SchemaVersion,
		event.DeviceTimestamp, event.ReceivedAt, now, now,
	)
}

// ─────────────────────────────────────────────────────────────────────────────
// Message parsing and enrichment
// ─────────────────────────────────────────────────────────────────────────────

func parseMQTTMessage(msg MQTTMessage) (EnrichedEvent, error) {
	var raw RawGPSEvent
	if err := json.Unmarshal(msg.Payload, &raw); err != nil {
		return EnrichedEvent{}, fmt.Errorf("unmarshal: %w", err)
	}

	// Topic: gps/{orgId}/{vehicleId} — fallback to payload fields.
	parts := strings.SplitN(msg.Topic, "/", 3)
	if len(parts) == 3 {
		if raw.OrgID == "" {
			raw.OrgID = parts[1]
		}
		if raw.VehicleID == "" {
			raw.VehicleID = parts[2]
		}
	}

	if raw.VehicleID == "" || raw.OrgID == "" {
		return EnrichedEvent{}, fmt.Errorf("missing vehicle_id or org_id in topic=%s", msg.Topic)
	}
	if raw.EventType == "" {
		raw.EventType = "NORMAL"
	}

	return EnrichedEvent{
		VehicleID:       raw.VehicleID,
		OrgID:           raw.OrgID,
		Latitude:        raw.Latitude,
		Longitude:       raw.Longitude,
		Altitude:        raw.Altitude,
		Speed:           raw.Speed,
		Heading:         raw.Heading,
		HDOP:            raw.HDOP,
		Satellites:      raw.Satellites,
		FixStatus:       raw.FixStatus,
		Rain:            raw.Rain,
		EventType:       raw.EventType,
		DeviceTimestamp: time.UnixMilli(raw.DeviceTimestampMs).UTC(),
		ReceivedAt:      time.Now().UTC(),
		SchemaVersion:   raw.SchemaVersion,
		RawMessage:      string(msg.Payload),
		EventID:         uuid.NewString(),
		TraceID:         uuid.NewString(),
	}, nil
}

func vehicleMetaKey(vehicleID string) string           { return "vehicle_meta:" + vehicleID }
func lastStateKey(orgID, vehicleID string) string      { return "vehicle:" + orgID + ":" + vehicleID }

// enrichEvent fetches vehicle metadata and last position from Redis in a
// single pipelined round-trip, then writes back the new state in a second
// pipeline.
//
// FIX: the old implementation made 4 sequential Redis calls (HGetAll meta +
// HGetAll state + HSet + Expire). This function reduces that to 2 round-trips:
// one read pipeline and one write pipeline.
//
// Note: with go-redis ClusterClient, Pipeline() automatically shards commands
// across slots, so cross-slot pipelining is safe.
//
// Caller MUST hold StripedLock(vehicleID) before calling — see WorkerPool.
func enrichEvent(event EnrichedEvent, client *redis.ClusterClient) (EnrichedEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	metaKey  := vehicleMetaKey(event.VehicleID)
	stateKey := lastStateKey(event.OrgID, event.VehicleID)

	// ── Read pipeline (2 RTTs → 1) ──────────────────────────────────────────
	pipe := client.Pipeline()
	metaCmd  := pipe.HGetAll(ctx, metaKey)
	stateCmd := pipe.HGetAll(ctx, stateKey)
	_, _ = pipe.Exec(ctx)

	// Apply vehicle metadata.
	if meta := metaCmd.Val(); len(meta) > 0 {
		event.VehiclePlate = meta["plate"]
		event.RouteID      = meta["route_id"]
		event.DriverID     = meta["driver_id"]
		event.ConductorID  = meta["conductor_id"]
		fmt.Sscanf(meta["capacity"], "%d", &event.Capacity)
	}

	// Compute movement filter from last state.
	if state := stateCmd.Val(); len(state) > 0 {
		var lastLat, lastLon float64
		var lastTsMs int64
		fmt.Sscanf(state["lat"], "%f", &lastLat)
		fmt.Sscanf(state["lon"], "%f", &lastLon)
		fmt.Sscanf(state["ts"], "%d", &lastTsMs)

		dist := haversineMeters(lastLat, lastLon, event.Latitude, event.Longitude)
		event.DistanceFromLast = float32(dist)
		event.TimeSinceLast    = int32((event.DeviceTimestamp.UnixMilli() - lastTsMs) / 1000)
	}

	// ── Write pipeline (2 RTTs → 1) ─────────────────────────────────────────
	// 30 s TTL: vehicles that go offline don't hold state forever.
	newState := map[string]interface{}{
		"lat": fmt.Sprintf("%f", event.Latitude),
		"lon": fmt.Sprintf("%f", event.Longitude),
		"ts":  fmt.Sprintf("%d", event.DeviceTimestamp.UnixMilli()),
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer writeCancel()

	writePipe := client.Pipeline()
	writePipe.HSet(writeCtx, stateKey, newState)
	writePipe.Expire(writeCtx, stateKey, 30*time.Second)
	_, _ = writePipe.Exec(writeCtx)

	return event, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Event classification
// ─────────────────────────────────────────────────────────────────────────────

var criticalEventTypes = map[string]bool{
	"PANIC_BUTTON":    true,
	"OVERSPEED":       true,
	"GPS_SIGNAL_LOST": true,
	"GEOFENCE_ENTER":  true,
	"GEOFENCE_EXIT":   true,
}

func isCriticalEvent(event EnrichedEvent) bool {
	return criticalEventTypes[event.EventType]
}

// shouldBroadcast returns true when the event clears the movement filter.
// FIX: was hardcoding 20 m / 5 s. Now uses MovementFilter from config so
// thresholds are tunable without a rebuild.
func shouldBroadcast(event EnrichedEvent, filter MovementFilter) bool {
	return float64(event.DistanceFromLast) >= filter.MinDistanceMeters ||
		int(event.TimeSinceLast) >= filter.MinTimeSeconds
}

// ─────────────────────────────────────────────────────────────────────────────
// Redis stream writers
// ─────────────────────────────────────────────────────────────────────────────

// writeToBatchStream writes an event to the cold-path batch stream.
// FIX: was void and silently discarded errors. Now returns error so the caller
// can track it in metrics.
func writeToBatchStream(event EnrichedEvent, client *redis.ClusterClient) error {
	streamKey := "gps:batch:" + event.OrgID
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 100000,
		Approx: true,
		Values: event.ToMap(),
	}).Err()
}

// writeToRedisStreams writes to the realtime stream (WebSocket gateway) and
// the batch stream (batch-writer). Returns the realtime write error because
// that is what triggers backpressure — batch errors are logged separately.
func writeToRedisStreams(event EnrichedEvent, client *redis.ClusterClient) error {
	realtimeKey := "gps:realtime:" + event.OrgID
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: realtimeKey,
		MaxLen: 5000,
		Approx: true,
		Values: event.ToMap(),
	}).Err(); err != nil {
		return err // triggers backpressure in caller
	}

	if err := writeToBatchStream(event, client); err != nil {
		log.Printf("[mqtt-consumer] batch stream XAdd vehicle=%s: %v", event.VehicleID, err)
		// Not returned — batch errors don't trigger backpressure; the batch-writer
		// has its own retry / DLQ path.
	}

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Haversine distance
// ─────────────────────────────────────────────────────────────────────────────

const earthRadiusM = 6_371_000.0

func haversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := toRad(lat2 - lat1)
	dLon := toRad(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func toRad(deg float64) float64 { return deg * math.Pi / 180 }

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
