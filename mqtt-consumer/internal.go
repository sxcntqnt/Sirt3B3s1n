// mqtt-consumer/internal.go
// Defines all types, client wrappers, and helper functions used by main.go.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
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
// MQTT types and client
// ─────────────────────────────────────────────────────────────────────────────

// MQTTMessage carries the topic and raw payload of a single MQTT publish.
type MQTTMessage struct {
	Topic   string
	Payload []byte
}

// MQTTClient is the interface WorkerPool uses — thin enough to be mocked in tests.
type MQTTClient interface {
	Receive() (MQTTMessage, error)
	Disconnect(quiesce uint)
}

// pahoClient adapts paho.mqtt.golang to MQTTClient using a buffered channel.
type pahoClient struct {
	inner  mqtt.Client
	msgCh  chan MQTTMessage
	closed bool
	mu     sync.Mutex
}

func initMQTTClient(cfg MQTTConfig) MQTTClient {
	msgCh := make(chan MQTTMessage, 10000)

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + cfg.Broker).
		SetClientID("mqtt-consumer-" + envOr("INSTANCE_ID", uuid.NewString())).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Printf("[mqtt-consumer] connected to broker %s", cfg.Broker)
			for _, topic := range cfg.Topics {
				if tok := c.Subscribe(topic, cfg.QoS, func(_ mqtt.Client, m mqtt.Message) {
					msgCh <- MQTTMessage{Topic: m.Topic(), Payload: m.Payload()}
				}); tok.Wait() && tok.Error() != nil {
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
// GPS event types
// ─────────────────────────────────────────────────────────────────────────────

// RawGPSEvent is the JSON payload published by a GPS device to MQTT.
// Topic format: gps/{orgId}/{vehicleId}
type RawGPSEvent struct {
	VehicleID       string  `json:"vehicle_id"`
	OrgID           string  `json:"org_id"`
	Latitude        float64 `json:"lat"`
	Longitude       float64 `json:"lon"`
	Altitude        int16   `json:"alt"`
	Speed           int16   `json:"speed"`
	Heading         int16   `json:"heading"`
	HDOP            float32 `json:"hdop"`
	Satellites      int8    `json:"sats"`
	FixStatus       string  `json:"fix"`
	Rain            bool    `json:"rain"`
	EventType       string  `json:"event_type"`
	DeviceTimestampMs int64 `json:"ts"`
	SchemaVersion   int8    `json:"schema_v"`
}

// EnrichedEvent is a RawGPSEvent annotated with vehicle metadata fetched from
// the Redis hash and movement-filter state computed from the previous event.
type EnrichedEvent struct {
	// From device
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

	// Pipeline
	TraceID string
	EventID string
	RawMessage string
}

// ToMap serialises the event to a map suitable for redis.XAddArgs.Values.
// All values are strings because Redis stream fields are stored as strings.
func (e *EnrichedEvent) ToMap() map[string]interface{} {
	payload, _ := json.Marshal(map[string]interface{}{
		"event_id":          e.EventID,
		"trace_id":          e.TraceID,
		"vehicle_id":        e.VehicleID,
		"org_id":            e.OrgID,
		"latitude":          e.Latitude,
		"longitude":         e.Longitude,
		"altitude":          e.Altitude,
		"speed":             e.Speed,
		"heading":           e.Heading,
		"hdop":              e.HDOP,
		"satellites":        e.Satellites,
		"fix_status":        e.FixStatus,
		"rain":              e.Rain,
		"event_type":        e.EventType,
		"movement_filtered": e.MovementFiltered,
		"distance_from_last": e.DistanceFromLast,
		"time_since_last":   e.TimeSinceLast,
		"vehicle_plate":     e.VehiclePlate,
		"route_id":          e.RouteID,
		"driver_id":         e.DriverID,
		"conductor_id":      e.ConductorID,
		"capacity":          e.Capacity,
		"raw_message":       e.RawMessage,
		"schema_version":    e.SchemaVersion,
		"device_timestamp_ms": e.DeviceTimestamp.UnixMilli(),
		"received_at_ms":    e.ReceivedAt.UnixMilli(),
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
// ClickHouse (direct write path — critical events only)
// ─────────────────────────────────────────────────────────────────────────────

func initClickHouseClient(cfg ClickHouseConfig) (*driver.Conn, error) {
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
	return &conn, nil
}

// writeToClickhouse does a single-row insert for critical events that must
// bypass the batch path and land in ClickHouse immediately.
func writeToClickhouse(event EnrichedEvent, conn *driver.Conn) error {
	const q = `INSERT INTO gps_events
		(event_id,trace_id,vehicle_id,organization_id,latitude,longitude,
		 altitude,speed,heading,hdop,satellites,fix_status,rain,event_type,
		 movement_filtered,distance_from_last,time_since_last,vehicle_plate,
		 route_id,driver_id,conductor_id,capacity,raw_message,schema_version,
		 device_timestamp,received_at,processed_at,recorded_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

	now := time.Now().UTC()
	return (*conn).Exec(context.Background(), q,
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

	// Topic format: gps/{orgId}/{vehicleId} — fallback to payload fields.
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

// vehicleMetaKey is the Redis hash key for vehicle metadata.
func vehicleMetaKey(vehicleID string) string {
	return "vehicle_meta:" + vehicleID
}

// lastStateKey is the Redis hash key for the previous GPS position of a vehicle.
func lastStateKey(orgID, vehicleID string) string {
	return fmt.Sprintf("vehicle:%s:%s", orgID, vehicleID)
}

// enrichEvent looks up vehicle metadata and last-known-position from Redis
// to compute the movement filter fields.
func enrichEvent(event EnrichedEvent, client *redis.ClusterClient) (EnrichedEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Fetch vehicle metadata (plate, route, driver, etc.)
	metaKey := vehicleMetaKey(event.VehicleID)
	meta, err := client.HGetAll(ctx, metaKey).Result()
	if err == nil && len(meta) > 0 {
		event.VehiclePlate = meta["plate"]
		event.RouteID = meta["route_id"]
		event.DriverID = meta["driver_id"]
		event.ConductorID = meta["conductor_id"]
		if cap, ok := meta["capacity"]; ok {
			fmt.Sscanf(cap, "%d", &event.Capacity)
		}
	}

	// Fetch last state for movement filter computation.
	stateKey := lastStateKey(event.OrgID, event.VehicleID)
	state, err := client.HGetAll(ctx, stateKey).Result()
	if err == nil && len(state) > 0 {
		var lastLat, lastLon float64
		var lastTsMs int64
		fmt.Sscanf(state["lat"], "%f", &lastLat)
		fmt.Sscanf(state["lon"], "%f", &lastLon)
		fmt.Sscanf(state["ts"], "%d", &lastTsMs)

		dist := haversineMeters(lastLat, lastLon, event.Latitude, event.Longitude)
		event.DistanceFromLast = float32(dist)
		event.TimeSinceLast = int32(event.DeviceTimestamp.UnixMilli()-lastTsMs) / 1000
	}

	// Write new state back (fire-and-forget, 30 s TTL per architecture spec).
	newState := map[string]interface{}{
		"lat": fmt.Sprintf("%f", event.Latitude),
		"lon": fmt.Sprintf("%f", event.Longitude),
		"ts":  fmt.Sprintf("%d", event.DeviceTimestamp.UnixMilli()),
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer writeCancel()
	_ = client.HSet(writeCtx, stateKey, newState).Err()
	_ = client.Expire(writeCtx, stateKey, 30*time.Second).Err()

	return event, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Movement filter and event classification
// ─────────────────────────────────────────────────────────────────────────────

var criticalEventTypes = map[string]bool{
	"PANIC_BUTTON":   true,
	"OVERSPEED":      true,
	"GPS_SIGNAL_LOST": true,
	"GEOFENCE_ENTER": true,
	"GEOFENCE_EXIT":  true,
}

func isCriticalEvent(event EnrichedEvent) bool {
	return criticalEventTypes[event.EventType]
}

// shouldBroadcast returns true when the event clears the movement filter
// thresholds (20 m displacement OR 5 s since last broadcast).
func shouldBroadcast(event EnrichedEvent) bool {
	const (
		minDistM  = 20
		minTimeSec = 5
	)
	return event.DistanceFromLast >= minDistM || event.TimeSinceLast >= minTimeSec
}

// ─────────────────────────────────────────────────────────────────────────────
// Redis stream writers
// ─────────────────────────────────────────────────────────────────────────────

// writeToBatchStream writes an event to the cold-path batch stream for
// persistence to ClickHouse via the batch-writer service.
func writeToBatchStream(event EnrichedEvent, client *redis.ClusterClient) {
	streamKey := fmt.Sprintf("gps:batch:%s", event.OrgID)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 100000,
		Approx: true,
		Values: event.ToMap(),
	}).Err()
	if err != nil {
		log.Printf("[mqtt-consumer] batch stream XAdd error vehicle=%s: %v", event.VehicleID, err)
	}
}

// writeToRedisStreams writes to both the realtime stream (WebSocket gateway)
// and the batch stream (batch-writer). Returns the realtime write error
// because that is what triggers backpressure.
func writeToRedisStreams(event EnrichedEvent, client *redis.ClusterClient) error {
	// Realtime stream — WebSocket gateway consumers.
	realtimeKey := fmt.Sprintf("gps:realtime:%s", event.OrgID)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: realtimeKey,
		MaxLen: 5000,
		Approx: true,
		Values: event.ToMap(),
	}).Err(); err != nil {
		return err
	}

	// Batch stream — written unconditionally for durability.
	writeToBatchStream(event, client)
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
