// batch-writer/internal.go
//
// All types, client wrappers, and helpers for the batch-writer.
// Same package as main.go — every symbol is directly accessible.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.05))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamRegistry
//
// Redis Streams XREADGROUP does NOT accept glob patterns — every stream key
// must be named explicitly. StreamRegistry solves this by:
//
//   1. Scanning Redis for keys matching the batch prefix (e.g. "gps:batch:*").
//   2. Running XGROUP CREATE … MKSTREAM for any newly-discovered key so the
//      consumer group exists before we try to read from it.
//   3. Periodically re-scanning to pick up new org streams without restart.
//   4. Exposing StreamArgs() which returns the correctly-formatted slice
//      [key1, key2, …, ">", ">", …] that XReadGroup expects.
// ─────────────────────────────────────────────────────────────────────────────

type StreamRegistry struct {
	rdb    *redis.ClusterClient
	cfg    RedisConfig
	mu     sync.RWMutex
	known  map[string]struct{} // keys we have already created a consumer group for
}

func NewStreamRegistry(rdb *redis.ClusterClient, cfg RedisConfig) *StreamRegistry {
	return &StreamRegistry{
		rdb:   rdb,
		cfg:   cfg,
		known: make(map[string]struct{}),
	}
}

// Bootstrap performs an initial SCAN so the first read has streams to consume.
func (r *StreamRegistry) Bootstrap(ctx context.Context) error {
	return r.refresh(ctx)
}

// Run refreshes the registry at cfg.ReadConfig.StreamRefreshInterval until ctx
// is cancelled. Intended to be called in a dedicated goroutine.
func (r *StreamRegistry) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReadConfig.StreamRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.refresh(ctx); err != nil {
				log.Printf("[batch-writer/registry] refresh error: %v", err)
			}
		}
	}
}

// Keys returns a snapshot of all currently-known stream keys.
func (r *StreamRegistry) Keys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.known))
	for k := range r.known {
		keys = append(keys, k)
	}
	return keys
}

// StreamArgs returns the stream-slice expected by XReadGroupArgs:
//
//	[key1, key2, …, ">", ">", …]
//
// Returns nil when no streams are known yet.
func (r *StreamRegistry) StreamArgs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.known) == 0 {
		return nil
	}
	keys := make([]string, 0, len(r.known)*2)
	for k := range r.known {
		keys = append(keys, k)
	}
	for range r.known {
		keys = append(keys, ">")
	}
	return keys
}

// refresh scans Redis for batch stream keys and creates consumer groups for
// any that are new. Safe to call concurrently — it only holds the write lock
// at the very end when updating r.known.
func (r *StreamRegistry) refresh(ctx context.Context) error {
	pattern := r.cfg.Streams.BatchPrefix + "*"
	var found []string

	// SCAN across all cluster nodes (go-redis v9 ClusterClient handles fanout).
	iter := r.rdb.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		found = append(found, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("SCAN %s: %w", pattern, err)
	}

	r.mu.RLock()
	var newKeys []string
	for _, key := range found {
		if _, exists := r.known[key]; !exists {
			newKeys = append(newKeys, key)
		}
	}
	r.mu.RUnlock()

	// Create consumer groups for new keys — MKSTREAM creates the stream if it
	// doesn't exist, $ means "only new messages from now on".
	for _, key := range newKeys {
		err := r.rdb.XGroupCreateMkStream(ctx, key, r.cfg.ConsumerGroup, "$").Err()
		if err != nil && !isGroupExistsErr(err) {
			log.Printf("[batch-writer/registry] XGroupCreateMkStream key=%s: %v", key, err)
			continue // don't add to known — retry next refresh cycle
		}
		log.Printf("[batch-writer/registry] registered stream %s", key)
	}

	if len(newKeys) > 0 {
		r.mu.Lock()
		for _, key := range newKeys {
			r.known[key] = struct{}{}
		}
		r.mu.Unlock()
	}

	return nil
}

// isGroupExistsErr returns true when Redis reports the consumer group already
// exists — this is not an error condition for us.
func isGroupExistsErr(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}

// ─────────────────────────────────────────────────────────────────────────────
// Redis client
// ─────────────────────────────────────────────────────────────────────────────

// RedisMessage is a single entry read from a Redis stream consumer group.
type RedisMessage struct {
	Stream string
	ID     string
	Values map[string]interface{}
	OrgID  string
}

func initRedisClusterClient(cfg RedisConfig) *redis.ClusterClient {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    cfg.Nodes,
		Password: cfg.Password,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[batch-writer] redis ping warning: %v", err)
	}
	return client
}

// ─────────────────────────────────────────────────────────────────────────────
// ClickHouse
// ─────────────────────────────────────────────────────────────────────────────

type ClickHouseWriter struct {
	conn driver.Conn
}

func NewClickHouseWriter(cfg ClickHouseConfig) (*ClickHouseWriter, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Host},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		MaxOpenConns:    cfg.Performance.MaxOpen,
		MaxIdleConns:    cfg.Performance.MaxIdle,
		ConnMaxLifetime: cfg.Performance.MaxLifetime,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &ClickHouseWriter{conn: conn}, nil
}

// InsertBatch writes messages to ClickHouse using explicit column-order Append
// instead of AppendStruct, which avoids runtime failures on older clickhouse-go
// v2 versions that don't implement the Struct scanner interface.
//
// The INSERT lists all columns explicitly so the order here must exactly match
// the column list in the SQL string.
func (w *ClickHouseWriter) InsertBatch(ctx context.Context, table string, messages []ProcessedMessage) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			event_id, trace_id, vehicle_id, organization_id,
			latitude, longitude, altitude, speed, heading, hdop,
			satellites, fix_status, rain, event_type,
			movement_filtered, distance_from_last, time_since_last,
			vehicle_plate, route_id, driver_id, conductor_id, capacity,
			raw_message, schema_version,
			device_timestamp, received_at, processed_at, recorded_at
		)`, table)

	batch, err := w.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("PrepareBatch: %w", err)
	}

	now := time.Now().UTC()
	for _, msg := range messages {
		if err := batch.Append(
			msg.EventID,
			msg.TraceID,
			msg.VehicleID,
			msg.OrgID,
			msg.Latitude,
			msg.Longitude,
			msg.Altitude,
			msg.Speed,
			msg.Heading,
			msg.HDOP,
			msg.Satellites,
			msg.FixStatus,
			msg.Rain,
			msg.EventType,
			msg.MovementFiltered,
			msg.DistanceFromLast,
			msg.TimeSinceLast,
			msg.VehiclePlate,
			msg.RouteID,
			msg.DriverID,
			msg.ConductorID,
			msg.Capacity,
			msg.RawMessage,
			msg.SchemaVersion,
			msg.DeviceTimestamp,
			msg.ReceivedAt,
			now, // processed_at
			now, // recorded_at
		); err != nil {
			// Log the individual row error but continue — a single bad row should
			// not abort the entire batch; the caller decides on DLQ routing.
			log.Printf("[batch-writer] batch.Append vehicle=%s: %v", msg.VehicleID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch.Send: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Domain types
// ─────────────────────────────────────────────────────────────────────────────

// ProcessedMessage is the in-process GPS event representation.
// Stream/RedisID are the Redis provenance, used for XAck.
// Attempts tracks DLQ retry history.
type ProcessedMessage struct {
	Stream   string
	RedisID  string
	Attempts int // incremented on each DLQ retry

	EventID          string
	TraceID          string
	VehicleID        string
	OrgID            string
	Latitude         float64
	Longitude        float64
	Altitude         int16
	Speed            int16
	Heading          int16
	HDOP             float32
	Satellites       int8
	FixStatus        string
	Rain             bool
	EventType        string
	MovementFiltered bool
	DistanceFromLast float32
	TimeSinceLast    int32
	VehiclePlate     string
	RouteID          string
	DriverID         string
	ConductorID      string
	Capacity         int8
	RawMessage       string
	SchemaVersion    int8
	DeviceTimestamp  time.Time
	ReceivedAt       time.Time
}

// wireEvent is the JSON payload stored in each Redis stream entry.
// Tags match field names written by mqtt-consumer.
type wireEvent struct {
	EventID          string  `json:"event_id"`
	TraceID          string  `json:"trace_id"`
	VehicleID        string  `json:"vehicle_id"`
	OrgID            string  `json:"org_id"`
	Latitude         float64 `json:"latitude"`
	Longitude        float64 `json:"longitude"`
	Altitude         int16   `json:"altitude"`
	Speed            int16   `json:"speed"`
	Heading          int16   `json:"heading"`
	HDOP             float32 `json:"hdop"`
	Satellites       int8    `json:"satellites"`
	FixStatus        string  `json:"fix_status"`
	Rain             bool    `json:"rain"`
	EventType        string  `json:"event_type"`
	MovementFiltered bool    `json:"movement_filtered"`
	DistanceFromLast float32 `json:"distance_from_last"`
	TimeSinceLast    int32   `json:"time_since_last"`
	VehiclePlate     string  `json:"vehicle_plate"`
	RouteID          string  `json:"route_id"`
	DriverID         string  `json:"driver_id"`
	ConductorID      string  `json:"conductor_id"`
	Capacity         int8    `json:"capacity"`
	RawMessage       string  `json:"raw_message"`
	SchemaVersion    int8    `json:"schema_version"`
	DeviceTimestamp  int64   `json:"device_timestamp_ms"`
	ReceivedAt       int64   `json:"received_at_ms"`
}

func deserialiseMessage(msg RedisMessage) (ProcessedMessage, error) {
	payloadRaw, ok := msg.Values["payload"]
	if !ok {
		return ProcessedMessage{}, fmt.Errorf("missing 'payload' field in entry %s", msg.ID)
	}
	payloadStr, ok := payloadRaw.(string)
	if !ok {
		return ProcessedMessage{}, fmt.Errorf("payload not a string in entry %s", msg.ID)
	}

	var we wireEvent
	if err := json.Unmarshal([]byte(payloadStr), &we); err != nil {
		return ProcessedMessage{}, fmt.Errorf("json unmarshal entry %s: %w", msg.ID, err)
	}
	if we.EventID == "" {
		we.EventID = uuid.NewString()
	}

	return ProcessedMessage{
		Stream:           msg.Stream,
		RedisID:          msg.ID,
		EventID:          we.EventID,
		TraceID:          we.TraceID,
		VehicleID:        we.VehicleID,
		OrgID:            we.OrgID,
		Latitude:         we.Latitude,
		Longitude:        we.Longitude,
		Altitude:         we.Altitude,
		Speed:            we.Speed,
		Heading:          we.Heading,
		HDOP:             we.HDOP,
		Satellites:       we.Satellites,
		FixStatus:        we.FixStatus,
		Rain:             we.Rain,
		EventType:        we.EventType,
		MovementFiltered: we.MovementFiltered,
		DistanceFromLast: we.DistanceFromLast,
		TimeSinceLast:    we.TimeSinceLast,
		VehiclePlate:     we.VehiclePlate,
		RouteID:          we.RouteID,
		DriverID:         we.DriverID,
		ConductorID:      we.ConductorID,
		Capacity:         we.Capacity,
		RawMessage:       we.RawMessage,
		SchemaVersion:    we.SchemaVersion,
		DeviceTimestamp:  time.UnixMilli(we.DeviceTimestamp).UTC(),
		ReceivedAt:       time.UnixMilli(we.ReceivedAt).UTC(),
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// DLQ Processor
//
// Lifecycle:
//   ingestLoop  — drains msgChan, writes entries to gps:dlq:{orgId} streams.
//   retryLoop   — periodically reads from DLQ streams, re-attempts ClickHouse
//                 insert, XAcks on success; increments attempts and re-queues
//                 on failure; drops and XAcks after MaxRetries.
//
// dlqDepth gauge is backed by metrics.dlqDepthCount (atomic.Int64) so we can
// call Set() accurately without racy Add-only increments.
// ─────────────────────────────────────────────────────────────────────────────

type DLQProcessor struct {
	cfgHolder   *ConfigHolder
	redisClient *redis.ClusterClient
	chWriter    *ClickHouseWriter
	metrics     *BatchWriterMetrics

	msgChan  chan []ProcessedMessage
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewDLQProcessor(
	cfgHolder *ConfigHolder,
	rdb *redis.ClusterClient,
	chWriter *ClickHouseWriter,
	metrics *BatchWriterMetrics,
) *DLQProcessor {
	return &DLQProcessor{
		cfgHolder:   cfgHolder,
		redisClient: rdb,
		chWriter:    chWriter,
		metrics:     metrics,
		msgChan:     make(chan []ProcessedMessage, 256),
		stopChan:    make(chan struct{}),
	}
}

// AddMessages enqueues messages for DLQ ingestion (non-blocking).
func (d *DLQProcessor) AddMessages(messages []ProcessedMessage) {
	select {
	case d.msgChan <- messages:
	default:
		log.Printf("[batch-writer/dlq] channel full — dropping %d messages", len(messages))
	}
}

func (d *DLQProcessor) Start() {
	d.wg.Add(2)
	go d.ingestLoop()
	go d.retryLoop()
}

func (d *DLQProcessor) Stop() {
	close(d.stopChan)
	d.wg.Wait()
}

// ingestLoop writes failed messages to their per-org DLQ stream.
// It also maintains dlqDepthCount so the gauge reflects actual DLQ depth.
func (d *DLQProcessor) ingestLoop() {
	defer d.wg.Done()
	for {
		select {
		case <-d.stopChan:
			return
		case msgs := <-d.msgChan:
			cfg := d.cfgHolder.Get()
			for _, msg := range msgs {
				payload, _ := json.Marshal(msg)
				streamKey := cfg.Redis.Streams.DLQPrefix + msg.OrgID
				err := d.redisClient.XAdd(context.Background(), &redis.XAddArgs{
					Stream: streamKey,
					MaxLen: int64(cfg.DLQ.BatchSize * 20),
					Approx: true,
					Values: map[string]interface{}{
						"payload":  string(payload),
						"attempts": msg.Attempts + 1,
					},
				}).Err()
				if err != nil {
					log.Printf("[batch-writer/dlq] XAdd stream=%s: %v", streamKey, err)
					continue
				}
				d.metrics.dlqIngested.Inc()
				n := d.metrics.dlqDepthCount.Add(1)
				d.metrics.dlqDepth.Set(float64(n))
			}
		}
	}
}

// retryLoop periodically reads from DLQ streams and retries ClickHouse inserts.
//
// Per-message outcomes:
//   - Success          → XAck from DLQ stream, decrement depth counter, inc dlqRetried
//   - Failure, attempts < MaxRetries → XAck + re-push with Attempts+1 via AddMessages
//   - Failure, attempts >= MaxRetries → XAck (remove from DLQ), inc dlqDropped, log
//
// Using XAck-then-re-push (rather than leaving un-acked) avoids the PEL growing
// unboundedly when consumers restart. The attempt counter in the payload is the
// retry ledger.
func (d *DLQProcessor) retryLoop() {
	defer d.wg.Done()
	cfg := d.cfgHolder.Get()
	ticker := time.NewTicker(cfg.DLQ.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopChan:
			return
		case <-ticker.C:
			d.processDLQBatch()
		}
	}
}

func (d *DLQProcessor) processDLQBatch() {
	cfg := d.cfgHolder.Get()
	ctx := context.Background()

	// Discover DLQ streams the same way StreamRegistry discovers batch streams.
	pattern := cfg.Redis.Streams.DLQPrefix + "*"
	dlqStreams := d.scanDLQStreams(ctx, pattern)
	if len(dlqStreams) == 0 {
		return
	}

	// Ensure consumer group exists for each DLQ stream.
	dlqGroup := cfg.Redis.ConsumerGroup + "-dlq"
	for _, stream := range dlqStreams {
		err := d.redisClient.XGroupCreateMkStream(ctx, stream, dlqGroup, "0").Err()
		if err != nil && !isGroupExistsErr(err) {
			log.Printf("[batch-writer/dlq] XGroupCreateMkStream stream=%s: %v", stream, err)
		}
	}

	// Build XReadGroup args for all DLQ streams.
	streamArgs := make([]string, 0, len(dlqStreams)*2)
	streamArgs = append(streamArgs, dlqStreams...)
	for range dlqStreams {
		streamArgs = append(streamArgs, ">")
	}

	result, err := d.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    dlqGroup,
		Consumer: cfg.Redis.ConsumerName + "-dlq",
		Streams:  streamArgs,
		Count:    int64(cfg.DLQ.BatchSize),
		Block:    0, // non-blocking: return immediately if no messages
		NoAck:    false,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			log.Printf("[batch-writer/dlq] XReadGroup: %v", err)
		}
		return
	}

	for _, sr := range result {
		for _, m := range sr.Messages {
			d.retryOne(ctx, sr.Stream, m, dlqGroup, cfg)
		}
	}
}

func (d *DLQProcessor) retryOne(
	ctx context.Context,
	stream string,
	m redis.XMessage,
	group string,
	cfg *Config,
) {
	// Deserialise the DLQ entry back into a ProcessedMessage.
	payloadRaw, ok := m.Values["payload"]
	if !ok {
		log.Printf("[batch-writer/dlq] entry %s missing payload — dropping", m.ID)
		d.ackDLQ(ctx, stream, group, m.ID)
		d.decrementDepth()
		return
	}
	payloadStr, _ := payloadRaw.(string)

	var msg ProcessedMessage
	if err := json.Unmarshal([]byte(payloadStr), &msg); err != nil {
		log.Printf("[batch-writer/dlq] unmarshal entry %s: %v — dropping", m.ID, err)
		d.ackDLQ(ctx, stream, group, m.ID)
		d.decrementDepth()
		return
	}

	// Attach the DLQ stream provenance so ackDLQ can target the right stream.
	msg.Stream = stream
	msg.RedisID = m.ID

	// Attempt ClickHouse insert.
	err := d.chWriter.InsertBatch(ctx, cfg.ClickHouse.Table, []ProcessedMessage{msg})

	// Always XAck the DLQ entry first — whether we succeed or re-queue.
	// This keeps the PEL clean regardless of outcome.
	d.ackDLQ(ctx, stream, group, m.ID)
	d.decrementDepth()

	if err == nil {
		d.metrics.dlqRetried.Inc()
		return
	}

	// Insert failed.
	msg.Attempts++
	if msg.Attempts >= cfg.DLQ.MaxRetries {
		log.Printf("[batch-writer/dlq] permanently dropping vehicle=%s event=%s after %d attempts: %v",
			msg.VehicleID, msg.EventID, msg.Attempts, err)
		d.metrics.dlqDropped.Inc()
		return
	}

	// Re-queue with incremented attempt counter.
	log.Printf("[batch-writer/dlq] re-queuing vehicle=%s event=%s attempt=%d: %v",
		msg.VehicleID, msg.EventID, msg.Attempts, err)
	d.AddMessages([]ProcessedMessage{msg})
}

func (d *DLQProcessor) ackDLQ(ctx context.Context, stream, group, id string) {
	if err := d.redisClient.XAck(ctx, stream, group, id).Err(); err != nil {
		log.Printf("[batch-writer/dlq] XAck stream=%s id=%s: %v", stream, id, err)
	}
}

func (d *DLQProcessor) decrementDepth() {
	n := d.metrics.dlqDepthCount.Add(-1)
	if n < 0 {
		// Guard against underflow if a message was counted before the atomic was initialised.
		d.metrics.dlqDepthCount.Store(0)
		n = 0
	}
	d.metrics.dlqDepth.Set(float64(n))
}

func (d *DLQProcessor) scanDLQStreams(ctx context.Context, pattern string) []string {
	var keys []string
	iter := d.redisClient.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		log.Printf("[batch-writer/dlq] SCAN %s: %v", pattern, err)
	}
	return keys
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// extractOrgID returns the orgId suffix from a stream key given its prefix.
// e.g. extractOrgID("gps:batch:org-42", "gps:batch:") → "org-42"
func extractOrgID(stream, prefix string) string {
	return strings.TrimPrefix(stream, prefix)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
