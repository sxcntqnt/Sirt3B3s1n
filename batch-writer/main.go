// batch-writer/main.go
//
// Fixes applied (scalability audit):
//  1. [CRASH] NewBatchWriterMetrics: initialise all 7 Prometheus fields before
//     MustRegister — dlqSize, clickhouseErrors, redisReadErrors, batchSize were
//     nil, causing an immediate panic.
//  2. [COMPILE] osGetenv → os.Getenv in loadConfig.
//  3. [LOGIC] BatchProcessor.config changed from ProcessingConfig to the full
//     Config so readBatchFromRedis can reach config.Redis.ConsumerGroup and
//     config.Redis.ReadConfig.BatchSize. NewBatchProcessor signature updated.
//  4. [SCALE] startMetricsServer now reads METRICS_PORT from the environment
//     (injected by the orchestrator as 9200, 9201 … per replica) instead of
//     hard-coding 9090, which caused "address already in use" on replica 1+.
//     INSTANCE_ID is also used as the Redis consumer name for uniqueness.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	Redis      RedisConfig
	ClickHouse ClickHouseConfig
	Processing ProcessingConfig
	DLQ        DLQConfig
	Metrics    MetricsConfig
}

type RedisConfig struct {
	Cluster       bool
	Nodes         []string
	Password      string
	ConsumerGroup string
	// ConsumerName is set at runtime from INSTANCE_ID so every replica in the
	// consumer group has a unique identity. Without this, two replicas share a
	// name and steal each other's messages (audit bug 4 via orchestrator).
	ConsumerName string
	Streams      RedisStreamConfig
	ReadConfig   RedisReadConfig
}

type RedisStreamConfig struct {
	Batch string
	DLQ   string
}

type RedisReadConfig struct {
	BatchSize int64
	BlockTime time.Duration
}

type ClickHouseConfig struct {
	Host         string
	Username     string
	Password     string
	Database     string
	Table        string
	Optimization ClickHouseOptimization
	Performance  ClickHousePerformance
}

type ClickHouseOptimization struct {
	BatchSize       int
	MaxRowsPerBatch int
	FlushInterval   time.Duration
	ParallelWrites  int
	Compression     string
}

type ClickHousePerformance struct {
	MaxRetries   int
	RetryBackoff time.Duration
	ConnectionPool ConnectionPool
}

type ConnectionPool struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
}

type ProcessingConfig struct {
	Workers    int
	BufferSize int
	BatchProcessing BatchProcessing
}

type BatchProcessing struct {
	MaxBatchRows      int
	MaxBatchSizeBytes int
	OrgBatchSize      int
	TimeBatchWindow   time.Duration
}

type DLQConfig struct {
	MaxRetries    int
	RetryInterval time.Duration
	MaxAgeDays    int
	Processing    DLQProcessing
}

type DLQProcessing struct {
	Workers   int
	BatchSize int
	Interval  time.Duration
}

type MetricsConfig struct {
	Port int
	Path string
}

// ─────────────────────────────────────────────────────────────────────────────
// Config loader
// ─────────────────────────────────────────────────────────────────────────────

func loadConfig() Config {
	// INSTANCE_ID is injected by the orchestrator (e.g. "batch-writer-0").
	// Used as the Redis consumer name so each replica is distinct within the
	// consumer group — prevents message theft between siblings.
	instanceID := envOr("INSTANCE_ID", "batch-writer-0")

	// METRICS_PORT is injected by the orchestrator as BaseMetricsPort+replica
	// (9200, 9201, …). Fallback to 9090 for standalone runs.
	metricsPort := envInt("METRICS_PORT", 9090)

	return Config{
		Redis: RedisConfig{
			Cluster:       true,
			Nodes:         []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password:      os.Getenv("REDIS_PASSWORD"), // FIX: was osGetenv (undefined)
			ConsumerGroup: "batch-writers",
			ConsumerName:  instanceID,                  // FIX: unique per replica
			Streams: RedisStreamConfig{
				Batch: "gps:batch:{orgId}",
				DLQ:   "gps:dlq:{orgId}",
			},
			ReadConfig: RedisReadConfig{
				BatchSize: 10000,
				BlockTime: 5 * time.Second,
			},
		},
		ClickHouse: ClickHouseConfig{
			Host:     os.Getenv("CLICKHOUSE_HOST"),
			Username: os.Getenv("CLICKHOUSE_USERNAME"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			Database: "default",
			Table:    "gps_events",
			Optimization: ClickHouseOptimization{
				BatchSize:       10000,
				MaxRowsPerBatch: 100000,
				FlushInterval:   5 * time.Second,
				ParallelWrites:  4,
				Compression:     "lz4",
			},
			Performance: ClickHousePerformance{
				MaxRetries:   3,
				RetryBackoff: 1 * time.Second,
				ConnectionPool: ConnectionPool{
					MaxOpen:     20,
					MaxIdle:     10,
					MaxLifetime: 5 * time.Minute,
				},
			},
		},
		Processing: ProcessingConfig{
			Workers:    4,
			BufferSize: 50000,
			BatchProcessing: BatchProcessing{
				MaxBatchRows:      10000,
				MaxBatchSizeBytes: 10485760,
				OrgBatchSize:      1000,
				TimeBatchWindow:   5 * time.Second,
			},
		},
		DLQ: DLQConfig{
			MaxRetries:    3,
			RetryInterval: 60 * time.Second,
			MaxAgeDays:    7,
			Processing: DLQProcessing{
				Workers:   2,
				BatchSize: 1000,
				Interval:  300 * time.Second,
			},
		},
		Metrics: MetricsConfig{
			Port: metricsPort, // FIX: from env, not hard-coded 9090
			Path: "/metrics",
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Tracing
// ─────────────────────────────────────────────────────────────────────────────

func setupTracing(serviceName string) (trace.TracerProvider, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(
		jaeger.WithEndpoint(os.Getenv("JAEGER_ENDPOINT")),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", "production"),
			attribute.String("instance_id", envOr("INSTANCE_ID", "unknown")),
		)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.05))),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
// FIX: all 7 fields are now initialised before MustRegister is called.
// Previously dlqSize, clickhouseErrors, redisReadErrors, batchSize were nil,
// causing a panic at MustRegister on startup.
// ─────────────────────────────────────────────────────────────────────────────

type BatchWriterMetrics struct {
	redisStreamLag     prometheus.Gauge
	batchWriteDuration prometheus.Histogram
	rowsInserted       prometheus.Counter
	dlqSize            prometheus.Gauge
	clickhouseErrors   prometheus.Counter
	redisReadErrors    prometheus.Counter
	batchSize          prometheus.Histogram
}

func NewBatchWriterMetrics() *BatchWriterMetrics {
	m := &BatchWriterMetrics{
		redisStreamLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_writer_redis_stream_lag",
			Help: "Number of messages lagging in Redis streams",
		}),
		batchWriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_writer_batch_write_duration_ms",
			Help:    "Batch write duration to ClickHouse in milliseconds",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000},
		}),
		rowsInserted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_rows_inserted_total",
			Help: "Total number of rows inserted to ClickHouse",
		}),
		// FIX: these four were uninitialised (nil) in the original, causing
		// MustRegister to panic immediately on startup.
		dlqSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_writer_dlq_size",
			Help: "Number of messages currently in the dead-letter queue",
		}),
		clickhouseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_clickhouse_errors_total",
			Help: "Total number of ClickHouse write errors",
		}),
		redisReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_redis_read_errors_total",
			Help: "Total number of Redis read errors",
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_writer_batch_size",
			Help:    "Number of rows per ClickHouse batch",
			Buckets: []float64{100, 500, 1000, 2500, 5000, 10000, 50000},
		}),
	}

	prometheus.MustRegister(
		m.redisStreamLag,
		m.batchWriteDuration,
		m.rowsInserted,
		m.dlqSize,
		m.clickhouseErrors,
		m.redisReadErrors,
		m.batchSize,
	)

	return m
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch Processor
// FIX: config field changed from ProcessingConfig to the full Config struct.
// readBatchFromRedis was referencing p.config.Redis.ConsumerGroup which does
// not exist on ProcessingConfig — the Redis config was entirely unreachable.
// NewBatchProcessor now accepts Config and stores it in full.
// ─────────────────────────────────────────────────────────────────────────────

type BatchProcessor struct {
	redisClient  *redis.ClusterClient
	chWriter     *ClickHouseWriter
	dlqProcessor *DLQProcessor
	metrics      *BatchWriterMetrics
	config       Config // FIX: was ProcessingConfig
	stopChan     chan struct{}
}

func NewBatchProcessor(
	config Config, // FIX: was ProcessingConfig
	redisClient *redis.ClusterClient,
	chWriter *ClickHouseWriter,
	dlqProcessor *DLQProcessor,
	metrics *BatchWriterMetrics,
) *BatchProcessor {
	return &BatchProcessor{
		redisClient:  redisClient,
		chWriter:     chWriter,
		dlqProcessor: dlqProcessor,
		metrics:      metrics,
		config:       config,
		stopChan:     make(chan struct{}),
	}
}

func (p *BatchProcessor) Start() {
	go p.processBatchLoop()
}

func (p *BatchProcessor) Stop() {
	close(p.stopChan)
}

func (p *BatchProcessor) processBatchLoop() {
	for {
		select {
		case <-p.stopChan:
			return
		default:
		}

		messages, err := p.readBatchFromRedis()
		if err != nil {
			p.metrics.redisReadErrors.Inc()
			time.Sleep(1 * time.Second)
			continue
		}

		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		startTime := time.Now()
		processedMessages, failedMessages := p.processBatch(messages)

		if len(processedMessages) > 0 {
			if err = p.writeToClickhouse(processedMessages); err != nil {
				p.metrics.clickhouseErrors.Inc()
				p.dlqProcessor.AddMessages(processedMessages)
			} else {
				p.ackRedisMessages(processedMessages)
				p.metrics.rowsInserted.Add(float64(len(processedMessages)))
				p.metrics.batchWriteDuration.Observe(float64(time.Since(startTime).Milliseconds()))
			}
		}

		if len(failedMessages) > 0 {
			p.dlqProcessor.AddMessages(failedMessages)
		}
	}
}

func (p *BatchProcessor) readBatchFromRedis() ([]RedisMessage, error) {
	// FIX: p.config is now the full Config, so p.config.Redis.* is reachable.
	streamArgs := redis.XReadGroupArgs{
		Group:    p.config.Redis.ConsumerGroup,
		Consumer: p.config.Redis.ConsumerName,
		Streams:  []string{"gps:batch:*", ">"},
		Count:    p.config.Redis.ReadConfig.BatchSize,
		Block:    p.config.Redis.ReadConfig.BlockTime,
		NoAck:    false,
	}

	result, err := p.redisClient.XReadGroup(context.Background(), &streamArgs).Result()
	if err != nil {
		if err == redis.Nil {
			return []RedisMessage{}, nil
		}
		return nil, err
	}

	// Update stream lag metric.
	p.metrics.redisStreamLag.Set(float64(len(result)))

	var messages []RedisMessage
	for _, streamResult := range result {
		for _, message := range streamResult.Messages {
			messages = append(messages, RedisMessage{
				Stream: streamResult.Stream,
				ID:     message.ID,
				Values: message.Values,
				OrgID:  extractOrgID(streamResult.Stream),
			})
		}
	}
	return messages, nil
}

func (p *BatchProcessor) writeToClickhouse(messages []ProcessedMessage) error {
	batch, err := p.chWriter.PrepareBatch(
		context.Background(),
		fmt.Sprintf("INSERT INTO %s", p.config.ClickHouse.Table),
	)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		row := ClickHouseRow{
			EventID:          msg.EventID,
			TraceID:          msg.TraceID,
			VehicleID:        msg.VehicleID,
			OrganizationID:   msg.OrgID,
			Latitude:         msg.Latitude,
			Longitude:        msg.Longitude,
			Altitude:         msg.Altitude,
			Speed:            msg.Speed,
			Heading:          msg.Heading,
			HDOP:             msg.HDOP,
			Satellites:       msg.Satellites,
			FixStatus:        msg.FixStatus,
			Rain:             msg.Rain,
			EventType:        msg.EventType,
			MovementFiltered: msg.MovementFiltered,
			DistanceFromLast: msg.DistanceFromLast,
			TimeSinceLast:    msg.TimeSinceLast,
			VehiclePlate:     msg.VehiclePlate,
			RouteID:          msg.RouteID,
			DriverID:         msg.DriverID,
			ConductorID:      msg.ConductorID,
			Capacity:         msg.Capacity,
			RawMessage:       msg.RawMessage,
			SchemaVersion:    msg.SchemaVersion,
			DeviceTimestamp:  msg.DeviceTimestamp,
			ReceivedAt:       msg.ReceivedAt,
			ProcessedAt:      time.Now(),
			RecordedAt:       time.Now(),
		}
		if err := batch.AppendStruct(row); err != nil {
			// Skip malformed rows; rest of batch is still committed.
			log.Printf("[batch-writer] skipping malformed row vehicle=%s: %v", msg.VehicleID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}

	p.metrics.batchSize.Observe(float64(len(messages)))
	return nil
}

func (p *BatchProcessor) ackRedisMessages(messages []ProcessedMessage) {
	byStream := make(map[string][]string)
	for _, msg := range messages {
		byStream[msg.Stream] = append(byStream[msg.Stream], msg.RedisID)
	}
	for stream, ids := range byStream {
		if err := p.redisClient.XAck(context.Background(), stream,
			p.config.Redis.ConsumerGroup, ids...).Err(); err != nil {
			log.Printf("[batch-writer] XAck error stream=%s: %v", stream, err)
		}
	}
}

func (p *BatchProcessor) processBatch(messages []RedisMessage) ([]ProcessedMessage, []RedisMessage) {
	var processed []ProcessedMessage
	var failed []RedisMessage
	for _, msg := range messages {
		pm, err := deserialiseMessage(msg)
		if err != nil {
			log.Printf("[batch-writer] deserialise error id=%s: %v", msg.ID, err)
			failed = append(failed, msg)
			continue
		}
		processed = append(processed, pm)
	}
	return processed, failed
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics HTTP server
// FIX: port now comes from config (which reads METRICS_PORT env) instead of
// the previous hard-coded 9090.
// ─────────────────────────────────────────────────────────────────────────────

func startMetricsServer(config MetricsConfig) {
	mux := http.NewServeMux()
	mux.Handle(config.Path, promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", config.Port)
	log.Printf("[batch-writer] metrics + health on %s", addr)

	srv := &http.Server{Addr: addr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[batch-writer] metrics server error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[batch-writer] starting...")

	config := loadConfig()

	tracerProvider, err := setupTracing("batch-writer")
	if err != nil {
		log.Fatalf("[batch-writer] tracing setup failed: %v", err)
	}
	defer tracerProvider.Shutdown(context.Background())

	metrics := NewBatchWriterMetrics()

	redisClient := initRedisClusterClient(config.Redis)

	clickhouseWriter, err := NewClickHouseWriter(config.ClickHouse)
	if err != nil {
		log.Fatalf("[batch-writer] ClickHouse init failed: %v", err)
	}

	dlqProcessor := NewDLQProcessor(config.DLQ, redisClient, metrics)

	// FIX: pass full Config (not config.Processing) so BatchProcessor can
	// access config.Redis.* inside readBatchFromRedis.
	batchProcessor := NewBatchProcessor(config, redisClient, clickhouseWriter, dlqProcessor, metrics)

	go startMetricsServer(config.Metrics)

	batchProcessor.Start()
	dlqProcessor.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("[batch-writer] running (instance=%s, metrics-port=%d)",
		envOr("INSTANCE_ID", "batch-writer-0"), config.Metrics.Port)

	<-sigChan
	log.Println("[batch-writer] shutting down...")
	batchProcessor.Stop()
	dlqProcessor.Stop()
	log.Println("[batch-writer] shutdown complete")
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func extractOrgID(stream string) string {
	// stream format: gps:batch:{orgId}
	parts := splitLast(stream, ":")
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

func splitLast(s, sep string) []string {
	idx := len(s) - len(sep)
	for i := len(s) - len(sep); i >= 0; i-- {
		if s[i:i+len(sep)] == sep {
			idx = i
			break
		}
	}
	return []string{s[:idx], s[idx+len(sep):]}
}
