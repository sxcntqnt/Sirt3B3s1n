// batch-writer/main.go
// All types, clients, and helpers live in internal.go (same package).
// Fixes applied (scalability audit):
//  1. All 7 Prometheus fields initialised before MustRegister.
//  2. osGetenv → os.Getenv.
//  3. BatchProcessor.config is full Config (was ProcessingConfig).
//  4. Metrics port from METRICS_PORT env; /health endpoint added.
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
	"github.com/redis/go-redis/v9"
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
	ConsumerName  string
	Streams       RedisStreamConfig
	ReadConfig    RedisReadConfig
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
	MaxRetries     int
	RetryBackoff   time.Duration
	ConnectionPool ConnectionPool
}

type ConnectionPool struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
}

type ProcessingConfig struct {
	Workers         int
	BufferSize      int
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
// Config
// ─────────────────────────────────────────────────────────────────────────────

func loadConfig() Config {
	instanceID  := envOr("INSTANCE_ID", "batch-writer-0")
	metricsPort := envInt("METRICS_PORT", 9090)

	return Config{
		Redis: RedisConfig{
			Cluster:       true,
			Nodes:         []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password:      os.Getenv("REDIS_PASSWORD"), // FIX: was osGetenv
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
// Prometheus metrics
// FIX: all 7 fields initialised before MustRegister (4 were nil → panic)
// ─────────────────────────────────────────────────────────────────────────────

type BatchWriterMetrics struct {
	redisStreamLag     prometheus.Gauge
	batchWriteDuration prometheus.Histogram
	rowsInserted       prometheus.Counter
	dlqSize            prometheus.Gauge     // was nil
	clickhouseErrors   prometheus.Counter   // was nil
	redisReadErrors    prometheus.Counter   // was nil
	batchSize          prometheus.Histogram // was nil
}

func NewBatchWriterMetrics() *BatchWriterMetrics {
	m := &BatchWriterMetrics{
		redisStreamLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_writer_redis_stream_lag",
			Help: "Messages lagging in Redis streams",
		}),
		batchWriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_writer_batch_write_duration_ms",
			Help:    "ClickHouse batch write duration ms",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000},
		}),
		rowsInserted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_rows_inserted_total",
			Help: "Rows inserted to ClickHouse",
		}),
		dlqSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_writer_dlq_size",
			Help: "Current DLQ depth",
		}),
		clickhouseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_clickhouse_errors_total",
			Help: "ClickHouse write errors",
		}),
		redisReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_redis_read_errors_total",
			Help: "Redis read errors",
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_writer_batch_size",
			Help:    "Rows per INSERT batch",
			Buckets: []float64{100, 500, 1000, 2500, 5000, 10000, 50000},
		}),
	}
	prometheus.MustRegister(
		m.redisStreamLag, m.batchWriteDuration, m.rowsInserted,
		m.dlqSize, m.clickhouseErrors, m.redisReadErrors, m.batchSize,
	)
	return m
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch Processor
// FIX: config field is now full Config (was ProcessingConfig) so
// readBatchFromRedis can reach config.Redis.*
// ─────────────────────────────────────────────────────────────────────────────

type BatchProcessor struct {
	redisClient  *redis.ClusterClient
	chWriter     *ClickHouseWriter // defined in internal.go
	dlqProcessor *DLQProcessor     // defined in internal.go
	metrics      *BatchWriterMetrics
	config       Config // FIX: was ProcessingConfig
	stopChan     chan struct{}
}

func NewBatchProcessor(
	config Config, // FIX: was ProcessingConfig
	rdb *redis.ClusterClient,
	chWriter *ClickHouseWriter,
	dlq *DLQProcessor,
	metrics *BatchWriterMetrics,
) *BatchProcessor {
	return &BatchProcessor{
		redisClient:  rdb,
		chWriter:     chWriter,
		dlqProcessor: dlq,
		metrics:      metrics,
		config:       config,
		stopChan:     make(chan struct{}),
	}
}

func (p *BatchProcessor) Start() { go p.processBatchLoop() }
func (p *BatchProcessor) Stop()  { close(p.stopChan) }

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

		start := time.Now()
		processed, failed := p.processBatch(messages)

		if len(processed) > 0 {
			if err = p.writeToClickhouse(processed); err != nil {
				p.metrics.clickhouseErrors.Inc()
				p.dlqProcessor.AddMessages(processed)
			} else {
				p.ackRedisMessages(processed)
				p.metrics.rowsInserted.Add(float64(len(processed)))
				p.metrics.batchWriteDuration.Observe(float64(time.Since(start).Milliseconds()))
			}
		}
		if len(failed) > 0 {
			p.dlqProcessor.AddMessages(failed)
		}
	}
}

func (p *BatchProcessor) readBatchFromRedis() ([]RedisMessage, error) {
	// FIX: p.config is full Config — p.config.Redis.* is now reachable.
	args := redis.XReadGroupArgs{
		Group:    p.config.Redis.ConsumerGroup,
		Consumer: p.config.Redis.ConsumerName,
		Streams:  []string{"gps:batch:*", ">"},
		Count:    p.config.Redis.ReadConfig.BatchSize,
		Block:    p.config.Redis.ReadConfig.BlockTime,
		NoAck:    false,
	}

	result, err := p.redisClient.XReadGroup(context.Background(), &args).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	p.metrics.redisStreamLag.Set(float64(len(result)))

	var msgs []RedisMessage
	for _, sr := range result {
		for _, m := range sr.Messages {
			msgs = append(msgs, RedisMessage{
				Stream: sr.Stream,
				ID:     m.ID,
				Values: m.Values,
				OrgID:  extractOrgID(sr.Stream), // defined in internal.go
			})
		}
	}
	return msgs, nil
}

func (p *BatchProcessor) processBatch(messages []RedisMessage) ([]ProcessedMessage, []ProcessedMessage) {
	var ok, fail []ProcessedMessage
	for _, msg := range messages {
		pm, err := deserialiseMessage(msg) // defined in internal.go
		if err != nil {
			log.Printf("[batch-writer] deserialise id=%s: %v", msg.ID, err)
			fail = append(fail, ProcessedMessage{Stream: msg.Stream, RedisID: msg.ID, OrgID: msg.OrgID})
			continue
		}
		ok = append(ok, pm)
	}
	return ok, fail
}

func (p *BatchProcessor) writeToClickhouse(messages []ProcessedMessage) error {
	batch, err := p.chWriter.PrepareBatch(
		context.Background(),
		fmt.Sprintf("INSERT INTO %s", p.config.ClickHouse.Table),
	)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, msg := range messages {
		row := ClickHouseRow{ // defined in internal.go
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
			ProcessedAt:      now,
			RecordedAt:       now,
		}
		if err := batch.AppendStruct(row); err != nil {
			log.Printf("[batch-writer] append vehicle=%s: %v", msg.VehicleID, err)
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
	for _, m := range messages {
		byStream[m.Stream] = append(byStream[m.Stream], m.RedisID)
	}
	for stream, ids := range byStream {
		if err := p.redisClient.XAck(context.Background(), stream,
			p.config.Redis.ConsumerGroup, ids...).Err(); err != nil {
			log.Printf("[batch-writer] XAck stream=%s: %v", stream, err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics + health server — FIX: port from METRICS_PORT env
// ─────────────────────────────────────────────────────────────────────────────

func startMetricsServer(cfg MetricsConfig) {
	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("[batch-writer] metrics+health on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[batch-writer] metrics server: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[batch-writer] starting...")

	cfg := loadConfig()

	// setupTracing is defined in internal.go and returns (func(ctx) error, error)
	shutdown, err := setupTracing("batch-writer")
	if err != nil {
		log.Fatalf("[batch-writer] tracing: %v", err)
	}
	defer shutdown(context.Background())

	metrics  := NewBatchWriterMetrics()
	rdb      := initRedisClusterClient(cfg.Redis) // internal.go
	chWriter, err := NewClickHouseWriter(cfg.ClickHouse) // internal.go
	if err != nil {
		log.Fatalf("[batch-writer] ClickHouse: %v", err)
	}

	dlq   := NewDLQProcessor(cfg.DLQ, rdb, metrics) // internal.go
	batch := NewBatchProcessor(cfg, rdb, chWriter, dlq, metrics)

	go startMetricsServer(cfg.Metrics)
	batch.Start()
	dlq.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("[batch-writer] running (instance=%s port=%d)",
		envOr("INSTANCE_ID", "batch-writer-0"), cfg.Metrics.Port)

	<-sig
	log.Println("[batch-writer] shutting down...")
	batch.Stop()
	dlq.Stop()
	log.Println("[batch-writer] done")
}

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
