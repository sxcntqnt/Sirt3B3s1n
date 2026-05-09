// batch-writer/main.go
//
// Reads GPS events from Redis Streams, batches them, and inserts into
// ClickHouse. Exposes /metrics (Prometheus) and /health for the orchestrator.
//
// Config precedence: ENV > orchestrator.yaml > coded defaults.
// All env keys are prefixed BATCH_WRITER_ by Viper (e.g. BATCH_WRITER_REDIS_PASSWORD).
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	redis "github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config — Viper-backed, atomic ConfigHolder, 12-factor layering
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
	BatchPrefix string // e.g. "gps:batch:" — registry appends orgId
	DLQPrefix   string // e.g. "gps:dlq:"
}

type RedisReadConfig struct {
	BatchSize           int64
	BlockTime           time.Duration
	StreamRefreshInterval time.Duration // how often StreamRegistry re-SCАNs for new orgs
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
	BatchSize     int
	FlushInterval time.Duration
	Compression   string
}

type ClickHousePerformance struct {
	MaxRetries   int
	RetryBackoff time.Duration
	MaxOpen      int
	MaxIdle      int
	MaxLifetime  time.Duration
}

type ProcessingConfig struct {
	Workers    int
	BufferSize int
}

type DLQConfig struct {
	MaxRetries    int
	RetryInterval time.Duration
	MaxAgeDays    int
	BatchSize     int
}

type MetricsConfig struct {
	Port int
	Path string
}

// ─── ConfigHolder — immutable snapshot + atomic swap, zero mutexes ──────────

type ConfigHolder struct {
	val atomic.Value // stores *Config
}

func NewConfigHolder(cfg *Config) *ConfigHolder {
	h := &ConfigHolder{}
	h.val.Store(cfg)
	return h
}

func (h *ConfigHolder) Get() *Config { return h.val.Load().(*Config) }
func (h *ConfigHolder) Set(cfg *Config) { h.val.Store(cfg) }

// ─── Viper loader ────────────────────────────────────────────────────────────

func loadConfig() (*Config, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix("BATCH_WRITER")

	v.SetConfigName("batch-writer")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/matatu-pulse")
	_ = v.ReadInConfig()

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "batch-writer-0"
	}

	// ── defaults ─────────────────────────────────────────────────────────────
	v.SetDefault("redis.cluster", true)
	v.SetDefault("redis.nodes", []string{"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"})
	v.SetDefault("redis.consumer_group", "batch-writers")
	v.SetDefault("redis.streams.batch_prefix", "gps:batch:")
	v.SetDefault("redis.streams.dlq_prefix", "gps:dlq:")
	v.SetDefault("redis.read.batch_size", 10000)
	v.SetDefault("redis.read.block_time", "5s")
	v.SetDefault("redis.read.stream_refresh_interval", "30s")

	v.SetDefault("clickhouse.database", "default")
	v.SetDefault("clickhouse.table", "gps_events")
	v.SetDefault("clickhouse.optimization.batch_size", 10000)
	v.SetDefault("clickhouse.optimization.flush_interval", "5s")
	v.SetDefault("clickhouse.optimization.compression", "lz4")
	v.SetDefault("clickhouse.performance.max_retries", 3)
	v.SetDefault("clickhouse.performance.retry_backoff", "1s")
	v.SetDefault("clickhouse.performance.max_open", 20)
	v.SetDefault("clickhouse.performance.max_idle", 10)
	v.SetDefault("clickhouse.performance.max_lifetime", "5m")

	v.SetDefault("processing.workers", 4)
	v.SetDefault("processing.buffer_size", 50000)

	v.SetDefault("dlq.max_retries", 3)
	v.SetDefault("dlq.retry_interval", "60s")
	v.SetDefault("dlq.max_age_days", 7)
	v.SetDefault("dlq.batch_size", 1000)

	metricsPort := 9090
	if p := os.Getenv("METRICS_PORT"); p != "" {
		if n,err := strconv.Atoi(p); err == nil && n > 0 {
			metricsPort = n
		}
	}
	v.SetDefault("metrics.port", metricsPort)
	v.SetDefault("metrics.path", "/metrics")

	cfg := &Config{
		Redis: RedisConfig{
			Cluster:       v.GetBool("redis.cluster"),
			Nodes:         v.GetStringSlice("redis.nodes"),
			Password:      v.GetString("redis.password"),
			ConsumerGroup: v.GetString("redis.consumer_group"),
			ConsumerName:  instanceID,
			Streams: RedisStreamConfig{
				BatchPrefix: v.GetString("redis.streams.batch_prefix"),
				DLQPrefix:   v.GetString("redis.streams.dlq_prefix"),
			},
			ReadConfig: RedisReadConfig{
				BatchSize:             v.GetInt64("redis.read.batch_size"),
				BlockTime:             v.GetDuration("redis.read.block_time"),
				StreamRefreshInterval: v.GetDuration("redis.read.stream_refresh_interval"),
			},
		},
		ClickHouse: ClickHouseConfig{
			Host:     v.GetString("clickhouse.host"),
			Username: v.GetString("clickhouse.username"),
			Password: v.GetString("clickhouse.password"),
			Database: v.GetString("clickhouse.database"),
			Table:    v.GetString("clickhouse.table"),
			Optimization: ClickHouseOptimization{
				BatchSize:     v.GetInt("clickhouse.optimization.batch_size"),
				FlushInterval: v.GetDuration("clickhouse.optimization.flush_interval"),
				Compression:   v.GetString("clickhouse.optimization.compression"),
			},
			Performance: ClickHousePerformance{
				MaxRetries:   v.GetInt("clickhouse.performance.max_retries"),
				RetryBackoff: v.GetDuration("clickhouse.performance.retry_backoff"),
				MaxOpen:      v.GetInt("clickhouse.performance.max_open"),
				MaxIdle:      v.GetInt("clickhouse.performance.max_idle"),
				MaxLifetime:  v.GetDuration("clickhouse.performance.max_lifetime"),
			},
		},
		Processing: ProcessingConfig{
			Workers:    v.GetInt("processing.workers"),
			BufferSize: v.GetInt("processing.buffer_size"),
		},
		DLQ: DLQConfig{
			MaxRetries:    v.GetInt("dlq.max_retries"),
			RetryInterval: v.GetDuration("dlq.retry_interval"),
			MaxAgeDays:    v.GetInt("dlq.max_age_days"),
			BatchSize:     v.GetInt("dlq.batch_size"),
		},
		Metrics: MetricsConfig{
			Port: v.GetInt("metrics.port"),
			Path: v.GetString("metrics.path"),
		},
	}

	// ── validation ───────────────────────────────────────────────────────────
	if len(cfg.Redis.Nodes) == 0 {
		return nil, fmt.Errorf("redis.nodes must not be empty")
	}
	if cfg.ClickHouse.Host == "" {
		return nil, fmt.Errorf("BATCH_WRITER_CLICKHOUSE_HOST is required")
	}
	if cfg.Processing.Workers < 1 {
		return nil, fmt.Errorf("processing.workers must be >= 1")
	}

	return cfg, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────────────────────────────────────

// BatchWriterMetrics groups all Prometheus instruments for this service.
// dlqDepth is backed by an atomic int64 so AddMessages/XAck can do real Set()
// calls on the gauge without exposing the gauge directly to concurrent writers.
type BatchWriterMetrics struct {
	// Gauges
	redisStreamLag prometheus.Gauge // actual XPENDING count — see measureLag()
	dlqDepth       prometheus.Gauge // real DLQ depth (Set by DLQProcessor)

	// Histograms
	batchWriteDuration prometheus.Histogram // ms per ClickHouse INSERT
	batchSizeRows      prometheus.Histogram // rows per INSERT

	// Counters
	rowsInserted     prometheus.Counter
	clickhouseErrors prometheus.Counter
	redisReadErrors  prometheus.Counter
	dlqIngested      prometheus.Counter // messages pushed into DLQ (ever)
	dlqRetried       prometheus.Counter // messages successfully retried out of DLQ
	dlqDropped       prometheus.Counter // messages abandoned after MaxRetries

	// Internal atomic — DLQProcessor uses this so it can Set() dlqDepth safely.
	dlqDepthCount atomic.Int64
}

func NewBatchWriterMetrics() *BatchWriterMetrics {
	m := &BatchWriterMetrics{
		redisStreamLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_writer_redis_stream_lag",
			Help: "Total pending messages across all tracked Redis streams (XPENDING sum)",
		}),
		dlqDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "batch_writer_dlq_depth",
			Help: "Current number of messages sitting in the DLQ stream(s)",
		}),
		batchWriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_writer_batch_write_duration_ms",
			Help:    "ClickHouse batch write duration in milliseconds",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000},
		}),
		batchSizeRows: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_writer_batch_size_rows",
			Help:    "Number of rows per ClickHouse INSERT batch",
			Buckets: []float64{100, 500, 1000, 2500, 5000, 10000, 50000},
		}),
		rowsInserted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_rows_inserted_total",
			Help: "Total rows successfully inserted into ClickHouse",
		}),
		clickhouseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_clickhouse_errors_total",
			Help: "ClickHouse write errors",
		}),
		redisReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_redis_read_errors_total",
			Help: "Redis XReadGroup errors",
		}),
		dlqIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_dlq_ingested_total",
			Help: "Total messages ever pushed into the DLQ",
		}),
		dlqRetried: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_dlq_retried_total",
			Help: "Messages successfully retried out of the DLQ",
		}),
		dlqDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_writer_dlq_dropped_total",
			Help: "Messages abandoned after MaxRetries",
		}),
	}

	prometheus.MustRegister(
		m.redisStreamLag,
		m.dlqDepth,
		m.batchWriteDuration,
		m.batchSizeRows,
		m.rowsInserted,
		m.clickhouseErrors,
		m.redisReadErrors,
		m.dlqIngested,
		m.dlqRetried,
		m.dlqDropped,
	)
	return m
}

// ─────────────────────────────────────────────────────────────────────────────
// BatchProcessor
// ─────────────────────────────────────────────────────────────────────────────

type BatchProcessor struct {
	cfgHolder    *ConfigHolder
	redisClient  *redis.ClusterClient
	chWriter     *ClickHouseWriter
	dlqProcessor *DLQProcessor
	metrics      *BatchWriterMetrics
	registry     *StreamRegistry
	stopChan     chan struct{}
}

func NewBatchProcessor(
	cfgHolder *ConfigHolder,
	rdb *redis.ClusterClient,
	chWriter *ClickHouseWriter,
	dlq *DLQProcessor,
	metrics *BatchWriterMetrics,
	registry *StreamRegistry,
) *BatchProcessor {
	return &BatchProcessor{
		cfgHolder:    cfgHolder,
		redisClient:  rdb,
		chWriter:     chWriter,
		dlqProcessor: dlq,
		metrics:      metrics,
		registry:     registry,
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
			cfg := p.cfgHolder.Get()
			if err = p.writeToClickhouse(processed, cfg.ClickHouse.Table); err != nil {
				p.metrics.clickhouseErrors.Inc()
				log.Printf("[batch-writer] ClickHouse write failed (%d rows): %v", len(processed), err)
				p.dlqProcessor.AddMessages(processed)
			} else {
				p.ackRedisMessages(processed)
				p.metrics.rowsInserted.Add(float64(len(processed)))
				p.metrics.batchWriteDuration.Observe(float64(time.Since(start).Milliseconds()))
				p.metrics.batchSizeRows.Observe(float64(len(processed)))
			}
		}
		if len(failed) > 0 {
			p.dlqProcessor.AddMessages(failed)
		}
	}
}

func (p *BatchProcessor) readBatchFromRedis() ([]RedisMessage, error) {
	cfg := p.cfgHolder.Get()

	// StreamRegistry returns only known-good, explicit stream keys.
	// This replaces the broken "gps:batch:*" glob — see StreamRegistry.
	streams := p.registry.StreamArgs()
	if len(streams) == 0 {
		return nil, nil // no org streams discovered yet
	}

	args := &redis.XReadGroupArgs{
		Group:    cfg.Redis.ConsumerGroup,
		Consumer: cfg.Redis.ConsumerName,
		Streams:  streams,
		Count:    cfg.Redis.ReadConfig.BatchSize,
		Block:    cfg.Redis.ReadConfig.BlockTime,
		NoAck:    false,
	}

	result, err := p.redisClient.XReadGroup(context.Background(), args).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	// Measure real lag: sum XPENDING counts across all tracked streams.
	go p.measureLag(cfg)

	var msgs []RedisMessage
	for _, sr := range result {
		for _, m := range sr.Messages {
			msgs = append(msgs, RedisMessage{
				Stream: sr.Stream,
				ID:     m.ID,
				Values: m.Values,
				OrgID:  extractOrgID(sr.Stream, cfg.Redis.Streams.BatchPrefix),
			})
		}
	}
	return msgs, nil
}

// measureLag runs in a goroutine after each read — it sums XPENDING counts
// across all tracked streams and updates the Prometheus gauge.
func (p *BatchProcessor) measureLag(cfg *Config) {
	keys := p.registry.Keys()
	var total int64
	for _, key := range keys {
		pending, err := p.redisClient.XPending(
			context.Background(), key, cfg.Redis.ConsumerGroup,
		).Result()
		if err == nil {
			total += pending.Count
		}
	}
	p.metrics.redisStreamLag.Set(float64(total))
}

func (p *BatchProcessor) processBatch(messages []RedisMessage) ([]ProcessedMessage, []ProcessedMessage) {
	var ok, fail []ProcessedMessage
	for _, msg := range messages {
		pm, err := deserialiseMessage(msg)
		if err != nil {
			log.Printf("[batch-writer] deserialise id=%s stream=%s: %v", msg.ID, msg.Stream, err)
			fail = append(fail, ProcessedMessage{
				Stream:  msg.Stream,
				RedisID: msg.ID,
				OrgID:   msg.OrgID,
			})
			continue
		}
		ok = append(ok, pm)
	}
	return ok, fail
}

func (p *BatchProcessor) writeToClickhouse(messages []ProcessedMessage, table string) error {
	return p.chWriter.InsertBatch(context.Background(), table, messages)
}

func (p *BatchProcessor) ackRedisMessages(messages []ProcessedMessage) {
	cfg := p.cfgHolder.Get()
	byStream := make(map[string][]string, len(messages))
	for _, m := range messages {
		byStream[m.Stream] = append(byStream[m.Stream], m.RedisID)
	}
	for stream, ids := range byStream {
		if err := p.redisClient.XAck(
			context.Background(), stream, cfg.Redis.ConsumerGroup, ids...,
		).Err(); err != nil {
			log.Printf("[batch-writer] XAck stream=%s ids=%d: %v", stream, len(ids), err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics + health server
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("[batch-writer] starting...")

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[batch-writer] config error: %v", err)
	}
	cfgHolder := NewConfigHolder(cfg)

	shutdown, err := setupTracing("batch-writer")
	if err != nil {
		log.Fatalf("[batch-writer] tracing: %v", err)
	}
	defer shutdown(context.Background())

	metrics  := NewBatchWriterMetrics()
	rdb      := initRedisClusterClient(cfg.Redis)
	chWriter, err := NewClickHouseWriter(cfg.ClickHouse)
	if err != nil {
		log.Fatalf("[batch-writer] ClickHouse init: %v", err)
	}

	// StreamRegistry discovers "gps:batch:{orgId}" keys via SCAN and ensures
	// consumer groups exist before BatchProcessor reads from them.
	registry := NewStreamRegistry(rdb, cfg.Redis)
	if err := registry.Bootstrap(context.Background()); err != nil {
		log.Printf("[batch-writer] stream registry bootstrap warning: %v", err)
	}

	dlq   := NewDLQProcessor(cfgHolder, rdb, chWriter, metrics)
	batch := NewBatchProcessor(cfgHolder, rdb, chWriter, dlq, metrics, registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startMetricsServer(cfg.Metrics)
	go registry.Run(ctx)
	batch.Start()
	dlq.Start()

	log.Printf("[batch-writer] running (instance=%s metrics-port=%d)",
		cfg.Redis.ConsumerName, cfg.Metrics.Port)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("[batch-writer] shutting down — draining...")
	cancel()
	batch.Stop()
	dlq.Stop()
	log.Println("[batch-writer] done")
}
