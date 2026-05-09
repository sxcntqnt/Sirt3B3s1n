// mqtt-consumer/main.go
//
// Reads GPS events from MQTT, parses and enriches them, applies a movement
// filter, then writes to Redis Streams for downstream consumers.
// Critical events are also written directly to ClickHouse.
//
// Config precedence: ENV > mqtt-consumer.yaml > coded defaults.
// All env keys are prefixed MQTT_CONSUMER_ by Viper.
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	redis "github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config — Viper-backed, atomic ConfigHolder, 12-factor layering
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	MQTT       MQTTConfig
	Redis      RedisConfig
	ClickHouse ClickHouseConfig
	Processing ProcessingConfig
	Metrics    MetricsConfig
}

type MQTTConfig struct {
	Broker   string
	Username string
	Password string
	Topics   []string
	QoS      byte
}

type RedisConfig struct {
	Cluster  bool
	Nodes    []string
	Password string
}

type ClickHouseConfig struct {
	DirectWriteEnabled bool
	Host               string
	Username           string
	Password           string
	Database           string
	Table              string
}

type MovementFilter struct {
	MinDistanceMeters float64
	MinTimeSeconds    int
}

type BackpressureConfig struct {
	MaxRedisLatencyMs int64
	LocalBufferSize   int
}

type ProcessingConfig struct {
	Workers        int
	BufferSize     int
	MovementFilter MovementFilter
	Backpressure   BackpressureConfig
}

type MetricsConfig struct {
	Port int
	Path string
}

// ─── ConfigHolder — immutable snapshot + atomic swap ────────────────────────

type ConfigHolder struct {
	val atomic.Value // stores *Config
}

func NewConfigHolder(cfg *Config) *ConfigHolder {
	h := &ConfigHolder{}
	h.val.Store(cfg)
	return h
}

func (h *ConfigHolder) Get() *Config    { return h.val.Load().(*Config) }
func (h *ConfigHolder) Set(cfg *Config) { h.val.Store(cfg) }

// ─── Viper loader ────────────────────────────────────────────────────────────

func loadConfig() (*Config, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix("MQTT_CONSUMER")

	v.SetConfigName("mqtt-consumer")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/matatu-pulse")
	_ = v.ReadInConfig()

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "mqtt-consumer-0"
	}

	// ── defaults ─────────────────────────────────────────────────────────────
	v.SetDefault("mqtt.topics", []string{"gps/+/+"})
	v.SetDefault("mqtt.qos", 1)

	v.SetDefault("redis.cluster", true)
	v.SetDefault("redis.nodes", []string{"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"})

	v.SetDefault("clickhouse.direct_write_enabled", false)
	v.SetDefault("clickhouse.database", "default")
	v.SetDefault("clickhouse.table", "gps_events")

	v.SetDefault("processing.workers", 8)
	v.SetDefault("processing.buffer_size", 10000)
	v.SetDefault("processing.movement_filter.min_distance_meters", 20.0)
	v.SetDefault("processing.movement_filter.min_time_seconds", 5)
	v.SetDefault("processing.backpressure.max_redis_latency_ms", 50)
	v.SetDefault("processing.backpressure.local_buffer_size", 10000)

	metricsPort := 9090
	if p := os.Getenv("METRICS_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			metricsPort = n
		}
	}
	v.SetDefault("metrics.port", metricsPort)
	v.SetDefault("metrics.path", "/metrics")

	cfg := &Config{
		MQTT: MQTTConfig{
			Broker:   v.GetString("mqtt.broker"),
			Username: v.GetString("mqtt.username"),
			Password: v.GetString("mqtt.password"),
			Topics:   v.GetStringSlice("mqtt.topics"),
			QoS:      byte(v.GetInt("mqtt.qos")),
		},
		Redis: RedisConfig{
			Cluster:  v.GetBool("redis.cluster"),
			Nodes:    v.GetStringSlice("redis.nodes"),
			Password: v.GetString("redis.password"),
		},
		ClickHouse: ClickHouseConfig{
			DirectWriteEnabled: v.GetBool("clickhouse.direct_write_enabled"),
			Host:               v.GetString("clickhouse.host"),
			Username:           v.GetString("clickhouse.username"),
			Password:           v.GetString("clickhouse.password"),
			Database:           v.GetString("clickhouse.database"),
			Table:              v.GetString("clickhouse.table"),
		},
		Processing: ProcessingConfig{
			Workers:    v.GetInt("processing.workers"),
			BufferSize: v.GetInt("processing.buffer_size"),
			MovementFilter: MovementFilter{
				MinDistanceMeters: v.GetFloat64("processing.movement_filter.min_distance_meters"),
				MinTimeSeconds:    v.GetInt("processing.movement_filter.min_time_seconds"),
			},
			Backpressure: BackpressureConfig{
				MaxRedisLatencyMs: v.GetInt64("processing.backpressure.max_redis_latency_ms"),
				LocalBufferSize:   v.GetInt("processing.backpressure.local_buffer_size"),
			},
		},
		Metrics: MetricsConfig{
			Port: v.GetInt("metrics.port"),
			Path: v.GetString("metrics.path"),
		},
	}

	// ── validation ───────────────────────────────────────────────────────────
	if cfg.MQTT.Broker == "" {
		return nil, fmt.Errorf("MQTT_CONSUMER_MQTT_BROKER is required")
	}
	if len(cfg.Redis.Nodes) == 0 {
		return nil, fmt.Errorf("redis.nodes must not be empty")
	}
	if cfg.Processing.Workers < 1 {
		return nil, fmt.Errorf("processing.workers must be >= 1")
	}

	return cfg, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────────────────────────────────────

type MetricsCollector struct {
	eventsReceived        prometheus.Counter
	eventsProcessed       prometheus.Counter
	eventsFiltered        prometheus.Counter
	parseErrors           prometheus.Counter // FIX: was called via Increment but not defined
	mqttDropped           prometheus.Counter // FIX: new — drop-at-source visibility
	redisWriteErrors      prometheus.Counter
	clickhouseWriteErrors prometheus.Counter
	batchStreamErrors     prometheus.Counter // FIX: writeToBatchStream errors now tracked
	processingTime        prometheus.Histogram
	redisLatency          prometheus.Histogram
	bufferSize            prometheus.Gauge
	workerQueueLength     prometheus.Gauge
	criticalDropped       prometheus.Counter // FIX: new — silent critical drop made visible
}

func NewMetricsCollector() *MetricsCollector {
	m := &MetricsCollector{
		eventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_received_total",
			Help: "Total MQTT messages received",
		}),
		eventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_processed_total",
			Help: "Events successfully processed and written to Redis",
		}),
		eventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_filtered_total",
			Help: "Events suppressed by the movement filter",
		}),
		parseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_parse_errors_total",
			Help: "MQTT messages that failed JSON parsing or validation",
		}),
		mqttDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_mqtt_dropped_total",
			Help: "MQTT messages dropped because the worker channel was full",
		}),
		redisWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_redis_write_errors_total",
			Help: "Redis XAdd errors on the realtime stream",
		}),
		clickhouseWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_clickhouse_write_errors_total",
			Help: "Direct ClickHouse write errors for critical events",
		}),
		batchStreamErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_batch_stream_errors_total",
			Help: "Redis XAdd errors on the batch stream",
		}),
		processingTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_consumer_event_processing_duration_ms",
			Help:    "Per-event processing latency in milliseconds",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 25, 50, 100},
		}),
		redisLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_consumer_redis_write_latency_ms",
			Help:    "Redis XADD latency in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
		}),
		bufferSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_consumer_backpressure_buffer_size",
			Help: "Current number of events in the backpressure buffer",
		}),
		workerQueueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_consumer_worker_queue_length",
			Help: "Current depth of the worker input channel",
		}),
		criticalDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_critical_buffer_dropped_total",
			Help: "Critical events dropped because the backpressure critical slot was full",
		}),
	}

	prometheus.MustRegister(
		m.eventsReceived, m.eventsProcessed, m.eventsFiltered,
		m.parseErrors, m.mqttDropped,
		m.redisWriteErrors, m.clickhouseWriteErrors, m.batchStreamErrors,
		m.processingTime, m.redisLatency,
		m.bufferSize, m.workerQueueLength, m.criticalDropped,
	)
	return m
}

// Increment dispatches named counter increments from processWorker.
// FIX: "parse_errors" case added; all callsites now have a matching case.
func (m *MetricsCollector) Increment(name string) {
	switch name {
	case "parse_errors":
		m.parseErrors.Inc()
	case "filtered_events":
		m.eventsFiltered.Inc()
	case "redis_write_errors":
		m.redisWriteErrors.Inc()
	case "clickhouse_write_errors":
		m.clickhouseWriteErrors.Inc()
	case "batch_stream_errors":
		m.batchStreamErrors.Inc()
	case "critical_dropped":
		m.criticalDropped.Inc()
	default:
		log.Printf("[mqtt-consumer/metrics] unknown counter: %s", name)
	}
}

func (m *MetricsCollector) RecordProcessingTime(d time.Duration) {
	m.processingTime.Observe(float64(d.Milliseconds()))
	m.eventsProcessed.Inc()
}

func (m *MetricsCollector) ObserveRedisLatency(d time.Duration) {
	m.redisLatency.Observe(float64(d.Milliseconds()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Backpressure ring buffer
// ─────────────────────────────────────────────────────────────────────────────

type BackpressureBuffer struct {
	mu       sync.Mutex
	normal   []EnrichedEvent
	critical []EnrichedEvent
	capacity int
	degraded atomic.Int32
	metrics  *MetricsCollector
}

func NewBackpressureBuffer(capacity int, m *MetricsCollector) *BackpressureBuffer {
	return &BackpressureBuffer{
		capacity: capacity,
		normal:   make([]EnrichedEvent, 0, capacity),
		critical: make([]EnrichedEvent, 0, capacity/2),
		metrics:  m,
	}
}

func (b *BackpressureBuffer) Push(ev EnrichedEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if isCriticalEvent(ev) {
		if len(b.critical) < b.capacity/2 {
			b.critical = append(b.critical, ev)
		} else {
			// FIX: was a silent drop — now logged and counted.
			log.Printf("[mqtt-consumer/buffer] critical slot full — dropping vehicle=%s event=%s",
				ev.VehicleID, ev.EventType)
			b.metrics.Increment("critical_dropped")
		}
	} else {
		if len(b.normal)+len(b.critical) >= b.capacity && len(b.normal) > 0 {
			b.normal = b.normal[1:] // evict oldest normal event
		}
		b.normal = append(b.normal, ev)
	}

	b.metrics.bufferSize.Set(float64(len(b.normal) + len(b.critical)))
}

// Drain returns up to n events, draining critical first.
func (b *BackpressureBuffer) Drain(n int) []EnrichedEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []EnrichedEvent
	for len(out) < n && len(b.critical) > 0 {
		out = append(out, b.critical[0])
		b.critical = b.critical[1:]
	}
	for len(out) < n && len(b.normal) > 0 {
		out = append(out, b.normal[0])
		b.normal = b.normal[1:]
	}
	b.metrics.bufferSize.Set(float64(len(b.normal) + len(b.critical)))
	return out
}

func (b *BackpressureBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.normal) + len(b.critical)
}

func (b *BackpressureBuffer) SetDegraded(v bool) {
	if v {
		b.degraded.Store(1)
	} else {
		b.degraded.Store(0)
	}
}
func (b *BackpressureBuffer) IsDegraded() bool { return b.degraded.Load() == 1 }

// ─────────────────────────────────────────────────────────────────────────────
// Worker pool
// ─────────────────────────────────────────────────────────────────────────────

type WorkerPool struct {
	cfgHolder    *ConfigHolder
	workers      int
	inputChannel chan MQTTMessage
	redisClient  *redis.ClusterClient
	// FIX: was *driver.Conn (pointer to interface = double indirection).
	// driver.Conn is already an interface; nil interface is the zero value.
	chConn       driver.Conn
	metrics      *MetricsCollector
	bpBuffer     *BackpressureBuffer
	bpMaxLatency time.Duration
	// FIX: movement filter stored so shouldBroadcast uses config values,
	// not hardcoded constants.
	movementFilter MovementFilter
	stripedLock    *StripedLock // FIX: per-vehicle enrichment serialization
	stopChan       chan struct{}
}

func NewWorkerPool(
	cfgHolder *ConfigHolder,
	rdb *redis.ClusterClient,
	chConn driver.Conn, // FIX: driver.Conn, not *driver.Conn
	m *MetricsCollector,
) *WorkerPool {
	cfg := cfgHolder.Get()
	return &WorkerPool{
		cfgHolder:      cfgHolder,
		workers:        cfg.Processing.Workers,
		inputChannel:   make(chan MQTTMessage, cfg.Processing.BufferSize),
		redisClient:    rdb,
		chConn:         chConn,
		metrics:        m,
		bpBuffer:       NewBackpressureBuffer(cfg.Processing.Backpressure.LocalBufferSize, m),
		bpMaxLatency:   time.Duration(cfg.Processing.Backpressure.MaxRedisLatencyMs) * time.Millisecond,
		movementFilter: cfg.Processing.MovementFilter,
		stripedLock:    NewStripedLock(256), // 256 stripes → ~1 lock per 400 vehicles at 100k scale
		stopChan:       make(chan struct{}),
	}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.workers; i++ {
		go p.processWorker()
	}
	go p.drainBackpressureBuffer()
	go p.reportQueueDepth()
}

func (p *WorkerPool) Stop() { close(p.stopChan) }

func (p *WorkerPool) processWorker() {
	for {
		select {
		case <-p.stopChan:
			return
		case msg, ok := <-p.inputChannel:
			if !ok {
				return
			}
			p.metrics.eventsReceived.Inc()
			start := time.Now()

			event, err := parseMQTTMessage(msg)
			if err != nil {
				p.metrics.Increment("parse_errors")
				continue
			}

			// FIX: stripedLock serializes concurrent enrichment for the same
			// vehicleID so two workers can't both read stale state, compute
			// wrong distance, and overwrite each other.
			p.stripedLock.Lock(event.VehicleID)
			enriched, _ := enrichEvent(event, p.redisClient)
			p.stripedLock.Unlock(event.VehicleID)

			// FIX: shouldBroadcast now uses config thresholds, not hardcoded constants.
			if !shouldBroadcast(enriched, p.movementFilter) && !isCriticalEvent(enriched) {
				p.metrics.Increment("filtered_events")
				if err := writeToBatchStream(enriched, p.redisClient); err != nil {
					p.metrics.Increment("batch_stream_errors")
				}
				continue
			}

			if err = p.timedRedisWrite(enriched); err != nil {
				p.metrics.Increment("redis_write_errors")
				p.handleBackpressure(enriched)
			}

			if p.chConn != nil && isCriticalEvent(enriched) {
				cfg := p.cfgHolder.Get()
				if err = writeToClickhouse(enriched, p.chConn, cfg.ClickHouse.Table); err != nil {
					p.metrics.Increment("clickhouse_write_errors")
				}
			}

			p.metrics.RecordProcessingTime(time.Since(start))
		}
	}
}

func (p *WorkerPool) timedRedisWrite(ev EnrichedEvent) error {
	start := time.Now()
	err := writeToRedisStreams(ev, p.redisClient)
	latency := time.Since(start)
	p.metrics.ObserveRedisLatency(latency)
	p.bpBuffer.SetDegraded(latency > p.bpMaxLatency)
	return err
}

func (p *WorkerPool) handleBackpressure(ev EnrichedEvent) {
	p.bpBuffer.Push(ev)
	log.Printf("[mqtt-consumer] backpressure vehicle=%s buffer=%d",
		ev.VehicleID, p.bpBuffer.Len())
}

func (p *WorkerPool) drainBackpressureBuffer() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopChan:
			p.flushBuffer()
			return
		case <-ticker.C:
			if !p.bpBuffer.IsDegraded() && p.bpBuffer.Len() > 0 {
				p.flushBuffer()
			}
		}
	}
}

// flushBuffer drains the backpressure buffer in batches.
// FIX: the old version returned immediately on the first failed write, leaving
// all subsequent events stuck. Now each event is attempted independently;
// failures are re-buffered but don't block the rest of the drain.
func (p *WorkerPool) flushBuffer() {
	for p.bpBuffer.Len() > 0 {
		events := p.bpBuffer.Drain(200)
		var requeue []EnrichedEvent
		for _, ev := range events {
			if err := p.timedRedisWrite(ev); err != nil {
				requeue = append(requeue, ev)
			}
		}
		// Re-buffer failures — but if Redis is still degraded, stop the drain
		// to avoid a busy-loop hammering a broken connection.
		for _, ev := range requeue {
			p.bpBuffer.Push(ev)
		}
		if p.bpBuffer.IsDegraded() {
			return
		}
	}
}

func (p *WorkerPool) reportQueueDepth() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.metrics.workerQueueLength.Set(float64(len(p.inputChannel)))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MQTT ingestion — drop at source
// ─────────────────────────────────────────────────────────────────────────────

// handleMQTTMessages reads from the MQTT client and forwards to the worker
// pool input channel.
//
// FIX: the old version used a blocking send (`pool.inputChannel <- msg`).
// When the channel is full, that blocks the receive goroutine, which blocks
// paho's internal delivery goroutine, which causes the broker to see a slow
// consumer and eventually drop the connection.
//
// Instead: use a non-blocking select and drop with metric + log so we have
// visibility into ingestion pressure without blocking the MQTT stack.
func handleMQTTMessages(client MQTTClient, pool *WorkerPool, metrics *MetricsCollector) {
	for {
		msg, err := client.Receive()
		if err != nil {
			log.Printf("[mqtt-consumer] receive: %v", err)
			continue
		}
		select {
		case pool.inputChannel <- msg:
		default:
			metrics.mqttDropped.Inc()
			log.Printf("[mqtt-consumer] worker channel full — dropping topic=%s", msg.Topic)
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
	log.Printf("[mqtt-consumer] metrics+health on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[mqtt-consumer] metrics server: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("[mqtt-consumer] starting...")

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[mqtt-consumer] config error: %v", err)
	}
	cfgHolder := NewConfigHolder(cfg)

	shutdown, err := setupTracing("mqtt-consumer")
	if err != nil {
		log.Fatalf("[mqtt-consumer] tracing: %v", err)
	}
	defer shutdown(context.Background())

	metrics := NewMetricsCollector()
	rdb := initRedisClusterClient(cfg.Redis)

	// FIX: chConn is driver.Conn (interface), not *driver.Conn (pointer to interface).
	var chConn driver.Conn
	if cfg.ClickHouse.DirectWriteEnabled {
		chConn, err = initClickHouseClient(cfg.ClickHouse)
		if err != nil {
			log.Printf("[mqtt-consumer] ClickHouse unavailable (direct write disabled): %v", err)
			chConn = nil
		}
	}

	pool   := NewWorkerPool(cfgHolder, rdb, chConn, metrics)
	client := initMQTTClient(cfg.MQTT)

	pool.Start()
	go startMetricsServer(cfg.Metrics)
	go handleMQTTMessages(client, pool, metrics)

	log.Printf("[mqtt-consumer] running (instance=%s port=%d workers=%d)",
		envOr("INSTANCE_ID", "mqtt-consumer-0"), cfg.Metrics.Port, cfg.Processing.Workers)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("[mqtt-consumer] shutting down...")
	client.Disconnect(250)
	pool.Stop()
	log.Println("[mqtt-consumer] done")
}
