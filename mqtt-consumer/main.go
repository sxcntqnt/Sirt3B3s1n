// mqtt-consumer/main.go
// All types, clients, and helpers live in internal.go (same package).
// Fixes applied (scalability audit):
//  1. All 9 Prometheus fields initialised; Increment() and RecordProcessingTime() defined.
//  2. Metrics port from METRICS_PORT env.
//  3. ConsumerName from INSTANCE_ID env (was "batch-writer").
//  4. handleBackpressure() implemented with ring buffer.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config types
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
	Cluster       bool
	Nodes         []string
	Password      string
	ConsumerGroup string
	ConsumerName  string
}

type ClickHouseConfig struct {
	DirectWriteEnabled bool
	Host               string
	Username           string
	Password           string
	Database           string
	Table              string
	BatchSize          int
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

// ─────────────────────────────────────────────────────────────────────────────
// Config loader
// ─────────────────────────────────────────────────────────────────────────────

func loadConfig() Config {
	instanceID  := envOr("INSTANCE_ID", "mqtt-consumer-0")
	metricsPort := envInt("METRICS_PORT", 9090) // FIX: from env

	return Config{
		MQTT: MQTTConfig{
			Broker:   os.Getenv("MQTT_BROKER"),
			Username: os.Getenv("MQTT_USERNAME"),
			Password: os.Getenv("MQTT_PASSWORD"),
			Topics:   []string{"gps/+/+"},
			QoS:      1,
		},
		Redis: RedisConfig{
			Cluster:       true,
			Nodes:         []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password:      os.Getenv("REDIS_PASSWORD"),
			ConsumerGroup: "mqtt-consumers",
			ConsumerName:  instanceID, // FIX: was "batch-writer"
		},
		ClickHouse: ClickHouseConfig{
			DirectWriteEnabled: false,
			Host:               os.Getenv("CLICKHOUSE_HOST"),
			Username:           os.Getenv("CLICKHOUSE_USERNAME"),
			Password:           os.Getenv("CLICKHOUSE_PASSWORD"),
			Database:           "default",
			Table:              "gps_events",
			BatchSize:          100,
		},
		Processing: ProcessingConfig{
			Workers:    8,
			BufferSize: 10000,
			MovementFilter: MovementFilter{
				MinDistanceMeters: 20,
				MinTimeSeconds:    5,
			},
			Backpressure: BackpressureConfig{
				MaxRedisLatencyMs: 50,
				LocalBufferSize:   10000,
			},
		},
		Metrics: MetricsConfig{Port: metricsPort, Path: "/metrics"},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
// FIX 1: all 9 fields initialised (6 were nil → MustRegister panic)
// FIX 2: Increment() and RecordProcessingTime() methods now defined
// ─────────────────────────────────────────────────────────────────────────────

type MetricsCollector struct {
	eventsReceived        prometheus.Counter
	eventsProcessed       prometheus.Counter
	eventsFiltered        prometheus.Counter
	redisWriteErrors      prometheus.Counter
	clickhouseWriteErrors prometheus.Counter
	processingTime        prometheus.Histogram
	redisLatency          prometheus.Histogram
	bufferSize            prometheus.Gauge
	workerQueueLength     prometheus.Gauge
}

func NewMetricsCollector() *MetricsCollector {
	m := &MetricsCollector{
		eventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_received_total"}),
		eventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_processed_total"}),
		eventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_filtered_total"}),
		redisWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_redis_write_errors_total"}),
		clickhouseWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_clickhouse_write_errors_total"}),
		processingTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_consumer_event_processing_duration_ms",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),
		redisLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_consumer_redis_write_latency_ms",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
		}),
		bufferSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_consumer_backpressure_buffer_size"}),
		workerQueueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_consumer_worker_queue_length"}),
	}
	prometheus.MustRegister(
		m.eventsReceived, m.eventsProcessed, m.eventsFiltered,
		m.redisWriteErrors, m.clickhouseWriteErrors, m.processingTime,
		m.redisLatency, m.bufferSize, m.workerQueueLength,
	)
	return m
}

// Increment is the named counter helper called from processWorker.
// FIX: was called but not defined → compile error.
func (m *MetricsCollector) Increment(name string) {
	switch name {
	case "filtered_events":
		m.eventsFiltered.Inc()
	case "redis_write_errors":
		m.redisWriteErrors.Inc()
	case "clickhouse_write_errors":
		m.clickhouseWriteErrors.Inc()
	}
}

// RecordProcessingTime records per-event latency.
// FIX: was called but not defined → compile error.
func (m *MetricsCollector) RecordProcessingTime(d time.Duration) {
	m.processingTime.Observe(float64(d.Milliseconds()))
	m.eventsProcessed.Inc()
}

func (m *MetricsCollector) ObserveRedisLatency(d time.Duration) {
	m.redisLatency.Observe(float64(d.Milliseconds()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Backpressure ring buffer
// FIX: handleBackpressure was called but never defined.
// Architecture spec: lag > 50 ms → divert to local 10 000-slot buffer.
// Critical events retained; normal events dropped first (hot-path-first).
// ─────────────────────────────────────────────────────────────────────────────

type BackpressureBuffer struct {
	mu       sync.Mutex
	normal   []EnrichedEvent // defined in internal.go
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
	if isCriticalEvent(ev) { // defined in internal.go
		if len(b.critical) < b.capacity/2 {
			b.critical = append(b.critical, ev)
		}
	} else {
		if len(b.normal)+len(b.critical) >= b.capacity && len(b.normal) > 0 {
			b.normal = b.normal[1:] // evict oldest normal event
		}
		b.normal = append(b.normal, ev)
	}
	b.metrics.bufferSize.Set(float64(len(b.normal) + len(b.critical)))
}

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
	workers      int
	inputChannel chan MQTTMessage // defined in internal.go
	redisClient  *redis.ClusterClient
	chConn       *driver.Conn
	metrics      *MetricsCollector
	bpBuffer     *BackpressureBuffer
	bpMaxLatency time.Duration
	stopChan     chan struct{}
}

func NewWorkerPool(cfg ProcessingConfig, rdb *redis.ClusterClient, ch *driver.Conn, m *MetricsCollector) *WorkerPool {
	return &WorkerPool{
		workers:      cfg.Workers,
		inputChannel: make(chan MQTTMessage, cfg.BufferSize),
		redisClient:  rdb,
		chConn:       ch,
		metrics:      m,
		bpBuffer:     NewBackpressureBuffer(cfg.Backpressure.LocalBufferSize, m),
		bpMaxLatency: time.Duration(cfg.Backpressure.MaxRedisLatencyMs) * time.Millisecond,
		stopChan:     make(chan struct{}),
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

			// parseMQTTMessage, enrichEvent, shouldBroadcast, isCriticalEvent
			// writeToBatchStream, writeToRedisStreams, writeToClickhouse
			// — all defined in internal.go
			event, err := parseMQTTMessage(msg)
			if err != nil {
				p.metrics.Increment("parse_errors")
				continue
			}

			enriched, _ := enrichEvent(event, p.redisClient)

			if !shouldBroadcast(enriched) && !isCriticalEvent(enriched) {
				p.metrics.Increment("filtered_events")
				writeToBatchStream(enriched, p.redisClient)
				continue
			}

			if err = p.timedRedisWrite(enriched); err != nil {
				p.metrics.Increment("redis_write_errors")
				p.handleBackpressure(enriched) // FIX: now defined below
			}

			if p.chConn != nil && isCriticalEvent(enriched) {
				if err = writeToClickhouse(enriched, p.chConn); err != nil {
					p.metrics.Increment("clickhouse_write_errors")
				}
			}

			p.metrics.RecordProcessingTime(time.Since(start))
		}
	}
}

// timedRedisWrite writes to both Redis streams and records XADD latency for
// the backpressure degraded-mode flag.
func (p *WorkerPool) timedRedisWrite(ev EnrichedEvent) error {
	start := time.Now()
	err := writeToRedisStreams(ev, p.redisClient)
	latency := time.Since(start)
	p.metrics.ObserveRedisLatency(latency)
	p.bpBuffer.SetDegraded(latency > p.bpMaxLatency)
	return err
}

// handleBackpressure diverts an event to the local ring buffer when Redis is
// slow or unavailable.
// FIX: was called in processWorker but never defined → compile error.
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

func (p *WorkerPool) flushBuffer() {
	for p.bpBuffer.Len() > 0 {
		events := p.bpBuffer.Drain(200)
		for _, ev := range events {
			if err := p.timedRedisWrite(ev); err != nil {
				p.bpBuffer.Push(ev)
				return
			}
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
// Metrics + health — FIX: port from env
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

func handleMQTTMessages(client MQTTClient, pool *WorkerPool) {
	for {
		msg, err := client.Receive()
		if err != nil {
			log.Printf("[mqtt-consumer] receive: %v", err)
			continue
		}
		pool.inputChannel <- msg
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[mqtt-consumer] starting...")
	cfg := loadConfig()

	// setupTracing defined in internal.go; returns (func(ctx) error, error)
	shutdown, err := setupTracing("mqtt-consumer")
	if err != nil {
		log.Fatalf("[mqtt-consumer] tracing: %v", err)
	}
	defer shutdown(context.Background())

	metrics := NewMetricsCollector()
	rdb     := initRedisClusterClient(cfg.Redis) // internal.go

	var chConn *driver.Conn
	if cfg.ClickHouse.DirectWriteEnabled {
		chConn, err = initClickHouseClient(cfg.ClickHouse) // internal.go
		if err != nil {
			log.Printf("[mqtt-consumer] ClickHouse unavailable: %v", err)
		}
	}

	pool   := NewWorkerPool(cfg.Processing, rdb, chConn, metrics)
	client := initMQTTClient(cfg.MQTT) // internal.go

	pool.Start()
	go startMetricsServer(cfg.Metrics)
	go handleMQTTMessages(client, pool)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("[mqtt-consumer] running (instance=%s port=%d)",
		envOr("INSTANCE_ID", "mqtt-consumer-0"), cfg.Metrics.Port)

	<-sig
	log.Println("[mqtt-consumer] shutting down...")
	client.Disconnect(250)
	pool.Stop()
	log.Println("[mqtt-consumer] done")
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
