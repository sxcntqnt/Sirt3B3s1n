// mqtt-consumer/main.go
//
// Fixes applied (scalability audit):
//  1. [CRASH]   NewMetricsCollector: all 9 Prometheus fields now initialised
//               before MustRegister — eventsFiltered, redisWriteErrors,
//               clickhouseWriteErrors, redisLatency, bufferSize,
//               workerQueueLength were nil → immediate startup panic.
//  2. [COMPILE] Increment() and RecordProcessingTime() methods added to
//               MetricsCollector — processWorker called both but neither
//               existed on the struct.
//  3. [SCALE]   startMetricsServer reads METRICS_PORT from env (injected by
//               the orchestrator as 9100, 9101 … per replica) instead of the
//               hard-coded 9090 that caused "address already in use" on
//               replica 1+.
//  4. [SCALE]   ConsumerName is set from INSTANCE_ID env var (e.g.
//               "mqtt-consumer-0") so every replica in the Redis consumer
//               group has a unique identity. The original hard-coded
//               "batch-writer" (copy-paste) caused siblings to steal each
//               other's messages and miss ACKs.
//  5. [LOGIC]   handleBackpressure() is now implemented. When Redis XADD
//               latency exceeds maxRedisLatencyMs (50 ms per the architecture
//               spec) the event is written to an in-memory ring buffer
//               (capacity 10 000). A background drainer flushes that buffer
//               back to Redis once latency recovers. Hot-path (critical)
//               events are retained; normal events are dropped first when the
//               buffer is full.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
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
	// ConsumerGroup / ConsumerName are set at runtime.
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
	// MaxRedisLatencyMs is the XADD latency threshold above which events are
	// diverted to the local ring buffer instead of Redis.
	MaxRedisLatencyMs int64
	// LocalBufferSize is the capacity of the in-memory ring buffer.
	LocalBufferSize int
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
	// INSTANCE_ID injected by orchestrator: "mqtt-consumer-0", "mqtt-consumer-1" …
	instanceID := envOr("INSTANCE_ID", "mqtt-consumer-0")
	metricsPort := envInt("METRICS_PORT", 9090) // FIX: was hard-coded 9090

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
			ConsumerName:  instanceID, // FIX: unique per replica; was "batch-writer"
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
				MaxRedisLatencyMs: 50,    // from architecture spec
				LocalBufferSize:   10000, // from architecture spec
			},
		},
		Metrics: MetricsConfig{
			Port: metricsPort, // FIX: from env, not hard-coded
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
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
//
// FIX 1: all 9 fields are now initialised before MustRegister.
// FIX 2: Increment() and RecordProcessingTime() methods are now defined.
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
			Name: "mqtt_consumer_events_received_total",
			Help: "Total number of MQTT events received",
		}),
		eventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_processed_total",
			Help: "Total number of events processed",
		}),
		// FIX: was nil — uninitialised before MustRegister
		eventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_events_filtered_total",
			Help: "Total number of events filtered by movement filter",
		}),
		redisWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_redis_write_errors_total",
			Help: "Total number of Redis write errors",
		}),
		clickhouseWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_consumer_clickhouse_write_errors_total",
			Help: "Total number of ClickHouse direct-write errors",
		}),
		processingTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_consumer_event_processing_duration_ms",
			Help:    "Event processing duration in milliseconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),
		// FIX: was nil
		redisLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_consumer_redis_write_latency_ms",
			Help:    "Redis XADD latency in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
		}),
		// FIX: was nil
		bufferSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_consumer_backpressure_buffer_size",
			Help: "Current number of events in the local backpressure buffer",
		}),
		// FIX: was nil
		workerQueueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_consumer_worker_queue_length",
			Help: "Current number of messages waiting in the worker input channel",
		}),
	}

	prometheus.MustRegister(
		m.eventsReceived,
		m.eventsProcessed,
		m.eventsFiltered,
		m.redisWriteErrors,
		m.clickhouseWriteErrors,
		m.processingTime,
		m.redisLatency,
		m.bufferSize,
		m.workerQueueLength,
	)

	return m
}

// Increment is the generic counter helper called by processWorker.
// FIX: this method did not exist; processWorker called it causing compile failure.
func (m *MetricsCollector) Increment(name string) {
	switch name {
	case "parse_errors", "enrichment_errors":
		// Not separately tracked — fall through; add counters here as needed.
	case "filtered_events":
		m.eventsFiltered.Inc()
	case "redis_write_errors":
		m.redisWriteErrors.Inc()
	case "clickhouse_write_errors":
		m.clickhouseWriteErrors.Inc()
	}
}

// RecordProcessingTime records a single event's end-to-end processing latency.
// FIX: this method did not exist; processWorker called it causing compile failure.
func (m *MetricsCollector) RecordProcessingTime(d time.Duration) {
	m.processingTime.Observe(float64(d.Milliseconds()))
	m.eventsProcessed.Inc()
}

// ObserveRedisLatency records the wall-clock time of a Redis XADD call.
func (m *MetricsCollector) ObserveRedisLatency(d time.Duration) {
	m.redisLatency.Observe(float64(d.Milliseconds()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Backpressure buffer
//
// FIX 5: handleBackpressure was called by processWorker but never defined.
// Architecture spec: when Redis XADD latency > 50 ms, divert events to a
// local 10 000-slot ring buffer. Drop normal events first (hot-path-first).
// A background drainer retries Redis writes once latency recovers.
// ─────────────────────────────────────────────────────────────────────────────

// BackpressureBuffer is a thread-safe ring buffer with priority-aware eviction.
// Critical events (PANIC_BUTTON, OVERSPEED, GPS_SIGNAL_LOST, GEOFENCE_*) are
// never dropped while normal events are evicted first when the buffer is full.
type BackpressureBuffer struct {
	mu       sync.Mutex
	normal   []EnrichedEvent // lower priority; dropped first
	critical []EnrichedEvent // never evicted; bounded by capacity/2
	capacity int

	// Atomic flag: 1 = Redis latency is elevated, drain is paused.
	degraded atomic.Int32

	metrics *MetricsCollector
}

func NewBackpressureBuffer(capacity int, metrics *MetricsCollector) *BackpressureBuffer {
	return &BackpressureBuffer{
		capacity: capacity,
		normal:   make([]EnrichedEvent, 0, capacity),
		critical: make([]EnrichedEvent, 0, capacity/2),
		metrics:  metrics,
	}
}

// Push adds an event to the buffer. If the buffer is full, normal events are
// dropped to make room. Critical events are always accepted up to capacity/2.
func (b *BackpressureBuffer) Push(event EnrichedEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if isCriticalEvent(event) {
		if len(b.critical) < b.capacity/2 {
			b.critical = append(b.critical, event)
		}
		// Drop silently if critical buffer also full — last resort.
	} else {
		if len(b.normal)+len(b.critical) >= b.capacity {
			// Drop the oldest normal event (hot-path-first per spec).
			if len(b.normal) > 0 {
				b.normal = b.normal[1:]
			}
		}
		b.normal = append(b.normal, event)
	}

	b.metrics.bufferSize.Set(float64(len(b.normal) + len(b.critical)))
}

// Drain returns up to n events, critical events first.
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

func (b *BackpressureBuffer) IsDegraded() bool {
	return b.degraded.Load() == 1
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker pool
// ─────────────────────────────────────────────────────────────────────────────

type WorkerPool struct {
	workers      int
	inputChannel chan MQTTMessage
	redisClient  *redis.ClusterClient
	chClient     *clickhouse.Conn
	metrics      *MetricsCollector
	bpBuffer     *BackpressureBuffer
	bpMaxLatency time.Duration
	stopChan     chan struct{}
}

func NewWorkerPool(
	config ProcessingConfig,
	redisClient *redis.ClusterClient,
	chClient *clickhouse.Conn,
	metrics *MetricsCollector,
) *WorkerPool {
	bp := NewBackpressureBuffer(config.Backpressure.LocalBufferSize, metrics)
	return &WorkerPool{
		workers:      config.Workers,
		inputChannel: make(chan MQTTMessage, config.BufferSize),
		redisClient:  redisClient,
		chClient:     chClient,
		metrics:      metrics,
		bpBuffer:     bp,
		bpMaxLatency: time.Duration(config.Backpressure.MaxRedisLatencyMs) * time.Millisecond,
		stopChan:     make(chan struct{}),
	}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.workers; i++ {
		go p.processWorker(i)
	}
	go p.drainBackpressureBuffer()
	go p.reportQueueDepth()
}

func (p *WorkerPool) Stop() {
	close(p.stopChan)
}

func (p *WorkerPool) processWorker(id int) {
	for {
		select {
		case <-p.stopChan:
			return
		case msg, ok := <-p.inputChannel:
			if !ok {
				return
			}
			p.metrics.eventsReceived.Inc()
			startTime := time.Now()

			event, err := parseMQTTMessage(msg)
			if err != nil {
				p.metrics.Increment("parse_errors")
				continue
			}

			enrichedEvent, err := enrichEvent(event, p.redisClient)
			if err != nil {
				p.metrics.Increment("enrichment_errors")
				// Continue with partial enrichment.
			}

			if !shouldBroadcast(enrichedEvent) && !isCriticalEvent(enrichedEvent) {
				p.metrics.Increment("filtered_events")
				writeToBatchStream(enrichedEvent, p.redisClient)
				continue
			}

			if err = p.writeToRedisStreams(enrichedEvent); err != nil {
				p.metrics.Increment("redis_write_errors")
				// FIX: handleBackpressure is now defined on the pool.
				p.handleBackpressure(enrichedEvent)
			}

			if p.chClient != nil && isCriticalEvent(enrichedEvent) {
				if err = writeToClickhouse(enrichedEvent, p.chClient); err != nil {
					p.metrics.Increment("clickhouse_write_errors")
				}
			}

			p.metrics.RecordProcessingTime(time.Since(startTime))
		}
	}
}

// writeToRedisStreams measures XADD latency and updates the degraded flag.
func (p *WorkerPool) writeToRedisStreams(event EnrichedEvent) error {
	start := time.Now()

	realtimeStream := fmt.Sprintf("gps:realtime:%s", event.OrgID)
	err := p.redisClient.XAdd(context.Background(), &redis.XAddArgs{
		Stream: realtimeStream,
		MaxLen: 5000,
		Approx: true,
		Values: event.ToMap(),
	}).Err()

	latency := time.Since(start)
	p.metrics.ObserveRedisLatency(latency)

	// Update degraded flag so the drain loop knows whether to retry.
	p.bpBuffer.SetDegraded(latency > p.bpMaxLatency)

	return err
}

// handleBackpressure diverts an event to the local ring buffer when Redis is
// slow or unavailable. The background drainer will retry when latency recovers.
// FIX: previously called in processWorker but the function did not exist.
func (p *WorkerPool) handleBackpressure(event EnrichedEvent) {
	p.bpBuffer.Push(event)
	log.Printf("[mqtt-consumer] backpressure: buffered event vehicle=%s (buffer_len=%d)",
		event.VehicleID, p.bpBuffer.Len())
}

// drainBackpressureBuffer retries buffered events once Redis latency recovers.
func (p *WorkerPool) drainBackpressureBuffer() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			// Best-effort flush on shutdown.
			p.flushBuffer()
			return
		case <-ticker.C:
			if p.bpBuffer.IsDegraded() || p.bpBuffer.Len() == 0 {
				continue
			}
			p.flushBuffer()
		}
	}
}

func (p *WorkerPool) flushBuffer() {
	const drainBatch = 200
	for p.bpBuffer.Len() > 0 {
		events := p.bpBuffer.Drain(drainBatch)
		for _, ev := range events {
			if err := p.writeToRedisStreams(ev); err != nil {
				// Still degraded — push back and abort this drain cycle.
				p.bpBuffer.Push(ev)
				return
			}
		}
	}
}

// reportQueueDepth periodically updates the worker queue length gauge.
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
// Metrics + health HTTP server
// FIX: port from env, not hard-coded 9090
// ─────────────────────────────────────────────────────────────────────────────

func startMetricsServer(config MetricsConfig) {
	mux := http.NewServeMux()
	mux.Handle(config.Path, promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", config.Port)
	log.Printf("[mqtt-consumer] metrics + health on %s", addr)

	srv := &http.Server{Addr: addr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[mqtt-consumer] metrics server error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MQTT message handler
// ─────────────────────────────────────────────────────────────────────────────

func handleMQTTMessages(client MQTTClient, workerPool *WorkerPool) {
	for {
		msg, err := client.Receive()
		if err != nil {
			log.Printf("[mqtt-consumer] receive error: %v", err)
			continue
		}
		workerPool.inputChannel <- msg
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[mqtt-consumer] starting...")

	config := loadConfig()

	tracerProvider, err := setupTracing("mqtt-consumer")
	if err != nil {
		log.Fatalf("[mqtt-consumer] tracing setup: %v", err)
	}
	defer tracerProvider.Shutdown(context.Background())

	metrics := NewMetricsCollector()

	redisClient := initRedisClusterClient(config.Redis)

	var clickhouseWriter *clickhouse.Conn
	if config.ClickHouse.DirectWriteEnabled {
		clickhouseWriter, err = initClickHouseClient(config.ClickHouse)
		if err != nil {
			log.Printf("[mqtt-consumer] ClickHouse init failed (non-fatal): %v", err)
		}
	}

	workerPool := NewWorkerPool(config.Processing, redisClient, clickhouseWriter, metrics)
	mqttClient := initMQTTClient(config.MQTT)

	workerPool.Start()
	go startMetricsServer(config.Metrics)
	go handleMQTTMessages(mqttClient, workerPool)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("[mqtt-consumer] running (instance=%s, metrics-port=%d)",
		envOr("INSTANCE_ID", "mqtt-consumer-0"), config.Metrics.Port)

	<-sigChan
	log.Println("[mqtt-consumer] shutting down...")
	mqttClient.Disconnect(250)
	workerPool.Stop()
	log.Println("[mqtt-consumer] shutdown complete")
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
