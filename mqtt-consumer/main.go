package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Configuration structure
type Config struct {
	MQTT      MQTTConfig
	Redis     RedisConfig
	ClickHouse ClickHouseConfig
	Processing ProcessingConfig
	Metrics    MetricsConfig
}

// Main function
func main() {
	log.Println("Starting MQTT Consumer for GPS architecture...")

	// Load configuration
	config := loadConfig()

	// Initialize tracing
	tracerProvider, err := setupTracing("mqtt-consumer")
	if err != nil {
		log.Fatalf("Failed to setup tracing: %v", err)
	}
	defer tracerProvider.Shutdown()

	// Initialize metrics
	metrics := NewMetricsCollector()

	// Initialize Redis client
	redisClient := initRedisClient(config.Redis)

	// Initialize ClickHouse writer (if enabled)
	var clickhouseWriter *ClickHouseWriter
	if config.ClickHouse.DirectWriteEnabled {
		clickhouseWriter, err = NewClickHouseWriter(config.ClickHouse)
		if err != nil {
			log.Printf("ClickHouse writer initialization failed: %v", err)
		}
	}

	// Initialize worker pool
	workerPool := NewWorkerPool(config.Processing, redisClient, clickhouseWriter, metrics)

	// Initialize MQTT client
	mqttClient := initMQTTClient(config.MQTT)

	// Start worker pool
	workerPool.Start()

	// Start metrics server
	go startMetricsServer(config.Metrics)

	// Handle MQTT messages
	go handleMQTTMessages(mqttClient, workerPool)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("MQTT Consumer running...")

	<-sigChan
	log.Println("Shutting down MQTT Consumer...")
	
	// Clean shutdown
	mqttClient.Disconnect(250)
	workerPool.Stop()
	
	log.Println("MQTT Consumer shutdown complete")
}

// Configuration loading
func loadConfig() Config {
	return Config{
		MQTT: MQTTConfig{
			Broker:   os.Getenv("MQTT_BROKER"),
			Username: os.Getenv("MQTT_USERNAME"),
			Password: os.Getenv("MQTT_PASSWORD"),
			Topics:   []string{"gps/+/+"},
			QoS:      1,
		},
		Redis: RedisConfig{
			Cluster: true,
			Nodes:   []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password: os.Getenv("REDIS_PASSWORD"),
		},
		ClickHouse: ClickHouseConfig{
			DirectWriteEnabled: false,
			Host:              os.Getenv("CLICKHOUSE_HOST"),
			Username:          os.Getenv("CLICKHOUSE_USERNAME"),
			Password:          os.Getenv("CLICKHOUSE_PASSWORD"),
			Database:          "default",
			Table:             "gps_events",
			BatchSize:         100,
		},
		Processing: ProcessingConfig{
			Workers:       8,
			BufferSize:    10000,
			MovementFilter: MovementFilter{
				MinDistanceMeters: 20,
				MinTimeSeconds:    5,
			},
		},
		Metrics: MetricsConfig{
			Port: 9090,
			Path: "/metrics",
		},
	}
}

// Tracing setup
func setupTracing(serviceName string) (trace.TracerProvider, error) {
	// Set up OpenTelemetry with Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(
		jaeger.WithEndpoint(os.Getenv("JAEGER_ENDPOINT")),
	))
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", "production"),
		)),
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(0.1))),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

// Metrics collector
type MetricsCollector struct {
	eventsReceived      prometheus.Counter
	eventsProcessed     prometheus.Counter
	eventsFiltered      prometheus.Counter
	redisWriteErrors    prometheus.Counter
	clickhouseWriteErrors prometheus.Counter
	processingTime      prometheus.Histogram
	redisLatency        prometheus.Histogram
	bufferSize          prometheus.Gauge
	workerQueueLength   prometheus.Gauge
}

func NewMetricsCollector() *MetricsCollector {
	collector := &MetricsCollector{}

	collector.eventsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mqtt_consumer_events_received_total",
		Help: "Total number of MQTT events received",
	})

	collector.eventsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mqtt_consumer_events_processed_total",
		Help: "Total number of events processed",
	})

	collector.processingTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "mqtt_consumer_event_processing_duration_ms",
		Help:    "Event processing duration in milliseconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	})

	// Register all metrics
	prometheus.MustRegister(
		collector.eventsReceived,
		collector.eventsProcessed,
		collector.eventsFiltered,
		collector.redisWriteErrors,
		collector.clickhouseWriteErrors,
		collector.processingTime,
		collector.redisLatency,
		collector.bufferSize,
		collector.workerQueueLength,
	)

	return collector
}

// Start metrics server
func startMetricsServer(config MetricsConfig) {
	log.Printf("Starting metrics server on port %d", config.Port)
	
	http.Handle(config.Path, promhttp.Handler())
	
	if err := http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

// Worker pool implementation
type WorkerPool struct {
	workers      int
	inputChannel chan MQTTMessage
	redisClient  *redis.Client
	chClient     *clickhouse.Conn
	metrics      *MetricsCollector
}

func NewWorkerPool(config ProcessingConfig, redisClient *redis.Client, chClient *clickhouse.Conn, metrics *MetricsCollector) *WorkerPool {
	return &WorkerPool{
		workers:      config.Workers,
		inputChannel: make(chan MQTTMessage, config.BufferSize),
		redisClient:  redisClient,
		chClient:     chClient,
		metrics:      metrics,
	}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.workers; i++ {
		go p.processWorker(i)
	}
}

func (p *WorkerPool) Stop() {
	close(p.inputChannel)
}

func (p *WorkerPool) processWorker(id int) {
	for msg := range p.inputChannel {
		startTime := time.Now()

		// Parse and validate message
		event, err := parseMQTTMessage(msg)
		if err != nil {
			p.metrics.Increment("parse_errors")
			continue
		}

		// Enrich with vehicle metadata
		enrichedEvent, err := enrichEvent(event, p.redisClient)
		if err != nil {
			p.metrics.Increment("enrichment_errors")
		}

		// Apply movement filter
		if !shouldBroadcast(enrichedEvent) && !isCriticalEvent(enrichedEvent) {
			p.metrics.Increment("filtered_events")
			// Still write to batch stream for persistence
			writeToBatchStream(enrichedEvent, p.redisClient)
			continue
		}

		// Write to Redis streams
		err = writeToRedisStreams(enrichedEvent, p.redisClient)
		if err != nil {
			p.metrics.Increment("redis_write_errors")
			handleBackpressure(enrichedEvent)
		}

		// Direct ClickHouse write for critical events
		if p.chClient != nil && isCriticalEvent(enrichedEvent) {
			err = writeToClickhouse(enrichedEvent, p.chClient)
			if err != nil {
				p.metrics.Increment("clickhouse_write_errors")
			}
	}

		// Record processing time
		p.metrics.RecordProcessingTime(time.Since(startTime))
	}
}

// Handle MQTT messages
func handleMQTTMessages(client MQTTClient, workerPool *WorkerPool) {
	for {
		msg, err := client.Receive()
		if err != nil {
			log.Printf("MQTT receive error: %v", err)
			continue
		}

		workerPool.inputChannel <- msg
	}
}
