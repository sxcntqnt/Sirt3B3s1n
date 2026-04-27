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
	Redis      RedisConfig
	ClickHouse ClickHouseConfig
	Processing ProcessingConfig
	DLQ        DLQConfig
	Metrics    MetricsConfig
}

// Main function
func main() {
	log.Println("Starting Batch Writer for GPS architecture...")

	// Load configuration
	config := loadConfig()

	// Initialize tracing
	tracerProvider, err := setupTracing("batch-writer")
	if err != nil {
		log.Fatalf("Failed to setup tracing: %v", err)
	}
	defer tracerProvider.Shutdown()

	// Initialize metrics
	metrics := NewBatchWriterMetrics()

	// Initialize Redis client
	redisClient := initRedisClusterClient(config.Redis)

	// Initialize ClickHouse writer
	clickhouseWriter, err := NewClickHouseWriter(config.ClickHouse)
	if err != nil {
		log.Fatalf("ClickHouse writer initialization failed: %v", err)
	}

	// Initialize DLQ processor
	dlqProcessor := NewDLQProcessor(config.DLQ, redisClient, metrics)

	// Initialize batch processor
	batchProcessor := NewBatchProcessor(config.Processing, redisClient, clickhouseWriter, dlqProcessor, metrics)

	// Start metrics server
	go startMetricsServer(config.Metrics)

	// Start batch processing
	batchProcessor.Start()

	// Start DLQ processing
	dlqProcessor.Start()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Batch Writer running...")

	<-sigChan
	log.Println("Shutting down Batch Writer...")

	// Clean shutdown
	batchProcessor.Stop()
	dlqProcessor.Stop()

	log.Println("Batch Writer shutdown complete")
}

// Configuration loading
func loadConfig() Config {
	return Config{
		Redis: RedisConfig{
			Cluster:      true,
			Nodes:        []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password:     osGetenv("REDIS_PASSWORD"),
			ConsumerGroup: "batch-writers",
			ConsumerName:  "batch-writer",
			Streams:       RedisStreamConfig{
				Batch: "gps:batch:{orgId}",
				DLQ:   "gps:dlq:{orgId}",
			},
			ReadConfig: RedisReadConfig{
				BatchSize: 10000,
				BlockTime: 5000,
			},
		},
		ClickHouse: ClickHouseConfig{
			Host:     os.Getenv("CLICKHOUSE_HOST"),
			Username: os.Getenv("CLICKHOUSE_USERNAME"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			Database: "default",
			Table:    "gps_events",
			Optimization: ClickHouseOptimization{
				BatchSize:      10000,
				MaxRowsPerBatch: 100000,
				FlushInterval:  "5s",
				ParallelWrites: 4,
				Compression:    "lz4",
			},
			Performance: ClickHousePerformance{
				MaxRetries: 3,
				RetryBackoff: "1s",
				ConnectionPool: ConnectionPool{
					MaxOpen:    20,
					MaxIdle:    10,
					MaxLifetime: "5m",
				},
			},
	},
		Processing: ProcessingConfig{
			Workers:       4,
			BufferSize:    50000,
			BatchProcessing: BatchProcessing{
				MaxBatchRows:      10000,
				MaxBatchSizeBytes: 10485760,
				OrgBatchSize:      1000,
				TimeBatchWindow:   "5s",
			},
		},
		DLQ: DLQConfig{
			MaxRetries:      3,
			RetryInterval:   "60s",
			MaxAgeDays:      7,
			Processing: DLQProcessing{
				Workers:   2,
				BatchSize: 1000,
				Interval:  "300s",
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
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(0.05))),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

// Batch Writer metrics
type BatchWriterMetrics struct {
	redisStreamLag      prometheus.Gauge
	batchWriteDuration  prometheus.Histogram
	rowsInserted        prometheus.Counter
	dlqSize             prometheus.Gauge
	clickhouseErrors    prometheus.Counter
	redisReadErrors     prometheus.Counter
	batchSize           prometheus.Histogram
}

func NewBatchWriterMetrics() *BatchWriterMetrics {
	metrics := &BatchWriterMetrics{}

	metrics.redisStreamLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "batch_writer_redis_stream_lags",
		Help: "Number of messages lagging in Redis streams",
	})

	metrics.batchWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "batch_writer_batch_write_duration_ms",
		Help:    "Batch write duration to ClickHouse in milliseconds",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000},
	})

	metrics.rowsInserted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "batch_writer_rows_inserted_total",
		Help: "Total number of rows inserted to ClickHouse",
	})

	// Register metrics
	prometheus.MustRegister(
		metrics.redisStreamLag,
		metrics.batchWriteDuration,
		metrics.rowsInserted,
		metrics.dlqSize,
		metrics.clickhouseErrors,
		metrics.redisReadErrors,
		metrics.batchSize,
	)

	return metrics
}

// Start metrics server
func startMetricsServer(config MetricsConfig) {
	log.Printf("Starting metrics server on port %d", config.Port)

	http.Handle(config.Path, promhttp.Handler())

	if err := http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

// Batch Processor implementation
type BatchProcessor struct {
	redisClient    *redis.Client
	chClient       *clickhouse.Conn
	dlqProcessor   *DLQProcessor
	metrics        *BatchWriterMetrics
	config         ProcessingConfig
	stopChan       chan bool
}

func NewBatchProcessor(config ProcessingConfig, redisClient *redis.Client, 
	chClient *clickhouse.Conn, dlqProcessor *DLQProcessor, metrics *BatchWriterMetrics) *BatchProcessor {
	return &BatchProcessor{
		redisClient:    redisClient,
		chClient:       chClient,
		dlqProcessor:   dlqProcessor,
		metrics:        metrics,
		config:         config,
		stopChan:       make(chan bool),
	}
}

func (p *BatchProcessor) Start() {
	go p.processBatchLoop()
}

func (p *BatchProcessor) Stop() {
	p.stopChan <- true
}

func (p *BatchProcessor) processBatchLoop() {
	for {
		// Check for stop signal
		select {
		case <-p.stopChan:
			return
		default:
		}

		// Read from Redis streams
		messages, err := p.readBatchFromRedis()
		if err != nil {
			p.metrics.redisReadErrors.Inc()
			time.Sleep(1 * time.Second)
			continue
		}

		if len(messages) == 0 {
			// No messages, sleep briefly
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Process batch
		startTime := time.Now()
		processedMessages, failedMessages := p.processBatch(messages)

		// Write to ClickHouse
		if len(processedMessages) > 0 {
			err = p.writeToClickhouse(processedMessages)
			if err != nil {
				p.metrics.clickhouseErrors.Inc()
				// Move failed messages to DLQ
				p.dlqProcessor.AddMessages(processedMessages)
			} else {
				// ACK successful messages
				p.ackRedisMessages(processedMessages)
				p.metrics.rowsInserted.Add(float64(len(processedMessages)))
				p.metrics.batchWriteDuration.Observe(float64(time.Since(startTime).Milliseconds()))
			}
		}

		// Handle failed messages
		if len(failedMessages) > 0 {
			p.dlqProcessor.AddMessages(failedMessages)
		}
	}
}

func (p *BatchProcessor) readBatchFromRedis() ([]RedisMessage, error) {
	streams := []string{"gps:batch:*"}
	streamArgs := redis.XReadGroupArgs{
		Group:    p.config.Redis.ConsumerGroup,
		Consumer: p.config.Redis.ConsumerName,
		Streams:  streams,
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

	messages := []RedisMessage{}
	for _, streamResult := range result {
		for _, message := range streamResult.Messages {
			msg := RedisMessage{
				Stream:   streamResult.Stream,
				ID:       message.ID,
				Values:   message.Values,
				OrgID:    extractOrgID(streamResult.Stream),
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

func (p *BatchProcessor) writeToClickhouse(messages []ProcessedMessage) error {
	startTime := time.Now()

	// Prepare batch
	batch, err := p.chClient.PrepareBatch(context.Background(), 
		fmt.Sprintf("INSERT INTO %s", p.config.ClickHouse.Table))
	
	if err != nil {
		return err
	}

	// Add rows to batch
	for _, msg := range messages {
		row := ClickHouseRow{
			EventID:        msg.EventID,
			TraceID:        msg.TraceID,
			VehicleID:      msg.VehicleID,
			OrganizationID: msg.OrgID,
			Latitude:       msg.Latitude,
			Longitude:      msg.Longitude,
			Altitude:       msg.Altitude,
			Speed:          msg.Speed,
			Heading:        msg.Heading,
			HDOP:           msg.HDOP,
			Satellites:     msg.Satellites,
			FixStatus:      msg.FixStatus,
			Rain:           msg.Rain,
			EventType:      msg.EventType,
			MovementFiltered: msg.MovementFiltered,
			DistanceFromLast: msg.DistanceFromLast,
			TimeSinceLast:   msg.TimeSinceLast,
			VehiclePlate:    msg.VehiclePlate,
			RouteID:         msg.RouteID,
			DriverID:        msg.DriverID,
			ConductorID:     msg.ConductorID,
			Capacity:        msg.Capacity,
			RawMessage:      msg.RawMessage,
			SchemaVersion:   msg.SchemaVersion,
			DeviceTimestamp: msg.DeviceTimestamp,
			ReceivedAt:      msg.ReceivedAt,
			ProcessedAt:     time.Now(),
			RecordedAt:      time.Now(),
		}

		err := batch.AppendStruct(row)
		if err != nil {
			continue
	}
	}

	// Execute batch insert
	err = batch.Send()
	if err != nil {
		return err
	}

	// Record metrics
	p.metrics.batchSize.Observe(float64(len(messages)))

	return nil
}
