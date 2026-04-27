package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Configuration structure
type Config struct {
	Server      ServerConfig
	Redis       RedisConfig
	ClickHouse  ClickHouseConfig
	Auth        AuthConfig
	Subscriptions SubscriptionConfig
	Messaging   MessagingConfig
	Metrics     MetricsConfig
	RateLimiting RateLimitingConfig
}

// Main function
func main() {
	log.Println("Starting WebSocket Gateway for GPS architecture...")

	// Load configuration
	config := loadConfig()

	// Initialize tracing
	tracerProvider, err := setupTracing("websocket-gateway")
	if err != nil {
		log.Fatalf("Failed to setup tracing: %v", err)
	}
	defer tracerProvider.Shutdown()

	// Initialize metrics
	metrics := NewWebSocketMetrics()

	// Initialize Redis client
	redisClient := initRedisClusterClient(config.Redis)

	// Initialize ClickHouse connection
	clickhouseClient, err := initClickHouseClient(config.ClickHouse)
	if err != nil {
		log.Printf("ClickHouse connection failed: %v", err)
	}

	// Initialize WebSocket backend
	backend := NewWebSocketBackend(config, redisClient, clickhouseClient, metrics)

	// Start metrics server
	go startMetricsServer(config.Metrics)

	// Start HTTP server with WebSocket handler
	go startHTTPServer(config.Server, backend)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("WebSocket Gateway running...")

	<-sigChan
	log.Println("Shutting down WebSocket Gateway...")

	// Clean shutdown
	backend.Shutdown()

	log.Println("WebSocket Gateway shutdown complete")
}

// Configuration loading
func loadConfig() Config {
	return Config{
		Server: ServerConfig{
			Port:             8080,
			Host:             "0.0.0.0",
			MaxConnections:   10000,
			MaxPayloadSize:   65536,
			PingInterval:     30000,
			PingTimeout:      10000,
			PerMessageDeflate: true,
			BackpressureLimit: 262144,
		},
		Redis: RedisConfig{
			Cluster: true,
			Nodes:   []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password: os.Getenv("REDIS_PASSWORD"),
			Streams: RedisStreamConfig{
				Realtime:      "gps:realtime:{orgId}",
				ConsumerGroup: "ws-gateway",
				ConsumerName:  "ws-gateway",
			},
		},
		ClickHouse: ClickHouseConfig{
			Host:          os.Getenv("CLICKHOUSE_HOST"),
			Username:      os.Getenv("CLICKHOUSE_USERNAME"),
			Password:      os.Getenv("CLICKHOUSE_PASSWORD"),
			Database:      "default",
			QueryTimeout:  5000,
			ConnectionPool: ConnectionPool{
				MaxSize:   10,
				MinIdle:   2,
				MaxLifetime: "5m",
			},
		},
		Auth: AuthConfig{
			ServiceURL:      os.Getenv("AUTH_SERVICE_URL"),
			JWTSecret:      os.Getenv("JWT_SECRET"),
			TokenExpiry:    900,
			RefreshThreshold: 300,
		},
		Subscriptions: SubscriptionConfig{
			MaxPerConnection:   1000,
			MaxVehiclesPerOrg:  10000,
			HeartbeatInterval:  30000,
			ReconnectTimeout:   10000,
			Backoff: BackoffConfig{
				Initial: 1000,
				Max:     10000,
				Factor:  2,
			},
		},
		Messaging: MessagingConfig{
			BatchSize:       50,
			FlushInterval:   100,
			MaxQueueSize:    10000,
			Priority: PriorityConfig{
				Critical: []string{"PANIC_BUTTON", "OVERSPEED", "GPS_SIGNAL_LOST"},
				High:     []string{"GEOFENCE_ENTER", "GEOFENCE_EXIT", "HARSH_BRAKING", "HARSH_ACCELERATION"},
				Normal:   []string{"NORMAL", "IGNITION_ON", "IGNITION_OFF"},
			},
		},
		Metrics: MetricsConfig{
			Port: 9090,
			Path: "/metrics",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		RateLimiting: RateLimitingConfig{
			Enabled:           true,
			MaxConnectionsPerIP: 100,
			MaxMessagesPerSecond: 1000,
			BurstSize:        100,
			WindowMs:         1000,
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
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(0.01))),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

// WebSocket metrics
type WebSocketMetrics struct {
	connectionsActive  prometheus.Gauge
	messagesPushed     prometheus.Counter
	clientBufferPressure prometheus.Gauge
	redisReadLag       prometheus.Gauge
	connectionErrors   prometheus.Counter
	authErrors         prometheus.Counter
	messageLatency     prometheus.Histogram
}

func NewWebSocketMetrics() *WebSocketMetrics {
	metrics := &WebSocketMetrics{}

	metrics.connectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "websocket_gateway_connections_active",
		Help: "Number of active WebSocket connections",
	})

	metrics.messagesPushed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "websocket_gateway_messages_pushed_total",
		Help: "Total number of messages pushed to clients",
	})

	metrics.messageLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "websocket_gateway_message_latency_ms",
		Help:    "Message delivery latency in milliseconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	})

	// Register metrics
	prometheus.MustRegister(
		metrics.connectionsActive,
		metrics.messagesPushed,
		metrics.clientBufferPressure,
		metrics.redisReadLag,
		metrics.connectionErrors,
		metrics.authErrors,
		metrics.messageLatency,
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

// Start HTTP server
func startHTTPServer(config ServerConfig, backend *WebSocketBackend) {
	// WebSocket upgrader
	upgrader := websocket.Upgrader{
		ReadBufferSize:  config.MaxPayloadSize,
		WriteBufferSize: config.MaxPayloadSize,
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins for now (configure in production)
		},
	}

	// HTTP handler
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// Authenticate JWT token
		orgId, userId, err := authenticateJWT(r.Header.Get("Authorization"), config.Auth)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Authentication failed"))
			return
		}

		// Apply rate limiting
		if config.RateLimiting.Enabled {
			if !checkRateLimit(r.RemoteAddr, orgId) {
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte("Rate limit exceeded"))
				return
			}
		}

		// Upgrade to WebSocket
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WebSocket upgrade error: %v", err)
			return
		}

		// Handle WebSocket connection
		backend.HandleConnection(conn, orgId, userId)
	})

	// Health endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Printf("Starting HTTP server on %s:%d", config.Host, config.Port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", config.Host, config.Port), nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// WebSocket backend implementation
type WebSocketBackend struct {
	config       Config
	redisClient  *redis.Client
	chClient     *clickhouse.Conn
	metrics      *WebSocketMetrics
	connections  map[string]*WebSocketConnection
}

func NewWebSocketBackend(config Config, redisClient *redis.Client, 
	chClient *clickhouse.Conn, metrics *WebSocketMetrics) *WebSocketBackend {
	return &WebSocketBackend{
		config:      config,
		redisClient: redisClient,
		chClient:    chClient,
		metrics:     metrics,
		connections: make(map[string]*WebSocketConnection),
	}
}

func (b *WebSocketBackend) Shutdown() {
	// Close all connections
	for _, conn := range b.connections {
		conn.Close()
	}
}

func (b *WebSocketBackend) HandleConnection(conn *websocket.Conn, orgId string, userId string) {
	connectionId := generateUUID()

	connection := &WebSocketConnection{
		ID:            connectionId,
		Conn:          conn,
		OrgID:         orgId,
		UserID:        userId,
		Subscriptions: make(map[string]bool),
	}

	b.connections[connectionId] = connection

	// Update metrics
	b.metrics.connectionsActive.Inc()

	// Start reading from Redis stream for this organization
	go b.startStreamReader(connection)

	// Handle incoming messages
	go b.handleIncomingMessages(connection)

	// Send initial vehicle positions
	go b.sendInitialPositions(connection)

	// Start heartbeat
	go b.startHeartbeat(connection)
}

func (b *WebSocketBackend) startStreamReader(connection *WebSocketConnection) {
	streamKey := fmt.Sprintf("gps:realtime:%s", connection.OrgID)

	// Use consumer group for fair distribution
	consumerArgs := redis.XReadGroupArgs{
		Group:    b.config.Redis.Streams.ConsumerGroup,
		Consumer: b.config.Redis.Streams.ConsumerName,
		Streams:  []string{streamKey, ">"},
		Count:    100,
		Block:    100, // milliseconds
	}

	for {
		result, err := b.redisClient.XReadGroup(context.Background(), &consumerArgs).Result()
		if err != nil {
			if err == redis.Nil {
				// No messages, continue
				continue
			}

			log.Printf("Stream read error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, streamResult := range result {
			for _, message := range streamResult.Messages {
				startTime := time.Now()

				positionUpdate := parsePositionUpdate(message.Values)

				// Check if vehicle is subscribed
				if connection.Subscriptions[positionUpdate.VehicleID] || connection.Subscriptions["all"] {
					// Send to WebSocket client
					b.sendPositionUpdate(connection, positionUpdate)

					// Update metrics
					b.metrics.messagesPushed.Inc()
					b.metrics.messageLatency.Observe(float64(time.Since(startTime).Milliseconds()))
				}

				// ACK message
				b.redisClient.XAck(context.Background(), streamKey, 
					b.config.Redis.Streams.ConsumerGroup, message.ID)
			}
		}

		// Check connection health
		if !b.isConnectionAlive(connection) {
			break
		}
	}
}

func (b *WebSocketBackend) sendInitialPositions(connection *WebSocketConnection) {
	// Query ClickHouse for latest vehicle positions
	query := fmt.Sprintf(
		"SELECT vehicle_id, latitude, longitude, speed, heading, recorded_at " +
		"FROM gps_events " +
		"WHERE organization_id = '%s' " +
		"AND recorded_at >= NOW() - INTERVAL 5 MINUTE " +
		"GROUP BY vehicle_id " +
		"ORDER BY recorded_at DESC",
		connection.OrgID
	)

	rows, err := b.chClient.Query(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return
	}

	var positions []VehiclePosition
	for rows.Next() {
		var pos VehiclePosition
		err := rows.ScanStruct(&pos)
		if err != nil {
			continue
		}
		positions = append(positions, pos)
	}

	// Send initial positions to client
	initialMessage := WebSocketMessage{
		Type:      "initial_positions",
		Positions: positions,
	}

	connection.Conn.WriteJSON(initialMessage)
}

func (b *WebSocketBackend) handleIncomingMessages(connection *WebSocketConnection) {
	for {
		var message WebSocketMessage
		err := connection.Conn.ReadJSON(&message)
		if err != nil {
			log.Printf("WebSocket read error: %v", err)
			b.removeConnection(connection.ID)
			return
		}

		switch message.Type {
		case "subscribe":
			// Handle subscription request
			b.handleSubscription(connection, message)
		
		case "unsubscribe":
			// Handle unsubscribe request
			b.handleUnsubscription(connection, message)
		
		case "ping":
			// Respond to ping
			connection.Conn.WriteJSON(WebSocketMessage{
				Type: "pong",
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}
}

func (b *WebSocketBackend) handleSubscription(connection *WebSocketConnection, message WebSocketMessage) {
	if message.All {
		// Subscribe to all vehicles
		connection.Subscriptions["all"] = true
	} else {
		// Subscribe to specific vehicles
		for _, vehicleId := message.VehicleIDs {
			connection.Subscriptions[vehicleId] = true
		}
	}

	// Send subscription confirmation
	connection.Conn.WriteJSON(WebSocketMessage{
		Type: "subscription_confirmation",
		SubscribedVehicles: len(connection.Subscriptions),
	})
}

func (b *WebSocketBackend) sendPositionUpdate(connection *WebSocketConnection, update PositionUpdate) {
	message := WebSocketMessage{
		Type:       "position",
		VehicleID:  update.VehicleID,
		Latitude:   update.Latitude,
		Longitude:  update.Longitude,
		Speed:      update.Speed,
		Heading:    update.Heading,
		Timestamp:  update.Timestamp,
	}

	connection.Conn.WriteJSON(message)
}
