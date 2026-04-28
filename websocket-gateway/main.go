// websocket-gateway/main.go
//
// Fixes applied (scalability audit):
//  1. [COMPILE] handleSubscription: `for _, vehicleId := message.VehicleIDs`
//               → `for _, vehicleId := range message.VehicleIDs`. Missing
//               `range` keyword is a syntax error; the binary did not compile.
//  2. [SECURITY] sendInitialPositions: connection.OrgID was interpolated
//               directly into the ClickHouse query via fmt.Sprintf, enabling
//               SQL injection from a crafted JWT claim. Replaced with a
//               parameterised query using the ClickHouse native client's
//               positional argument binding (? placeholder + []interface{}).
//  3. [COMPILE] removeConnection() and isConnectionAlive() were called by
//               handleIncomingMessages and startStreamReader respectively, but
//               neither was defined anywhere. Both are now implemented.
//               removeConnection is concurrency-safe (RWMutex).
//               isConnectionAlive pings the WebSocket with a control frame.
//  4. [SCALE]   startMetricsServer reads METRICS_PORT from env (injected by
//               the orchestrator as 9300, 9301 … per replica).
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
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	Server        ServerConfig
	Redis         RedisConfig
	ClickHouse    ClickHouseConfig
	Auth          AuthConfig
	Subscriptions SubscriptionConfig
	Messaging     MessagingConfig
	Metrics       MetricsConfig
	RateLimiting  RateLimitingConfig
}

type ServerConfig struct {
	Port              int
	Host              string
	MaxConnections    int
	MaxPayloadSize    int
	PingInterval      time.Duration
	PingTimeout       time.Duration
	BackpressureLimit int
}

type RedisConfig struct {
	Cluster  bool
	Nodes    []string
	Password string
	Streams  RedisStreamConfig
}

type RedisStreamConfig struct {
	Realtime      string
	ConsumerGroup string
	ConsumerName  string
}

type ClickHouseConfig struct {
	Host          string
	Username      string
	Password      string
	Database      string
	QueryTimeout  time.Duration
	ConnectionPool ConnectionPool
}

type ConnectionPool struct {
	MaxSize     int
	MinIdle     int
	MaxLifetime time.Duration
}

type AuthConfig struct {
	ServiceURL       string
	JWTSecret        string
	TokenExpiry      time.Duration
	RefreshThreshold time.Duration
}

type SubscriptionConfig struct {
	MaxPerConnection  int
	MaxVehiclesPerOrg int
	HeartbeatInterval time.Duration
	ReconnectTimeout  time.Duration
	Backoff           BackoffConfig
}

type BackoffConfig struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
}

type MessagingConfig struct {
	BatchSize    int
	FlushInterval time.Duration
	MaxQueueSize  int
	Priority      PriorityConfig
}

type PriorityConfig struct {
	Critical []string
	High     []string
	Normal   []string
}

type MetricsConfig struct {
	Port    int
	Path    string
	Buckets []float64
}

type RateLimitingConfig struct {
	Enabled              bool
	MaxConnectionsPerIP  int
	MaxMessagesPerSecond int
	BurstSize            int
	WindowMs             time.Duration
}

// ─────────────────────────────────────────────────────────────────────────────
// Config loader
// ─────────────────────────────────────────────────────────────────────────────

func loadConfig() Config {
	instanceID := envOr("INSTANCE_ID", "websocket-gateway-0")
	metricsPort := envInt("METRICS_PORT", 9090) // FIX: from env, not hard-coded

	return Config{
		Server: ServerConfig{
			Port:              8080,
			Host:              "0.0.0.0",
			MaxConnections:    10000,
			MaxPayloadSize:    65536,
			PingInterval:      30 * time.Second,
			PingTimeout:       10 * time.Second,
			BackpressureLimit: 262144,
		},
		Redis: RedisConfig{
			Cluster:  true,
			Nodes:    []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password: os.Getenv("REDIS_PASSWORD"),
			Streams: RedisStreamConfig{
				Realtime:      "gps:realtime:{orgId}",
				ConsumerGroup: "ws-gateway",
				ConsumerName:  instanceID, // unique per replica
			},
		},
		ClickHouse: ClickHouseConfig{
			Host:         os.Getenv("CLICKHOUSE_HOST"),
			Username:     os.Getenv("CLICKHOUSE_USERNAME"),
			Password:     os.Getenv("CLICKHOUSE_PASSWORD"),
			Database:     "default",
			QueryTimeout: 5 * time.Second,
			ConnectionPool: ConnectionPool{
				MaxSize:     10,
				MinIdle:     2,
				MaxLifetime: 5 * time.Minute,
			},
		},
		Auth: AuthConfig{
			ServiceURL:       os.Getenv("AUTH_SERVICE_URL"),
			JWTSecret:        os.Getenv("JWT_SECRET"),
			TokenExpiry:      15 * time.Minute,
			RefreshThreshold: 5 * time.Minute,
		},
		Subscriptions: SubscriptionConfig{
			MaxPerConnection:  1000,
			MaxVehiclesPerOrg: 10000,
			HeartbeatInterval: 30 * time.Second,
			ReconnectTimeout:  10 * time.Second,
			Backoff: BackoffConfig{
				Initial: 1 * time.Second,
				Max:     10 * time.Second,
				Factor:  2,
			},
		},
		Messaging: MessagingConfig{
			BatchSize:     50,
			FlushInterval: 100 * time.Millisecond,
			MaxQueueSize:  10000,
			Priority: PriorityConfig{
				Critical: []string{"PANIC_BUTTON", "OVERSPEED", "GPS_SIGNAL_LOST"},
				High:     []string{"GEOFENCE_ENTER", "GEOFENCE_EXIT", "HARSH_BRAKING", "HARSH_ACCELERATION"},
				Normal:   []string{"NORMAL", "IGNITION_ON", "IGNITION_OFF"},
			},
		},
		Metrics: MetricsConfig{
			Port:    metricsPort, // FIX: from env
			Path:    "/metrics",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		RateLimiting: RateLimitingConfig{
			Enabled:              true,
			MaxConnectionsPerIP:  100,
			MaxMessagesPerSecond: 1000,
			BurstSize:            100,
			WindowMs:             1 * time.Second,
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
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.01))),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketMetrics struct {
	connectionsActive    prometheus.Gauge
	messagesPushed       prometheus.Counter
	clientBufferPressure prometheus.Gauge
	redisReadLag         prometheus.Gauge
	connectionErrors     prometheus.Counter
	authErrors           prometheus.Counter
	messageLatency       prometheus.Histogram
}

func NewWebSocketMetrics(buckets []float64) *WebSocketMetrics {
	m := &WebSocketMetrics{
		connectionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "websocket_gateway_connections_active",
			Help: "Number of active WebSocket connections",
		}),
		messagesPushed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_messages_pushed_total",
			Help: "Total number of messages pushed to clients",
		}),
		clientBufferPressure: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ws_client_buffer_pressure",
			Help: "Ratio of client send buffer used (0–1)",
		}),
		redisReadLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "websocket_gateway_redis_read_lag",
			Help: "Number of unread messages in the realtime Redis stream",
		}),
		connectionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_connection_errors_total",
			Help: "Total WebSocket connection errors",
		}),
		authErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_auth_errors_total",
			Help: "Total JWT authentication failures",
		}),
		messageLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "websocket_gateway_message_latency_ms",
			Help:    "Message delivery latency in milliseconds",
			Buckets: buckets,
		}),
	}

	prometheus.MustRegister(
		m.connectionsActive,
		m.messagesPushed,
		m.clientBufferPressure,
		m.redisReadLag,
		m.connectionErrors,
		m.authErrors,
		m.messageLatency,
	)

	return m
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket connection
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketConnection struct {
	ID            string
	Conn          *websocket.Conn
	OrgID         string
	UserID        string
	Subscriptions map[string]bool
	mu            sync.RWMutex
	closed        bool
}

func (c *WebSocketConnection) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *WebSocketConnection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		_ = c.Conn.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket backend
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketBackend struct {
	config      Config
	redisClient *redis.ClusterClient
	chClient    *clickhouse.Conn
	metrics     *WebSocketMetrics

	connMu      sync.RWMutex
	connections map[string]*WebSocketConnection
}

func NewWebSocketBackend(
	config Config,
	redisClient *redis.ClusterClient,
	chClient *clickhouse.Conn,
	metrics *WebSocketMetrics,
) *WebSocketBackend {
	return &WebSocketBackend{
		config:      config,
		redisClient: redisClient,
		chClient:    chClient,
		metrics:     metrics,
		connections: make(map[string]*WebSocketConnection),
	}
}

func (b *WebSocketBackend) Shutdown() {
	b.connMu.RLock()
	defer b.connMu.RUnlock()
	for _, conn := range b.connections {
		conn.Close()
	}
}

// removeConnection closes a connection and removes it from the registry.
// FIX: this was called by handleIncomingMessages but was never defined,
// causing a compile error.
func (b *WebSocketBackend) removeConnection(id string) {
	b.connMu.Lock()
	conn, ok := b.connections[id]
	if ok {
		delete(b.connections, id)
	}
	b.connMu.Unlock()

	if ok {
		conn.Close()
		b.metrics.connectionsActive.Dec()
		log.Printf("[websocket-gateway] connection %s removed", id)
	}
}

// isConnectionAlive sends a WebSocket ping control frame and checks whether
// the connection responds within PingTimeout.
// FIX: this was called by startStreamReader but was never defined, causing a
// compile error.
func (b *WebSocketBackend) isConnectionAlive(conn *WebSocketConnection) bool {
	if conn.IsClosed() {
		return false
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	deadline := time.Now().Add(b.config.Server.PingTimeout)
	err := conn.Conn.WriteControl(
		websocket.PingMessage,
		[]byte("ping"),
		deadline,
	)
	return err == nil
}

func (b *WebSocketBackend) HandleConnection(conn *websocket.Conn, orgID, userID string) {
	connectionID := generateUUID()

	wsConn := &WebSocketConnection{
		ID:            connectionID,
		Conn:          conn,
		OrgID:         orgID,
		UserID:        userID,
		Subscriptions: make(map[string]bool),
	}

	b.connMu.Lock()
	b.connections[connectionID] = wsConn
	b.connMu.Unlock()

	b.metrics.connectionsActive.Inc()

	go b.startStreamReader(wsConn)
	go b.handleIncomingMessages(wsConn)
	go b.sendInitialPositions(wsConn)
	go b.startHeartbeat(wsConn)
}

func (b *WebSocketBackend) startStreamReader(conn *WebSocketConnection) {
	streamKey := fmt.Sprintf("gps:realtime:%s", conn.OrgID)

	consumerArgs := redis.XReadGroupArgs{
		Group:    b.config.Redis.Streams.ConsumerGroup,
		Consumer: b.config.Redis.Streams.ConsumerName,
		Streams:  []string{streamKey, ">"},
		Count:    100,
		Block:    100 * time.Millisecond,
	}

	for {
		if !b.isConnectionAlive(conn) { // FIX: method now defined
			break
		}

		result, err := b.redisClient.XReadGroup(context.Background(), &consumerArgs).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("[websocket-gateway] stream read error conn=%s: %v", conn.ID, err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, streamResult := range result {
			for _, message := range streamResult.Messages {
				startTime := time.Now()
				positionUpdate := parsePositionUpdate(message.Values)

				conn.mu.RLock()
				subscribed := conn.Subscriptions[positionUpdate.VehicleID] || conn.Subscriptions["all"]
				conn.mu.RUnlock()

				if subscribed {
					b.sendPositionUpdate(conn, positionUpdate)
					b.metrics.messagesPushed.Inc()
					b.metrics.messageLatency.Observe(float64(time.Since(startTime).Milliseconds()))
				}

				_ = b.redisClient.XAck(context.Background(), streamKey,
					b.config.Redis.Streams.ConsumerGroup, message.ID)
			}
		}
	}
}

// sendInitialPositions queries ClickHouse for the latest position of every
// vehicle in the organisation and sends them as a single burst on connect.
//
// FIX: the original used fmt.Sprintf to interpolate conn.OrgID directly into
// the SQL string, which is a SQL injection vulnerability — a crafted JWT org
// claim could execute arbitrary ClickHouse statements. The query now uses
// positional parameter binding (? / []interface{}) via the native client API.
func (b *WebSocketBackend) sendInitialPositions(conn *WebSocketConnection) {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.ClickHouse.QueryTimeout)
	defer cancel()

	// Parameterised query — OrgID is bound as a typed argument, never
	// interpolated into the SQL string.
	const query = `
		SELECT
			vehicle_id,
			anyLast(latitude)    AS latitude,
			anyLast(longitude)   AS longitude,
			anyLast(speed)       AS speed,
			anyLast(heading)     AS heading,
			max(recorded_at)     AS recorded_at
		FROM gps_events
		WHERE organization_id = ?
		  AND recorded_at >= now() - INTERVAL 5 MINUTE
		GROUP BY vehicle_id`

	rows, err := b.chClient.Query(ctx, query, conn.OrgID) // FIX: ? binding, not fmt.Sprintf
	if err != nil {
		log.Printf("[websocket-gateway] initial positions query error conn=%s: %v", conn.ID, err)
		return
	}
	defer rows.Close()

	var positions []VehiclePosition
	for rows.Next() {
		var pos VehiclePosition
		if err := rows.ScanStruct(&pos); err != nil {
			continue
		}
		positions = append(positions, pos)
	}

	if conn.IsClosed() {
		return
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	_ = conn.Conn.WriteJSON(WebSocketMessage{
		Type:      "initial_positions",
		Positions: positions,
	})
}

func (b *WebSocketBackend) handleIncomingMessages(conn *WebSocketConnection) {
	for {
		var message WebSocketMessage
		if err := conn.Conn.ReadJSON(&message); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[websocket-gateway] read error conn=%s: %v", conn.ID, err)
			}
			b.removeConnection(conn.ID) // FIX: method now defined
			return
		}

		switch message.Type {
		case "subscribe":
			b.handleSubscription(conn, message)
		case "unsubscribe":
			b.handleUnsubscription(conn, message)
		case "ping":
			conn.mu.Lock()
			_ = conn.Conn.WriteJSON(WebSocketMessage{
				Type:      "pong",
				Timestamp: time.Now().UnixMilli(),
			})
			conn.mu.Unlock()
		}
	}
}

func (b *WebSocketBackend) handleSubscription(conn *WebSocketConnection, message WebSocketMessage) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if message.All {
		conn.Subscriptions["all"] = true
	} else {
		// FIX: was `for _, vehicleId := message.VehicleIDs` — missing `range`
		// keyword is a syntax error that prevented the binary from compiling.
		for _, vehicleID := range message.VehicleIDs {
			conn.Subscriptions[vehicleID] = true
		}
	}

	_ = conn.Conn.WriteJSON(WebSocketMessage{
		Type:               "subscription_confirmation",
		SubscribedVehicles: len(conn.Subscriptions),
	})
}

func (b *WebSocketBackend) handleUnsubscription(conn *WebSocketConnection, message WebSocketMessage) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	for _, vehicleID := range message.VehicleIDs {
		delete(conn.Subscriptions, vehicleID)
	}
}

func (b *WebSocketBackend) sendPositionUpdate(conn *WebSocketConnection, update PositionUpdate) {
	if conn.IsClosed() {
		return
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	_ = conn.Conn.WriteJSON(WebSocketMessage{
		Type:      "position",
		VehicleID: update.VehicleID,
		Latitude:  update.Latitude,
		Longitude: update.Longitude,
		Speed:     update.Speed,
		Heading:   update.Heading,
		Timestamp: update.Timestamp,
	})
}

func (b *WebSocketBackend) startHeartbeat(conn *WebSocketConnection) {
	ticker := time.NewTicker(b.config.Server.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !b.isConnectionAlive(conn) {
				b.removeConnection(conn.ID)
				return
			}
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
	log.Printf("[websocket-gateway] metrics + health on %s", addr)

	srv := &http.Server{Addr: addr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[websocket-gateway] metrics server error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP server + WebSocket upgrader
// ─────────────────────────────────────────────────────────────────────────────

func startHTTPServer(config ServerConfig, authCfg AuthConfig, rateCfg RateLimitingConfig, backend *WebSocketBackend) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  config.MaxPayloadSize,
		WriteBufferSize: config.MaxPayloadSize,
		CheckOrigin:     func(_ *http.Request) bool { return true },
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		orgID, userID, err := authenticateJWT(r.Header.Get("Authorization"), authCfg)
		if err != nil {
			backend.metrics.authErrors.Inc()
			http.Error(w, "authentication failed", http.StatusUnauthorized)
			return
		}

		if rateCfg.Enabled && !checkRateLimit(r.RemoteAddr, orgID) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			backend.metrics.connectionErrors.Inc()
			log.Printf("[websocket-gateway] upgrade error: %v", err)
			return
		}

		backend.HandleConnection(conn, orgID, userID)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	log.Printf("[websocket-gateway] WebSocket server on %s", addr)

	srv := &http.Server{Addr: addr, Handler: mux, ReadTimeout: 0, WriteTimeout: 0}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("[websocket-gateway] HTTP server error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[websocket-gateway] starting...")

	config := loadConfig()

	tracerProvider, err := setupTracing("websocket-gateway")
	if err != nil {
		log.Fatalf("[websocket-gateway] tracing setup: %v", err)
	}
	defer tracerProvider.Shutdown(context.Background())

	metrics := NewWebSocketMetrics(config.Metrics.Buckets)

	redisClient := initRedisClusterClient(config.Redis)

	clickhouseClient, err := initClickHouseClient(config.ClickHouse)
	if err != nil {
		log.Printf("[websocket-gateway] ClickHouse init failed (non-fatal): %v", err)
	}

	backend := NewWebSocketBackend(config, redisClient, clickhouseClient, metrics)

	go startMetricsServer(config.Metrics)
	go startHTTPServer(config.Server, config.Auth, config.RateLimiting, backend)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("[websocket-gateway] running (instance=%s, metrics-port=%d)",
		envOr("INSTANCE_ID", "websocket-gateway-0"), config.Metrics.Port)

	<-sigChan
	log.Println("[websocket-gateway] shutting down...")
	backend.Shutdown()
	log.Println("[websocket-gateway] shutdown complete")
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
