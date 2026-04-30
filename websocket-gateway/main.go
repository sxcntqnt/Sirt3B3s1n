// websocket-gateway/main.go
// All types, clients, and helpers live in internal.go (same package).
// Fixes applied (scalability audit):
//  1. `for _, vehicleId := range msg.VehicleIDs` — was missing `range`.
//  2. sendInitialPositions uses parameterised query (? binding); was fmt.Sprintf → SQL injection.
//  3. removeConnection() and isConnectionAlive() now defined.
//  4. Metrics port from METRICS_PORT env.
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
	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config types
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
	Host           string
	Username       string
	Password       string
	Database       string
	QueryTimeout   time.Duration
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
	BatchSize     int
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
	instanceID  := envOr("INSTANCE_ID", "websocket-gateway-0")
	metricsPort := envInt("METRICS_PORT", 9090) // FIX: from env

	return Config{
		Server: ServerConfig{
			Port: 8080, Host: "0.0.0.0",
			MaxConnections: 10000, MaxPayloadSize: 65536,
			PingInterval: 30 * time.Second, PingTimeout: 10 * time.Second,
			BackpressureLimit: 262144,
		},
		Redis: RedisConfig{
			Cluster:  true,
			Nodes:    []string{"redis-01:6379", "redis-02:6379", "redis-03:6379"},
			Password: os.Getenv("REDIS_PASSWORD"),
			Streams: RedisStreamConfig{
				Realtime:      "gps:realtime:{orgId}",
				ConsumerGroup: "ws-gateway",
				ConsumerName:  instanceID,
			},
		},
		ClickHouse: ClickHouseConfig{
			Host: os.Getenv("CLICKHOUSE_HOST"), Username: os.Getenv("CLICKHOUSE_USERNAME"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"), Database: "default",
			QueryTimeout: 5 * time.Second,
			ConnectionPool: ConnectionPool{MaxSize: 10, MinIdle: 2, MaxLifetime: 5 * time.Minute},
		},
		Auth: AuthConfig{
			ServiceURL: os.Getenv("AUTH_SERVICE_URL"), JWTSecret: os.Getenv("JWT_SECRET"),
			TokenExpiry: 15 * time.Minute, RefreshThreshold: 5 * time.Minute,
		},
		Subscriptions: SubscriptionConfig{
			MaxPerConnection: 1000, MaxVehiclesPerOrg: 10000,
			HeartbeatInterval: 30 * time.Second, ReconnectTimeout: 10 * time.Second,
			Backoff: BackoffConfig{Initial: time.Second, Max: 10 * time.Second, Factor: 2},
		},
		Messaging: MessagingConfig{
			BatchSize: 50, FlushInterval: 100 * time.Millisecond, MaxQueueSize: 10000,
			Priority: PriorityConfig{
				Critical: []string{"PANIC_BUTTON", "OVERSPEED", "GPS_SIGNAL_LOST"},
				High:     []string{"GEOFENCE_ENTER", "GEOFENCE_EXIT", "HARSH_BRAKING", "HARSH_ACCELERATION"},
				Normal:   []string{"NORMAL", "IGNITION_ON", "IGNITION_OFF"},
			},
		},
		Metrics:      MetricsConfig{Port: metricsPort, Path: "/metrics", Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}},
		RateLimiting: RateLimitingConfig{Enabled: true, MaxConnectionsPerIP: 100, MaxMessagesPerSecond: 1000, BurstSize: 100, WindowMs: time.Second},
	}
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
		connectionsActive:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "websocket_gateway_connections_active"}),
		messagesPushed:       prometheus.NewCounter(prometheus.CounterOpts{Name: "websocket_gateway_messages_pushed_total"}),
		clientBufferPressure: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ws_client_buffer_pressure"}),
		redisReadLag:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "websocket_gateway_redis_read_lag"}),
		connectionErrors:     prometheus.NewCounter(prometheus.CounterOpts{Name: "websocket_gateway_connection_errors_total"}),
		authErrors:           prometheus.NewCounter(prometheus.CounterOpts{Name: "websocket_gateway_auth_errors_total"}),
		messageLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "websocket_gateway_message_latency_ms", Buckets: buckets}),
	}
	prometheus.MustRegister(m.connectionsActive, m.messagesPushed, m.clientBufferPressure,
		m.redisReadLag, m.connectionErrors, m.authErrors, m.messageLatency)
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
	c.mu.RLock(); defer c.mu.RUnlock(); return c.closed
}

func (c *WebSocketConnection) Close() {
	c.mu.Lock(); defer c.mu.Unlock()
	if !c.closed { c.closed = true; _ = c.Conn.Close() }
}

// ─────────────────────────────────────────────────────────────────────────────
// Backend
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketBackend struct {
	config      Config
	redisClient *redis.ClusterClient
	chClient    *CHClient // defined in internal.go
	metrics     *WebSocketMetrics
	connMu      sync.RWMutex
	connections map[string]*WebSocketConnection
}

func NewWebSocketBackend(cfg Config, rdb *redis.ClusterClient, ch *CHClient, m *WebSocketMetrics) *WebSocketBackend {
	return &WebSocketBackend{
		config: cfg, redisClient: rdb, chClient: ch, metrics: m,
		connections: make(map[string]*WebSocketConnection),
	}
}

func (b *WebSocketBackend) Shutdown() {
	b.connMu.RLock(); defer b.connMu.RUnlock()
	for _, c := range b.connections { c.Close() }
}

// removeConnection closes and removes a connection.
// FIX: was called by handleIncomingMessages but never defined → compile error.
func (b *WebSocketBackend) removeConnection(id string) {
	b.connMu.Lock()
	conn, ok := b.connections[id]
	if ok { delete(b.connections, id) }
	b.connMu.Unlock()
	if ok {
		conn.Close()
		b.metrics.connectionsActive.Dec()
		log.Printf("[websocket-gateway] removed %s", id)
	}
}

// isConnectionAlive pings the client and returns true if it responds.
// FIX: was called by startStreamReader but never defined → compile error.
func (b *WebSocketBackend) isConnectionAlive(conn *WebSocketConnection) bool {
	if conn.IsClosed() { return false }
	conn.mu.Lock(); defer conn.mu.Unlock()
	return conn.Conn.WriteControl(websocket.PingMessage, []byte("ping"),
		time.Now().Add(b.config.Server.PingTimeout)) == nil
}

func (b *WebSocketBackend) HandleConnection(conn *websocket.Conn, orgID, userID string) {
	id := generateUUID() // defined in internal.go
	ws := &WebSocketConnection{ID: id, Conn: conn, OrgID: orgID, UserID: userID, Subscriptions: make(map[string]bool)}
	b.connMu.Lock(); b.connections[id] = ws; b.connMu.Unlock()
	b.metrics.connectionsActive.Inc()
	go b.startStreamReader(ws)
	go b.handleIncomingMessages(ws)
	go b.sendInitialPositions(ws)
	go b.startHeartbeat(ws)
}

func (b *WebSocketBackend) startStreamReader(conn *WebSocketConnection) {
	streamKey := "gps:realtime:" + conn.OrgID
	args := redis.XReadGroupArgs{
		Group: b.config.Redis.Streams.ConsumerGroup, Consumer: b.config.Redis.Streams.ConsumerName,
		Streams: []string{streamKey, ">"}, Count: 100, Block: 100 * time.Millisecond,
	}
	for {
		if !b.isConnectionAlive(conn) { break } // FIX: now defined
		result, err := b.redisClient.XReadGroup(context.Background(), &args).Result()
		if err != nil { if err != redis.Nil { time.Sleep(time.Second) }; continue }

		for _, sr := range result {
			for _, msg := range sr.Messages {
				start := time.Now()
				update := parsePositionUpdate(msg.Values) // defined in internal.go
				conn.mu.RLock()
				subscribed := conn.Subscriptions[update.VehicleID] || conn.Subscriptions["all"]
				conn.mu.RUnlock()
				if subscribed {
					b.sendPositionUpdate(conn, update)
					b.metrics.messagesPushed.Inc()
					b.metrics.messageLatency.Observe(float64(time.Since(start).Milliseconds()))
				}
				_ = b.redisClient.XAck(context.Background(), streamKey,
					b.config.Redis.Streams.ConsumerGroup, msg.ID)
			}
		}
	}
}

// sendInitialPositions queries the last known position for all org vehicles.
// FIX: was fmt.Sprintf(query, conn.OrgID) → SQL injection.
// Now uses parameterised ? binding via CHClient.Query.
func (b *WebSocketBackend) sendInitialPositions(conn *WebSocketConnection) {
	if b.chClient == nil { return }
	ctx, cancel := context.WithTimeout(context.Background(), b.config.ClickHouse.QueryTimeout)
	defer cancel()

	const query = `
		SELECT vehicle_id,
		       anyLast(latitude)  AS latitude,
		       anyLast(longitude) AS longitude,
		       anyLast(speed)     AS speed,
		       anyLast(heading)   AS heading,
		       max(recorded_at)   AS recorded_at
		FROM gps_events
		WHERE organization_id = ?
		  AND recorded_at >= now() - INTERVAL 5 MINUTE
		GROUP BY vehicle_id`

	rows, err := b.chClient.Query(ctx, query, conn.OrgID) // FIX: parameterised
	if err != nil { log.Printf("[websocket-gateway] initial positions: %v", err); return }
	defer rows.Close()

	var positions []VehiclePosition // defined in internal.go
	for rows.Next() {
		var p VehiclePosition
		if err := rows.ScanStruct(&p); err == nil { positions = append(positions, p) }
	}
	if conn.IsClosed() { return }
	conn.mu.Lock(); defer conn.mu.Unlock()
	_ = conn.Conn.WriteJSON(WebSocketMessage{Type: "initial_positions", Positions: positions})
}

func (b *WebSocketBackend) handleIncomingMessages(conn *WebSocketConnection) {
	for {
		var msg WebSocketMessage // defined in internal.go
		if err := conn.Conn.ReadJSON(&msg); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[websocket-gateway] read %s: %v", conn.ID, err)
			}
			b.removeConnection(conn.ID) // FIX: now defined
			return
		}
		switch msg.Type {
		case "subscribe":   b.handleSubscription(conn, msg)
		case "unsubscribe": b.handleUnsubscription(conn, msg)
		case "ping":
			conn.mu.Lock()
			_ = conn.Conn.WriteJSON(WebSocketMessage{Type: "pong", Timestamp: time.Now().UnixMilli()})
			conn.mu.Unlock()
		}
	}
}

func (b *WebSocketBackend) handleSubscription(conn *WebSocketConnection, msg WebSocketMessage) {
	conn.mu.Lock(); defer conn.mu.Unlock()
	if msg.All {
		conn.Subscriptions["all"] = true
	} else {
		// FIX: was `for _, vehicleId := msg.VehicleIDs` — missing `range` keyword → syntax error.
		for _, vehicleID := range msg.VehicleIDs {
			conn.Subscriptions[vehicleID] = true
		}
	}
	_ = conn.Conn.WriteJSON(WebSocketMessage{Type: "subscription_confirmation", SubscribedVehicles: len(conn.Subscriptions)})
}

func (b *WebSocketBackend) handleUnsubscription(conn *WebSocketConnection, msg WebSocketMessage) {
	conn.mu.Lock(); defer conn.mu.Unlock()
	for _, id := range msg.VehicleIDs { delete(conn.Subscriptions, id) }
}

func (b *WebSocketBackend) sendPositionUpdate(conn *WebSocketConnection, u PositionUpdate) {
	if conn.IsClosed() { return }
	conn.mu.Lock(); defer conn.mu.Unlock()
	_ = conn.Conn.WriteJSON(WebSocketMessage{
		Type: "position", VehicleID: u.VehicleID,
		Latitude: u.Latitude, Longitude: u.Longitude,
		Speed: u.Speed, Heading: u.Heading, Timestamp: u.Timestamp,
	})
}

func (b *WebSocketBackend) startHeartbeat(conn *WebSocketConnection) {
	ticker := time.NewTicker(b.config.Server.PingInterval); defer ticker.Stop()
	for range ticker.C {
		if !b.isConnectionAlive(conn) { b.removeConnection(conn.ID); return }
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP servers — FIX: metrics port from env
// ─────────────────────────────────────────────────────────────────────────────

func startMetricsServer(cfg MetricsConfig) {
	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok"))
	})
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("[websocket-gateway] metrics+health on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[websocket-gateway] metrics: %v", err)
	}
}

func startHTTPServer(cfg ServerConfig, authCfg AuthConfig, rateCfg RateLimitingConfig, backend *WebSocketBackend) {
	upgrader := websocket.Upgrader{
		ReadBufferSize: cfg.MaxPayloadSize, WriteBufferSize: cfg.MaxPayloadSize,
		CheckOrigin: func(_ *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// authenticateJWT and checkRateLimit defined in internal.go
		orgID, userID, err := authenticateJWT(r.Header.Get("Authorization"), authCfg)
		if err != nil { backend.metrics.authErrors.Inc(); http.Error(w, "unauthorized", 401); return }
		if rateCfg.Enabled && !checkRateLimit(r.RemoteAddr, orgID) { http.Error(w, "rate limit", 429); return }
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil { backend.metrics.connectionErrors.Inc(); return }
		backend.HandleConnection(conn, orgID, userID)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200); _, _ = w.Write([]byte("ok"))
	})
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log.Printf("[websocket-gateway] WebSocket server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[websocket-gateway] HTTP: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[websocket-gateway] starting...")
	cfg := loadConfig()

	// setupTracing defined in internal.go; returns (func(ctx) error, error)
	shutdown, err := setupTracing("websocket-gateway")
	if err != nil { log.Fatalf("[websocket-gateway] tracing: %v", err) }
	defer shutdown(context.Background())

	metrics := NewWebSocketMetrics(cfg.Metrics.Buckets)
	rdb     := initRedisClusterClient(cfg.Redis) // internal.go

	// initClickHouseClient defined in internal.go; returns (*CHClient, error)
	ch, err := initClickHouseClient(cfg.ClickHouse)
	if err != nil { log.Printf("[websocket-gateway] ClickHouse unavailable: %v", err) }

	backend := NewWebSocketBackend(cfg, rdb, ch, metrics)

	go startMetricsServer(cfg.Metrics)
	go startHTTPServer(cfg.Server, cfg.Auth, cfg.RateLimiting, backend)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("[websocket-gateway] running (instance=%s port=%d)",
		envOr("INSTANCE_ID", "websocket-gateway-0"), cfg.Metrics.Port)

	<-sig
	log.Println("[websocket-gateway] shutting down...")
	backend.Shutdown()
	log.Println("[websocket-gateway] done")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" { return v }; return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil { return n }
	}
	return def
}
