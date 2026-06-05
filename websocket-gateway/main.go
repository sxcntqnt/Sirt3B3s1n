// websocket-gateway/main.go
//
// Config precedence: ENV > websocket-gateway.yaml > coded defaults.
// All env keys are prefixed WEBSOCKET_GATEWAY_ by Viper.
//
// P0 production fixes applied:
//   - WS_PORT env var: each replica binds a different port (no more 8080 collision).
//   - CLICKHOUSE_HOST validated at startup: fatal if empty.
//   - INSTANCE_ID used as Redis consumer name: correct multi-replica fanout.
//   - Strict CheckOrigin via configured AllowedOrigins list.
//   - Per-connection write goroutine: eliminates concurrent WriteJSON race.
//   - StreamDispatcher: one Redis reader per org (not per connection).
//   - VehicleIndex: O(1) fanout (not O(connections) scan).
//   - Write deadlines: slow clients dropped after maxConsecutiveDrops.
//   - Subscription limit enforced in handleSubscription.
//   - Pong handler + read deadline: stale connections detected and removed.
//   - startHeartbeat: goroutine exits on conn.closed (no leak).
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	redis "github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config — Viper-backed, atomic ConfigHolder, 12-factor layering
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	Server        ServerConfig
	Redis         RedisConfig
	ClickHouse    ClickHouseConfig
	Auth          AuthConfig
	Subscriptions SubscriptionConfig
	Metrics       MetricsConfig
	RateLimiting  RateLimitingConfig
}

type ServerConfig struct {
	Port           int
	Host           string
	MaxConnections int
	MaxPayloadSize int
	PingInterval   time.Duration
	PingTimeout    time.Duration
	WriteTimeout   time.Duration
	WriteBufSize   int      // per-connection writeChan capacity
	AllowedOrigins []string // strict origin allowlist; "*" = allow all (dev only)
}

type RedisConfig struct {
	Cluster  bool
	Nodes    []string
	Password string
	Streams  RedisStreamConfig
}

type RedisStreamConfig struct {
	ConsumerGroup string
	ConsumerName  string // set to INSTANCE_ID at runtime
}
type ClickHouseConfig struct {
        Host           string
        Port           int
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
	JWTSecret string
}

type SubscriptionConfig struct {
	MaxPerConnection int
}

type MetricsConfig struct {
	Port    int
	Path    string
	Buckets []float64
}

type RateLimitingConfig struct {
	Enabled             bool
	MaxConnectionsPerIP int64 // connection-level (handled by token bucket)
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
	v.SetEnvPrefix("WEBSOCKET_GATEWAY")

	v.SetConfigName("websocket-gateway")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/matatu-pulse")
	_ = v.ReadInConfig()

	instanceID := envOr("INSTANCE_ID", "websocket-gateway-0")

	// ── defaults ─────────────────────────────────────────────────────────────
	// P0 FIX: port from WS_PORT env so each replica binds a different port.
	// Orchestrator sets WS_PORT=8080+replica, e.g. 8080, 8081, 8082.
	serverPort := 9970
	if p := os.Getenv("WS_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			serverPort = n
		}
	}

	metricsPort := 9090
	if p := os.Getenv("METRICS_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			metricsPort = n
		}
	}

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.max_connections", 10000)
	v.SetDefault("server.max_payload_size", 65536)
	v.SetDefault("server.ping_interval", "30s")
	v.SetDefault("server.ping_timeout", "10s")
	v.SetDefault("server.write_timeout", "5s")
	v.SetDefault("server.write_buf_size", 256)
	v.SetDefault("server.allowed_origins", []string{}) // empty = deny all (safe default)

	v.SetDefault("redis.cluster", true)
	v.SetDefault("redis.nodes", []string{"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"})
	v.SetDefault("redis.streams.consumer_group", "ws-gateway")

        v.SetDefault("clickhouse.port", 9000)
	v.SetDefault("clickhouse.database", "default")
	v.SetDefault("clickhouse.query_timeout", "5s")
	v.SetDefault("clickhouse.connection_pool.max_size", 10)
	v.SetDefault("clickhouse.connection_pool.min_idle", 2)
	v.SetDefault("clickhouse.connection_pool.max_lifetime", "5m")

	v.SetDefault("subscriptions.max_per_connection", 1000)

	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("metrics.buckets",
		[]float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5})

	v.SetDefault("rate_limiting.enabled", true)
	v.SetDefault("rate_limiting.max_connections_per_ip", int64(100))

	cfg := &Config{
		Server: ServerConfig{
			Port:           serverPort, // P0 FIX
			Host:           v.GetString("server.host"),
			MaxConnections: v.GetInt("server.max_connections"),
			MaxPayloadSize: v.GetInt("server.max_payload_size"),
			PingInterval:   v.GetDuration("server.ping_interval"),
			PingTimeout:    v.GetDuration("server.ping_timeout"),
			WriteTimeout:   v.GetDuration("server.write_timeout"),
			WriteBufSize:   v.GetInt("server.write_buf_size"),
			AllowedOrigins: v.GetStringSlice("server.allowed_origins"),
		},
		Redis: RedisConfig{
			Cluster:  v.GetBool("redis.cluster"),
			Nodes:    v.GetStringSlice("redis.nodes"),
			Password: v.GetString("redis.password"),
			Streams: RedisStreamConfig{
				ConsumerGroup: v.GetString("redis.streams.consumer_group"),
				ConsumerName:  instanceID, // P0 FIX: unique per instance
			},
		},
		ClickHouse: ClickHouseConfig{
			Host:         v.GetString("clickhouse.host"),
		        Port:         v.GetInt("clickhouse.port"),
			Username:     v.GetString("clickhouse.username"),
			Password:     v.GetString("clickhouse.password"),
			Database:     v.GetString("clickhouse.database"),
			QueryTimeout: v.GetDuration("clickhouse.query_timeout"),
			ConnectionPool: ConnectionPool{
				MaxSize:     v.GetInt("clickhouse.connection_pool.max_size"),
				MinIdle:     v.GetInt("clickhouse.connection_pool.min_idle"),
				MaxLifetime: v.GetDuration("clickhouse.connection_pool.max_lifetime"),
			},
		},
		Auth: AuthConfig{
			JWTSecret: v.GetString("auth.jwt_secret"),
		},
		Subscriptions: SubscriptionConfig{
			MaxPerConnection: v.GetInt("subscriptions.max_per_connection"),
		},
		Metrics: MetricsConfig{
			Port:    metricsPort,
			Path:    v.GetString("metrics.path"),
			Buckets: getFloat64Slice(v, "metrics.buckets"),
		},
		RateLimiting: RateLimitingConfig{
			Enabled:             v.GetBool("rate_limiting.enabled"),
			MaxConnectionsPerIP: v.GetInt64("rate_limiting.max_connections_per_ip"),
		},
	}

	// ── validation ───────────────────────────────────────────────────────────
	// P0 FIX: validate ClickHouse host at startup — fail fast, not at first query.
	if cfg.ClickHouse.Host == "" {
		return nil, fmt.Errorf(
			"WEBSOCKET_GATEWAY_CLICKHOUSE_HOST (or clickhouse.host) is required; " +
				"set it to the ClickHouse host (e.g. 127.0.0.1, NOT 127.0.0.1:9000)")
	}
	if cfg.Auth.JWTSecret == "" {
		return nil, fmt.Errorf("WEBSOCKET_GATEWAY_AUTH_JWT_SECRET is required")
	}
	if len(cfg.Redis.Nodes) == 0 {
		return nil, fmt.Errorf("redis.nodes must not be empty")
	}

	return cfg, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketMetrics struct {
	connectionsActive    prometheus.Gauge
	messagesPushed       prometheus.Counter
	messagesDropped      prometheus.Counter   // slow-client drops
	clientBufferPressure prometheus.Gauge     // fraction of full write channels
	redisReadLag         prometheus.Gauge
	connectionErrors     prometheus.Counter
	authErrors           prometheus.Counter
	messageLatency       prometheus.Histogram
	subscriptionsActive  *prometheus.GaugeVec // per org
}

func NewWebSocketMetrics(buckets []float64) *WebSocketMetrics {
	m := &WebSocketMetrics{
		connectionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "websocket_gateway_connections_active",
			Help: "Number of active WebSocket connections",
		}),
		messagesPushed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_messages_pushed_total",
			Help: "Total messages delivered to WebSocket clients",
		}),
		messagesDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_messages_dropped_total",
			Help: "Messages dropped due to slow clients",
		}),
		clientBufferPressure: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "websocket_gateway_client_buffer_pressure",
			Help: "Fraction of connections with full write buffers [0,1]",
		}),
		redisReadLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "websocket_gateway_redis_read_lag",
			Help: "Approximate Redis stream lag observed by the dispatcher",
		}),
		connectionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_connection_errors_total",
			Help: "WebSocket upgrade errors",
		}),
		authErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "websocket_gateway_auth_errors_total",
			Help: "JWT authentication failures",
		}),
		messageLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "websocket_gateway_message_latency_ms",
			Help:    "Latency from stream read to client dispatch in milliseconds",
			Buckets: buckets,
		}),
		subscriptionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "websocket_gateway_subscriptions_active",
			Help: "Active vehicle subscriptions per org",
		}, []string{"org_id"}),
	}
	prometheus.MustRegister(
		m.connectionsActive, m.messagesPushed, m.messagesDropped,
		m.clientBufferPressure, m.redisReadLag,
		m.connectionErrors, m.authErrors, m.messageLatency,
		m.subscriptionsActive,
	)
	return m
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocketBackend
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketBackend struct {
	cfgHolder    *ConfigHolder
	redisClient  *redis.ClusterClient
	chClient     *CHClient
	metrics      *WebSocketMetrics
	vehicleIndex *VehicleIndex
	dispatcher   *StreamDispatcher

	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc

	connMu      sync.RWMutex
	connections map[string]*WebSocketConnection
}

func NewWebSocketBackend(
	cfgHolder *ConfigHolder,
	rdb *redis.ClusterClient,
	ch *CHClient,
	metrics *WebSocketMetrics,
) *WebSocketBackend {
	cfg := cfgHolder.Get()
	ctx, cancel := context.WithCancel(context.Background())
	index := NewVehicleIndex()
	dispatcher := NewStreamDispatcher(
		rdb,
		cfg.Redis.Streams.ConsumerGroup,
		cfg.Redis.Streams.ConsumerName,
		index,
		metrics,
	)
	return &WebSocketBackend{
		cfgHolder:      cfgHolder,
		redisClient:    rdb,
		chClient:       ch,
		metrics:        metrics,
		vehicleIndex:   index,
		dispatcher:     dispatcher,
		dispatchCtx:    ctx,
		dispatchCancel: cancel,
		connections:    make(map[string]*WebSocketConnection),
	}
}

func (b *WebSocketBackend) Shutdown() {
	b.dispatchCancel() // stops all StreamDispatcher org goroutines

	b.connMu.RLock()
	for _, c := range b.connections {
		c.Close()
	}
	b.connMu.RUnlock()
}

// HandleConnection is called once per accepted WebSocket upgrade.
//
// It sets up:
//   - The write goroutine (writeLoop) — sole owner of conn.Conn.Write*
//   - Pong handler + read deadline for liveness detection
//   - StreamDispatcher registration so the org stream is read
//   - Initial position snapshot (async, via conn.Send)
//   - Heartbeat goroutine
//   - Message read loop (blocking, current goroutine)
func (b *WebSocketBackend) HandleConnection(conn *websocket.Conn, orgID, userID string) {
	cfg := b.cfgHolder.Get()

	ws := newWebSocketConnection(conn, orgID, userID, cfg.Server.WriteBufSize)

	// Register connection.
	b.connMu.Lock()
	b.connections[ws.ID] = ws
	b.connMu.Unlock()
	b.metrics.connectionsActive.Inc()

	// Configure liveness: gorilla requires pong handler and read deadline
	// to be set before the first read. Read deadline is reset on every pong.
	pongWait := cfg.Server.PingInterval + cfg.Server.PingTimeout
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(_ string) error {
		ws.lastPongAt.Store(time.Now().UnixNano())
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	// Start the single write goroutine.
	go ws.writeLoop(cfg.Server.WriteTimeout)

	// Ensure the org's Redis stream reader is running.
	b.dispatcher.EnsureOrg(b.dispatchCtx, orgID)

	// Async: send last-known positions from ClickHouse.
	go b.sendInitialPositions(ws)

	// Async: ping at interval, rely on read deadline for pong enforcement.
	go b.startHeartbeat(ws)

	// Blocking: read loop.
	b.handleIncomingMessages(ws)
}

// removeConnection closes the connection and cleans up all index state.
func (b *WebSocketBackend) removeConnection(id string) {
	b.connMu.Lock()
	conn, ok := b.connections[id]
	if ok {
		delete(b.connections, id)
	}
	b.connMu.Unlock()

	if !ok {
		return
	}

	b.vehicleIndex.RemoveAll(conn)
	conn.Close()
	b.metrics.connectionsActive.Dec()
	b.metrics.subscriptionsActive.
		WithLabelValues(conn.OrgID).
		Sub(float64(conn.subscriptionCount.Load()))

	log.Printf("[websocket-gateway] disconnected %s org=%s subs=%d",
		conn.ID, conn.OrgID, conn.subscriptionCount.Load())
}

// handleIncomingMessages is the sole reader goroutine for a connection.
// It blocks until the connection closes, then calls removeConnection.
func (b *WebSocketBackend) handleIncomingMessages(conn *WebSocketConnection) {
	defer b.removeConnection(conn.ID)

	for {
		var msg WebSocketMessage
		if err := conn.Conn.ReadJSON(&msg); err != nil {
			if !websocket.IsCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				log.Printf("[websocket-gateway] read %s: %v", conn.ID, err)
			}
			return
		}

		switch msg.Type {
		case "subscribe":
			b.handleSubscription(conn, msg)
		case "unsubscribe":
			b.handleUnsubscription(conn, msg)
		case "ping":
			// Application-level ping (distinct from WebSocket control frame ping).
			conn.Send(WebSocketMessage{
				Type:      "pong",
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}
}

// handleSubscription updates the VehicleIndex and enforces subscription limits.
func (b *WebSocketBackend) handleSubscription(conn *WebSocketConnection, msg WebSocketMessage) {
	cfg := b.cfgHolder.Get()

	if msg.All {
		b.vehicleIndex.SubscribeAll(conn)
		conn.Send(WebSocketMessage{
			Type:               "subscription_confirmation",
			SubscribedVehicles: int(conn.subscriptionCount.Load()),
		})
		return
	}

	// Enforce per-connection subscription limit.
	current := conn.subscriptionCount.Load()
	headroom := int64(cfg.Subscriptions.MaxPerConnection) - current
	if headroom <= 0 {
		conn.Send(WebSocketMessage{
			Type:  "error",
			Error: fmt.Sprintf("subscription limit reached (%d)", cfg.Subscriptions.MaxPerConnection),
		})
		return
	}

	// Trim to available headroom.
	toAdd := msg.VehicleIDs
	if int64(len(toAdd)) > headroom {
		toAdd = toAdd[:headroom]
	}

	newCount := b.vehicleIndex.Subscribe(conn, toAdd)
	b.metrics.subscriptionsActive.WithLabelValues(conn.OrgID).Add(float64(len(toAdd)))

	conn.Send(WebSocketMessage{
		Type:               "subscription_confirmation",
		SubscribedVehicles: int(newCount),
	})
}

func (b *WebSocketBackend) handleUnsubscription(conn *WebSocketConnection, msg WebSocketMessage) {
	b.vehicleIndex.Unsubscribe(conn, msg.VehicleIDs)
	b.metrics.subscriptionsActive.
		WithLabelValues(conn.OrgID).
		Sub(float64(len(msg.VehicleIDs)))
}

// sendInitialPositions fetches the last-known position for all org vehicles
// from ClickHouse and sends it via the connection's write channel.
//
// FIX: old version held conn.mu.Lock() and called WriteJSON directly — racing
// with writeLoop. Now uses conn.Send() which is the only safe write path.
func (b *WebSocketBackend) sendInitialPositions(conn *WebSocketConnection) {
	if b.chClient == nil || conn.IsClosed() {
		return
	}

	cfg := b.cfgHolder.Get()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ClickHouse.QueryTimeout)
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

	rows, err := b.chClient.Query(ctx, query, conn.OrgID)
	if err != nil {
		log.Printf("[websocket-gateway] initial positions org=%s: %v", conn.OrgID, err)
		return
	}
	defer rows.Close()

	var positions []VehiclePosition
	for rows.Next() {
		var p VehiclePosition
		if err := rows.ScanStruct(&p); err == nil {
			positions = append(positions, p)
		}
	}

	conn.Send(WebSocketMessage{
		Type:      "initial_positions",
		Positions: positions,
	})
}

// startHeartbeat sends a WebSocket ping at PingInterval.
//
// FIX: old version looped on ticker.C with no exit condition — goroutine leak
// after the connection closed. New version selects on conn.closed.
//
// FIX: old version called WriteControl directly from this goroutine — racing
// with writeLoop. New version uses conn.SendPing() which enqueues via pingChan.
//
// Liveness enforcement is handled by the pong handler + read deadline:
// if no pong arrives within PingInterval+PingTimeout, ReadJSON returns a
// deadline error in handleIncomingMessages, which calls removeConnection.
func (b *WebSocketBackend) startHeartbeat(conn *WebSocketConnection) {
	cfg := b.cfgHolder.Get()
	ticker := time.NewTicker(cfg.Server.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-conn.closed: // FIX: exits cleanly on disconnect
			return
		case <-ticker.C:
			if !conn.SendPing() {
				return
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP servers
// ─────────────────────────────────────────────────────────────────────────────

func startMetricsServer(cfg MetricsConfig) {
	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("[websocket-gateway] metrics+health on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[websocket-gateway] metrics server: %v", err)
	}
}

func startHTTPServer(
	cfg ServerConfig,
	authCfg AuthConfig,
	rateCfg RateLimitingConfig,
	backend *WebSocketBackend,
	rdb *redis.ClusterClient,
) {
	// P0 FIX: strict CheckOrigin.
	// Build an O(1) lookup map from the allowed origins list.
	// Empty list = deny all (safe default — requires explicit configuration).
	// Single entry "*" = allow all (development only, never production).
	allowedOrigins := make(map[string]bool, len(cfg.AllowedOrigins))
	for _, o := range cfg.AllowedOrigins {
		allowedOrigins[o] = true
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  cfg.MaxPayloadSize,
		WriteBufferSize: cfg.MaxPayloadSize,
		CheckOrigin: func(r *http.Request) bool {
			if allowedOrigins["*"] {
				return true // dev mode only
			}
			origin := r.Header.Get("Origin")
			if origin == "" {
				return false // non-browser client without origin — deny
			}
			return allowedOrigins[origin]
		},
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// Rate limit before upgrading (cheap check on raw TCP connection).
		if rateCfg.Enabled {
			if !checkRateLimit(r.RemoteAddr, rateCfg.MaxConnectionsPerIP) {
				httpError(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
		}

		// JWT authentication.
		orgID, userID, jti, err := authenticateJWT(r.Header.Get("Authorization"), authCfg)
		if err != nil {
			backend.metrics.authErrors.Inc()
			httpError(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		// Token revocation check (Redis blocklist).
		ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
		revoked := isTokenRevoked(ctx, jti, rdb)
		cancel()
		if revoked {
			backend.metrics.authErrors.Inc()
			httpError(w, "token revoked", http.StatusUnauthorized)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			backend.metrics.connectionErrors.Inc()
			return
		}

		log.Printf("[websocket-gateway] connected %s org=%s", userID, orgID)
		backend.HandleConnection(conn, orgID, userID)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// P0 FIX: port is now per-replica (WS_PORT env), not hardcoded 8080.
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log.Printf("[websocket-gateway] WebSocket server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[websocket-gateway] HTTP server: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("[websocket-gateway] starting...")

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[websocket-gateway] config error: %v", err)
	}
	cfgHolder := NewConfigHolder(cfg)

	shutdown, err := setupTracing("websocket-gateway")
	if err != nil {
		log.Fatalf("[websocket-gateway] tracing: %v", err)
	}
	defer shutdown(context.Background())

	metrics := NewWebSocketMetrics(cfg.Metrics.Buckets)
	rdb := initRedisClusterClient(cfg.Redis)

	ch, err := initClickHouseClient(cfg.ClickHouse)
	if err != nil {
		// P0 FIX: ClickHouse unavailability is now a fatal startup error so
		// the orchestrator knows to restart rather than silently serving
		// connections with no initial position data.
		log.Fatalf("[websocket-gateway] ClickHouse init failed: %v", err)
	}

	backend := NewWebSocketBackend(cfgHolder, rdb, ch, metrics)

	go startMetricsServer(cfg.Metrics)
	go startHTTPServer(cfg.Server, cfg.Auth, cfg.RateLimiting, backend, rdb)

	log.Printf(
		"[websocket-gateway] running (instance=%s ws-port=%d metrics-port=%d origins=%v)",
		cfg.Redis.Streams.ConsumerName,
		cfg.Server.Port,
		cfg.Metrics.Port,
		cfg.Server.AllowedOrigins,
	)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("[websocket-gateway] shutting down...")
	backend.Shutdown()
	log.Println("[websocket-gateway] done")
}
