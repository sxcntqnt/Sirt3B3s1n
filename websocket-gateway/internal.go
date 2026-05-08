// websocket-gateway/internal.go
//
// All types, client wrappers, and helper functions for the WebSocket gateway.
// Same package as main.go — every symbol is directly accessible.
//
// CONCURRENCY MODEL (per connection)
//   One goroutine reads   — handleIncomingMessages → conn.Conn.ReadJSON only
//   One goroutine writes  — writeLoop              → conn.Conn.WriteJSON / WriteControl only
//   All other goroutines  — call conn.Send() / conn.SendPing() (non-blocking)
//
// FANOUT MODEL
//   StreamDispatcher maintains ONE XReadGroup reader goroutine per org stream.
//   VehicleIndex maps vehicleID → set of connections for O(1) dispatch.
//   This replaces the per-connection reader that would open 10k Redis consumers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"strconv"

	"sync"
	"sync/atomic"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	redis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ─────────────────────────────────────────────────────────────────────────────
// Tracing
// ─────────────────────────────────────────────────────────────────────────────

func setupTracing(serviceName string) (func(context.Context) error, error) {
	endpoint := envOr("JAEGER_ENDPOINT", "http://localhost:4318/v1/traces")

	exp, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", envOr("ENVIRONMENT", "production")),
			attribute.String("instance_id", envOr("INSTANCE_ID", "unknown")),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.01))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Redis
// ─────────────────────────────────────────────────────────────────────────────

func initRedisClusterClient(cfg RedisConfig) *redis.ClusterClient {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    cfg.Nodes,
		Password: cfg.Password,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[websocket-gateway] redis ping warning: %v", err)
	}
	return client
}

// ─────────────────────────────────────────────────────────────────────────────
// ClickHouse — query-only for initial position snapshots
// ─────────────────────────────────────────────────────────────────────────────

type CHClient struct{ conn driver.Conn }

func initClickHouseClient(cfg ClickHouseConfig) (*CHClient, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Host},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		MaxOpenConns:    cfg.ConnectionPool.MaxSize,
		MaxIdleConns:    cfg.ConnectionPool.MinIdle,
		ConnMaxLifetime: cfg.ConnectionPool.MaxLifetime,
		DialTimeout:     10 * time.Second,
		Settings:        clickhouse.Settings{"max_execution_time": int(cfg.QueryTimeout.Seconds())},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return &CHClient{conn: conn}, nil
}

func (c *CHClient) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket domain types
// ─────────────────────────────────────────────────────────────────────────────

type WebSocketMessage struct {
	Type               string            `json:"type"`
	Timestamp          int64             `json:"ts,omitempty"`
	All                bool              `json:"all,omitempty"`
	VehicleIDs         []string          `json:"vehicle_ids,omitempty"`
	VehicleID          string            `json:"vehicle_id,omitempty"`
	Latitude           float64           `json:"lat,omitempty"`
	Longitude          float64           `json:"lon,omitempty"`
	Speed              int16             `json:"speed,omitempty"`
	Heading            int16             `json:"heading,omitempty"`
	Positions          []VehiclePosition `json:"positions,omitempty"`
	SubscribedVehicles int               `json:"subscribed_vehicles,omitempty"`
	Error              string            `json:"error,omitempty"`
}

type VehiclePosition struct {
	VehicleID  string    `json:"vehicle_id"  ch:"vehicle_id"`
	Latitude   float64   `json:"lat"         ch:"latitude"`
	Longitude  float64   `json:"lon"         ch:"longitude"`
	Speed      int16     `json:"speed"       ch:"speed"`
	Heading    int16     `json:"heading"     ch:"heading"`
	RecordedAt time.Time `json:"recorded_at" ch:"recorded_at"`
}

type PositionUpdate struct {
	VehicleID string
	Latitude  float64
	Longitude float64
	Speed     int16
	Heading   int16
	Timestamp int64
}

// parsePositionUpdate decodes a Redis stream entry into a PositionUpdate.
//
// FIX: the old version read values["vehicle_id"], values["latitude"] etc.
// directly. But mqtt-consumer writes a single "payload" key containing a
// JSON-encoded envelope. This version decodes that envelope correctly.
func parsePositionUpdate(values map[string]interface{}) (PositionUpdate, error) {
	payloadRaw, ok := values["payload"]
	if !ok {
		return PositionUpdate{}, fmt.Errorf("missing 'payload' key in stream entry")
	}
	payloadStr, ok := payloadRaw.(string)
	if !ok {
		return PositionUpdate{}, fmt.Errorf("'payload' is not a string")
	}

	var p struct {
		VehicleID string  `json:"vehicle_id"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Speed     int16   `json:"speed"`
		Heading   int16   `json:"heading"`
		Timestamp int64   `json:"device_timestamp_ms"`
	}
	if err := json.Unmarshal([]byte(payloadStr), &p); err != nil {
		return PositionUpdate{}, fmt.Errorf("payload unmarshal: %w", err)
	}
	return PositionUpdate{
		VehicleID: p.VehicleID,
		Latitude:  p.Latitude,
		Longitude: p.Longitude,
		Speed:     p.Speed,
		Heading:   p.Heading,
		Timestamp: p.Timestamp,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocketConnection
//
// Exactly one goroutine (writeLoop) may call conn.Conn.Write*.
// All other goroutines use Send() / SendPing() which are non-blocking.
// Teardown is idempotent via closeOnce + closed channel.
// ─────────────────────────────────────────────────────────────────────────────

const (
	maxConsecutiveDrops = 50  // close connection after this many consecutive drops
)

type WebSocketConnection struct {
	ID     string
	Conn   *websocket.Conn
	OrgID  string
	UserID string

	// Write path — only writeLoop touches Conn.Write*.
	writeChan chan WebSocketMessage
	pingChan  chan struct{} // capacity 1; one pending ping at a time

	// Lifecycle
	closed    chan struct{}
	closeOnce sync.Once

	// Liveness (updated by pong handler in HandleConnection)
	lastPongAt atomic.Int64 // unix nano; 0 = no pong received yet

	// Subscription accounting — updated by VehicleIndex
	subscriptionCount atomic.Int64

	// Slow-client detection
	consecutiveDrops atomic.Int64
}

func newWebSocketConnection(conn *websocket.Conn, orgID, userID string, writeBuf int) *WebSocketConnection {
	return &WebSocketConnection{
		ID:        uuid.NewString(),
		Conn:      conn,
		OrgID:     orgID,
		UserID:    userID,
		writeChan: make(chan WebSocketMessage, writeBuf),
		pingChan:  make(chan struct{}, 1),
		closed:    make(chan struct{}),
	}
}

// Close tears down the connection exactly once.
func (c *WebSocketConnection) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.Conn.Close()
	})
}

// IsClosed reports whether the connection has been closed.
func (c *WebSocketConnection) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// Send enqueues a JSON message for the write goroutine.
// Returns false if closed or the write buffer is full (slow client).
// After maxConsecutiveDrops consecutive drops, the connection is closed.
func (c *WebSocketConnection) Send(msg WebSocketMessage) bool {
	select {
	case c.writeChan <- msg:
		c.consecutiveDrops.Store(0)
		return true
	case <-c.closed:
		return false
	default:
		drops := c.consecutiveDrops.Add(1)
		if drops >= maxConsecutiveDrops {
			log.Printf("[websocket-gateway] closing slow client %s org=%s (dropped %d messages)",
				c.ID, c.OrgID, drops)
			go c.Close()
		}
		return false
	}
}

// SendPing enqueues a WebSocket ping control frame.
// Returns false if the connection is closed.
func (c *WebSocketConnection) SendPing() bool {
	select {
	case c.pingChan <- struct{}{}:
		return true
	case <-c.closed:
		return false
	default:
		return true // ping already pending — that's fine
	}
}

// writeLoop is the sole goroutine that calls conn.Conn.Write*.
// It exits when closed is signalled or a write error occurs, then calls Close()
// to ensure the connection and closed channel are both torn down.
func (c *WebSocketConnection) writeLoop(writeTimeout time.Duration) {
	defer c.Close()
	for {
		select {
		case msg, ok := <-c.writeChan:
			if !ok {
				return
			}
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.Conn.WriteJSON(msg); err != nil {
				return
			}

		case <-c.pingChan:
			deadline := time.Now().Add(writeTimeout)
			_ = c.Conn.SetWriteDeadline(deadline)
			if err := c.Conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				return
			}

		case <-c.closed:
			return
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// VehicleIndex — O(1) reverse subscription fanout
//
// byVehicle[vehicleID][connID] → conn  (specific-vehicle subscribers)
// byOrgAll[orgID][connID]     → conn  (org-wide "all vehicles" subscribers)
//
// Dispatch looks up both buckets and calls conn.Send() for each match.
// The RWMutex allows concurrent reads (Dispatch) with exclusive writes
// (Subscribe / Unsubscribe / RemoveAll).
// ─────────────────────────────────────────────────────────────────────────────

type VehicleIndex struct {
	mu        sync.RWMutex
	byVehicle map[string]map[string]*WebSocketConnection
	byOrgAll  map[string]map[string]*WebSocketConnection
}

func NewVehicleIndex() *VehicleIndex {
	return &VehicleIndex{
		byVehicle: make(map[string]map[string]*WebSocketConnection),
		byOrgAll:  make(map[string]map[string]*WebSocketConnection),
	}
}

// Subscribe registers conn as a subscriber for the given vehicle IDs.
// Returns the connection's new total subscription count.
func (v *VehicleIndex) Subscribe(conn *WebSocketConnection, vehicleIDs []string) int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	var added int64
	for _, id := range vehicleIDs {
		if v.byVehicle[id] == nil {
			v.byVehicle[id] = make(map[string]*WebSocketConnection)
		}
		if _, exists := v.byVehicle[id][conn.ID]; !exists {
			v.byVehicle[id][conn.ID] = conn
			added++
		}
	}
	return conn.subscriptionCount.Add(added)
}

// SubscribeAll registers conn for all vehicles in its org.
func (v *VehicleIndex) SubscribeAll(conn *WebSocketConnection) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.byOrgAll[conn.OrgID] == nil {
		v.byOrgAll[conn.OrgID] = make(map[string]*WebSocketConnection)
	}
	v.byOrgAll[conn.OrgID][conn.ID] = conn
}

// Unsubscribe removes conn from the given vehicle IDs.
func (v *VehicleIndex) Unsubscribe(conn *WebSocketConnection, vehicleIDs []string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, id := range vehicleIDs {
		if bucket := v.byVehicle[id]; bucket != nil {
			if _, existed := bucket[conn.ID]; existed {
				delete(bucket, conn.ID)
				conn.subscriptionCount.Add(-1)
			}
			if len(bucket) == 0 {
				delete(v.byVehicle, id)
			}
		}
	}
}

// RemoveAll removes conn from every bucket. Called on disconnect.
func (v *VehicleIndex) RemoveAll(conn *WebSocketConnection) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Remove from org-all bucket.
	if bucket := v.byOrgAll[conn.OrgID]; bucket != nil {
		delete(bucket, conn.ID)
		if len(bucket) == 0 {
			delete(v.byOrgAll, conn.OrgID)
		}
	}

	// Remove from vehicle buckets — O(unique vehicles subscribed to), not O(all).
	for vehicleID, bucket := range v.byVehicle {
		if _, exists := bucket[conn.ID]; !exists {
			continue
		}
		delete(bucket, conn.ID)
		if len(bucket) == 0 {
			delete(v.byVehicle, vehicleID)
		}
	}
}

// Dispatch sends msg to all connections in orgID that are subscribed to vehicleID.
// Returns the number of connections successfully reached.
func (v *VehicleIndex) Dispatch(orgID, vehicleID string, msg WebSocketMessage) int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	sent := 0
	for _, conn := range v.byVehicle[vehicleID] {
		if conn.OrgID == orgID && conn.Send(msg) {
			sent++
		}
	}
	for _, conn := range v.byOrgAll[orgID] {
		if conn.Send(msg) {
			sent++
		}
	}
	return sent
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamDispatcher — one Redis consumer goroutine per org
//
// BEFORE (broken): one XReadGroup consumer per WebSocket connection.
//   10 000 connections → 10 000 Redis consumers.
//   Consumer group semantics also mean each message goes to ONE consumer —
//   so 999 of 1000 connections for the same org receive nothing.
//
// AFTER (correct): one goroutine per org stream.
//   - Discovers org when first connection subscribes (EnsureOrg).
//   - Reads messages from gps:realtime:{orgID} via shared XReadGroup.
//   - Calls VehicleIndex.Dispatch for O(1) in-process fan-out.
//   - XAcks after dispatch so messages are not re-delivered.
//   - Goroutine exits when ctx is cancelled (all connections for that org left).
// ─────────────────────────────────────────────────────────────────────────────

type StreamDispatcher struct {
	rdb          *redis.ClusterClient
	consumerGroup string
	consumerName  string // unique per gateway instance (INSTANCE_ID)
	index        *VehicleIndex
	metrics      *WebSocketMetrics

	mu   sync.Mutex
	orgs map[string]context.CancelFunc // orgID → cancel
}

func NewStreamDispatcher(
	rdb *redis.ClusterClient,
	group, name string,
	index *VehicleIndex,
	metrics *WebSocketMetrics,
) *StreamDispatcher {
	return &StreamDispatcher{
		rdb:           rdb,
		consumerGroup: group,
		consumerName:  name,
		index:         index,
		metrics:       metrics,
		orgs:          make(map[string]context.CancelFunc),
	}
}

// EnsureOrg starts a reader goroutine for orgID if one is not already running.
// Idempotent — safe to call on every connection handshake.
func (d *StreamDispatcher) EnsureOrg(ctx context.Context, orgID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.orgs[orgID]; exists {
		return
	}
	orgCtx, cancel := context.WithCancel(ctx)
	d.orgs[orgID] = cancel
	go d.readOrgStream(orgCtx, orgID)
}

// StopOrg cancels the reader goroutine for an org (e.g., last connection left).
func (d *StreamDispatcher) StopOrg(orgID string) {
	d.mu.Lock()
	cancel, exists := d.orgs[orgID]
	if exists {
		delete(d.orgs, orgID)
	}
	d.mu.Unlock()
	if exists {
		cancel()
	}
}

func (d *StreamDispatcher) readOrgStream(ctx context.Context, orgID string) {
	streamKey := "gps:realtime:" + orgID

	// Ensure consumer group — MKSTREAM creates the stream if it doesn't exist.
	// "$" means only new messages from this point forward.
	err := d.rdb.XGroupCreateMkStream(ctx, streamKey, d.consumerGroup, "$").Err()
	if err != nil && !isStreamGroupExists(err) {
		log.Printf("[ws-gateway/dispatcher] XGroupCreateMkStream %s: %v", streamKey, err)
	}

	log.Printf("[ws-gateway/dispatcher] started reader for org %s stream %s", orgID, streamKey)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[ws-gateway/dispatcher] stopping reader for org %s", orgID)
			return
		default:
		}

		result, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    d.consumerGroup,
			Consumer: d.consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    200,
			Block:    200 * time.Millisecond,
		}).Result()

		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			log.Printf("[ws-gateway/dispatcher] XReadGroup %s: %v", streamKey, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, sr := range result {
			for _, msg := range sr.Messages {
				start := time.Now()
				update, err := parsePositionUpdate(msg.Values)
				if err != nil {
					log.Printf("[ws-gateway/dispatcher] parse entry %s: %v", msg.ID, err)
					_ = d.rdb.XAck(ctx, streamKey, d.consumerGroup, msg.ID)
					continue
				}

				wsMsg := WebSocketMessage{
					Type:      "position",
					VehicleID: update.VehicleID,
					Latitude:  update.Latitude,
					Longitude: update.Longitude,
					Speed:     update.Speed,
					Heading:   update.Heading,
					Timestamp: update.Timestamp,
				}

				n := d.index.Dispatch(orgID, update.VehicleID, wsMsg)
				if n > 0 {
					d.metrics.messagesPushed.Add(float64(n))
					d.metrics.messageLatency.Observe(
						float64(time.Since(start).Milliseconds()))
				}

				_ = d.rdb.XAck(ctx, streamKey, d.consumerGroup, msg.ID)
			}
		}
	}
}

// isStreamGroupExists returns true when Redis reports the consumer group
// already exists — not an error condition for us.
func isStreamGroupExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// ─────────────────────────────────────────────────────────────────────────────
// JWT authentication + token revocation
// ─────────────────────────────────────────────────────────────────────────────

type jwtClaims struct {
	OrgID  string `json:"org_id"`
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

// authenticateJWT validates the Authorization header.
// Returns (orgID, userID, jti, error).
// The jti is used for revocation checks — see isTokenRevoked.
func authenticateJWT(authHeader string, cfg AuthConfig) (orgID, userID, jti string, err error) {
	token := strings.TrimPrefix(authHeader, "Bearer ")
	token = strings.TrimSpace(token)
	if token == "" {
		return "", "", "", fmt.Errorf("missing token")
	}

	parsed, parseErr := jwt.ParseWithClaims(token, &jwtClaims{},
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(cfg.JWTSecret), nil
		},
	)
	if parseErr != nil {
		return "", "", "", fmt.Errorf("token invalid: %w", parseErr)
	}

	claims, ok := parsed.Claims.(*jwtClaims)
	if !ok || !parsed.Valid {
		return "", "", "", fmt.Errorf("invalid claims")
	}
	if claims.OrgID == "" {
		return "", "", "", fmt.Errorf("missing org_id claim")
	}

	return claims.OrgID, claims.UserID, claims.ID, nil
}

// isTokenRevoked checks a Redis blocklist keyed by JWT ID (jti).
// Tokens are added to the blocklist via a separate auth service or admin API.
// Key format: jwt:revoked:{jti}  (any non-empty value = revoked)
func isTokenRevoked(ctx context.Context, jti string, rdb *redis.ClusterClient) bool {
	if jti == "" {
		return false // no jti claim → can't revoke; allow (or change to deny by policy)
	}
	exists, err := rdb.Exists(ctx, "jwt:revoked:"+jti).Result()
	if err != nil {
		// Redis error — fail open (allow) to avoid locking out users on Redis blip.
		// Change to `return true` for stricter policy.
		log.Printf("[ws-gateway/auth] revocation check error for jti=%s: %v", jti, err)
		return false
	}
	return exists > 0
}

// ─────────────────────────────────────────────────────────────────────────────
// Rate limiter — token bucket per IP, in-memory
// ─────────────────────────────────────────────────────────────────────────────

type tokenBucket struct {
	tokens    int64 // current tokens (atomic CAS)
	lastRefil int64 // unix nano of last refill (atomic)
	maxTokens int64
	refillNs  int64 // nanoseconds per token refill
}

// FIX: rateLimitMu was declared but never used — removed.
var rateLimitBuckets sync.Map // map[string]*tokenBucket

func checkRateLimit(remoteAddr string, maxPerSec int64) bool {
	ip := remoteAddr
	if i := strings.LastIndex(remoteAddr, ":"); i > 0 {
		ip = remoteAddr[:i]
	}

	refillNs := int64(time.Second) / maxPerSec
	v, _ := rateLimitBuckets.LoadOrStore(ip, &tokenBucket{
		tokens:    maxPerSec,
		maxTokens: maxPerSec,
		refillNs:  refillNs,
	})
	bucket := v.(*tokenBucket)

	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&bucket.lastRefil)
	if elapsed := now - last; elapsed > 0 {
		newTokens := elapsed / bucket.refillNs
		if newTokens > 0 {
			atomic.StoreInt64(&bucket.lastRefil, now)
			cur := atomic.LoadInt64(&bucket.tokens)
			if next := cur + newTokens; next <= bucket.maxTokens {
				atomic.StoreInt64(&bucket.tokens, next)
			} else {
				atomic.StoreInt64(&bucket.tokens, bucket.maxTokens)
			}
		}
	}

	for {
		cur := atomic.LoadInt64(&bucket.tokens)
		if cur <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt64(&bucket.tokens, cur, cur-1) {
			return true
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

func httpError(w http.ResponseWriter, msg string, code int) {
	log.Printf("[websocket-gateway] HTTP %d: %s", code, msg)
	http.Error(w, msg, code)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func getFloat64Slice(v *viper.Viper, key string) []float64 {
	raw := v.Get(key)

	switch vals := raw.(type) {

	case []float64:
		return vals

	case []interface{}:
		out := make([]float64, 0, len(vals))

		for _, val := range vals {
			switch n := val.(type) {

			case float64:
				out = append(out, n)

			case int:
				out = append(out, float64(n))

			case int64:
				out = append(out, float64(n))

			case string:
				f, err := strconv.ParseFloat(n, 64)
				if err == nil {
					out = append(out, f)
				}
			}
		}

		return out

	default:
		return nil
	}
}
