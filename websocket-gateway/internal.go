// websocket-gateway/internal.go
// Defines all types, client wrappers, and helper functions used by main.go.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
// ClickHouse — query-only client for initial position snapshots
// ─────────────────────────────────────────────────────────────────────────────

// CHClient wraps driver.Conn and exposes only the Query surface needed by
// the WebSocket gateway. Using a wrapper avoids importing clickhouse-go types
// directly in main.go.
type CHClient struct {
	conn driver.Conn
}

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
		Settings: clickhouse.Settings{
			"max_execution_time": int(cfg.QueryTimeout.Seconds()),
		},
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

// Query delegates to the underlying driver.Conn.
func (c *CHClient) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket message types
// ─────────────────────────────────────────────────────────────────────────────

// WebSocketMessage is the JSON envelope for every client ↔ gateway message.
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
}

// VehiclePosition is the snapshot row returned by sendInitialPositions.
type VehiclePosition struct {
	VehicleID  string    `json:"vehicle_id"  ch:"vehicle_id"`
	Latitude   float64   `json:"lat"         ch:"latitude"`
	Longitude  float64   `json:"lon"         ch:"longitude"`
	Speed      int16     `json:"speed"       ch:"speed"`
	Heading    int16     `json:"heading"     ch:"heading"`
	RecordedAt time.Time `json:"recorded_at" ch:"recorded_at"`
}

// PositionUpdate is parsed from a Redis stream entry.
type PositionUpdate struct {
	VehicleID string
	Latitude  float64
	Longitude float64
	Speed     int16
	Heading   int16
	Timestamp int64
}

// parsePositionUpdate extracts a PositionUpdate from a Redis stream message
// Values map (all values are strings in the Redis wire format).
func parsePositionUpdate(values map[string]interface{}) PositionUpdate {
	get := func(key string) string {
		if v, ok := values[key]; ok {
			return fmt.Sprintf("%v", v)
		}
		return ""
	}
	var pu PositionUpdate
	pu.VehicleID = get("vehicle_id")
	fmt.Sscanf(get("latitude"), "%f", &pu.Latitude)
	fmt.Sscanf(get("longitude"), "%f", &pu.Longitude)
	fmt.Sscanf(get("speed"), "%d", &pu.Speed)
	fmt.Sscanf(get("heading"), "%d", &pu.Heading)
	fmt.Sscanf(get("device_timestamp_ms"), "%d", &pu.Timestamp)
	return pu
}

// ─────────────────────────────────────────────────────────────────────────────
// JWT authentication
// ─────────────────────────────────────────────────────────────────────────────

type jwtClaims struct {
	OrgID  string `json:"org_id"`
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

// authenticateJWT validates the Authorization header and returns (orgID, userID).
// Accepts both "Bearer <token>" and raw "<token>" formats.
func authenticateJWT(authHeader string, cfg AuthConfig) (string, string, error) {
	token := strings.TrimPrefix(authHeader, "Bearer ")
	token = strings.TrimSpace(token)
	if token == "" {
		return "", "", fmt.Errorf("missing token")
	}

	parsed, err := jwt.ParseWithClaims(token, &jwtClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(cfg.JWTSecret), nil
	})
	if err != nil {
		return "", "", fmt.Errorf("token invalid: %w", err)
	}

	claims, ok := parsed.Claims.(*jwtClaims)
	if !ok || !parsed.Valid {
		return "", "", fmt.Errorf("invalid claims")
	}
	if claims.OrgID == "" {
		return "", "", fmt.Errorf("missing org_id claim")
	}
	return claims.OrgID, claims.UserID, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Rate limiter — token bucket per IP, in-memory
// ─────────────────────────────────────────────────────────────────────────────

type tokenBucket struct {
	tokens    int64 // current tokens (atomic)
	lastRefil int64 // unix nano of last refill (atomic)
	maxTokens int64
	refillNs  int64 // nanoseconds per token refill
}

var (
	rateLimitMu      sync.Mutex
	rateLimitBuckets sync.Map // map[string]*tokenBucket
)

func checkRateLimit(remoteAddr, _ string) bool {
	ip := remoteAddr
	if i := strings.LastIndex(remoteAddr, ":"); i > 0 {
		ip = remoteAddr[:i]
	}

	v, _ := rateLimitBuckets.LoadOrStore(ip, &tokenBucket{
		tokens:    100,
		maxTokens: 100,
		// Refill 100 tokens/sec = 1 token per 10ms
		refillNs: int64(10 * time.Millisecond),
	})
	bucket := v.(*tokenBucket)

	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&bucket.lastRefil)
	elapsed := now - last
	if elapsed > 0 {
		newTokens := elapsed / bucket.refillNs
		if newTokens > 0 {
			atomic.StoreInt64(&bucket.lastRefil, now)
			cur := atomic.LoadInt64(&bucket.tokens)
			next := cur + newTokens
			if next > bucket.maxTokens {
				next = bucket.maxTokens
			}
			atomic.StoreInt64(&bucket.tokens, next)
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

func generateUUID() string {
	return uuid.NewString()
}

// httpError is a helper that also logs the rejection.
func httpError(w http.ResponseWriter, msg string, code int) {
	log.Printf("[websocket-gateway] HTTP %d: %s", code, msg)
	http.Error(w, msg, code)
}
