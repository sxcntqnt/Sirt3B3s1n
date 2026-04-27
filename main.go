// Matatu Pulse — GPS Pipeline Orchestrator
// Manages mqtt-consumer, batch-writer, and websocket-gateway as supervised
// subprocesses. Provides auto-scaling, health monitoring, circuit-breaking,
// and a control-plane HTTP API. Designed for 100 000 concurrent vehicles
// (~20 000 GPS events/sec at the default 5-second update interval).
//
// Shutdown order (reverse dependency):
//   websocket-gateway → mqtt-consumer pool → batch-writer
//
// Auto-scaling signal:
//   Reads batch_writer_redis_stream_lag from each batch-writer /metrics
//   endpoint and scales the MQTT consumer pool up/down accordingly.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

// OrchestratorConfig holds all tuneable parameters. Values can be overridden
// with environment variables (see loadOrchestratorConfig).
type OrchestratorConfig struct {
	// BinaryDir is the directory that contains compiled service binaries.
	// Defaults to the directory of the orchestrator itself.
	BinaryDir string

	// UseGoRun instructs the orchestrator to use "go run ./…" instead of
	// pre-built binaries. Useful in development.
	UseGoRun bool

	// ControlPlane is the address for the admin HTTP API.
	ControlPlaneAddr string

	// ScaleInterval is how often the auto-scaler evaluates metrics.
	ScaleInterval time.Duration

	// HealthCheckInterval is how often service health is polled.
	HealthCheckInterval time.Duration

	// HealthCheckTimeout is the per-request timeout for health polls.
	HealthCheckTimeout time.Duration

	// UnhealthyThreshold is the number of consecutive failures before a
	// replica is considered dead and is restarted.
	UnhealthyThreshold int

	// RestartBackoffMax caps exponential restart back-off.
	RestartBackoffMax time.Duration

	Services ServicesConfig
}

type ServicesConfig struct {
	MQTTConsumer    ServiceConfig
	BatchWriter     ServiceConfig
	WebSocketGateway ServiceConfig
}

// ServiceConfig describes one service pool.
type ServiceConfig struct {
	// Binary is the executable name relative to BinaryDir (or the package
	// path when UseGoRun is true).
	Binary string

	MinReplicas int
	MaxReplicas int

	// BasePort is the first metrics/health port. Replicas get BasePort+N.
	BaseMetricsPort int
	BaseHealthPort  int

	// Env holds additional environment variables for this service.
	// Values may reference OS env with ${VAR} expansion.
	Env map[string]string
}

func loadOrchestratorConfig() OrchestratorConfig {
	cfg := OrchestratorConfig{
		BinaryDir:           envOr("BINARY_DIR", "."),
		UseGoRun:            envBool("USE_GO_RUN", false),
		ControlPlaneAddr:    envOr("CONTROL_PLANE_ADDR", ":7070"),
		ScaleInterval:       envDuration("SCALE_INTERVAL", 15*time.Second),
		HealthCheckInterval: envDuration("HEALTH_CHECK_INTERVAL", 10*time.Second),
		HealthCheckTimeout:  envDuration("HEALTH_CHECK_TIMEOUT", 3*time.Second),
		UnhealthyThreshold:  envInt("UNHEALTHY_THRESHOLD", 3),
		RestartBackoffMax:   envDuration("RESTART_BACKOFF_MAX", 60*time.Second),

		Services: ServicesConfig{
			MQTTConsumer: ServiceConfig{
				Binary:          envOr("MQTT_CONSUMER_BINARY", "mqtt-consumer"),
				MinReplicas:     envInt("MQTT_CONSUMER_MIN", 4),
				MaxReplicas:     envInt("MQTT_CONSUMER_MAX", 32),
				BaseMetricsPort: envInt("MQTT_CONSUMER_METRICS_BASE_PORT", 9100),
				BaseHealthPort:  envInt("MQTT_CONSUMER_HEALTH_BASE_PORT", 8100),
				Env: map[string]string{
					"MQTT_BROKER":      "${MQTT_BROKER}",
					"MQTT_USERNAME":    "${MQTT_USERNAME}",
					"MQTT_PASSWORD":    "${MQTT_PASSWORD}",
					"REDIS_PASSWORD":   "${REDIS_PASSWORD}",
					"JAEGER_ENDPOINT":  "${JAEGER_ENDPOINT}",
				},
			},
			BatchWriter: ServiceConfig{
				Binary:          envOr("BATCH_WRITER_BINARY", "batch-writer"),
				MinReplicas:     envInt("BATCH_WRITER_MIN", 2),
				MaxReplicas:     envInt("BATCH_WRITER_MAX", 8),
				BaseMetricsPort: envInt("BATCH_WRITER_METRICS_BASE_PORT", 9200),
				BaseHealthPort:  envInt("BATCH_WRITER_HEALTH_BASE_PORT", 8200),
				Env: map[string]string{
					"REDIS_PASSWORD":       "${REDIS_PASSWORD}",
					"CLICKHOUSE_HOST":      "${CLICKHOUSE_HOST}",
					"CLICKHOUSE_USERNAME":  "${CLICKHOUSE_USERNAME}",
					"CLICKHOUSE_PASSWORD":  "${CLICKHOUSE_PASSWORD}",
					"JAEGER_ENDPOINT":      "${JAEGER_ENDPOINT}",
				},
			},
			WebSocketGateway: ServiceConfig{
				Binary:          envOr("WS_GATEWAY_BINARY", "websocket-gateway"),
				MinReplicas:     envInt("WS_GATEWAY_MIN", 2),
				MaxReplicas:     envInt("WS_GATEWAY_MAX", 10),
				BaseMetricsPort: envInt("WS_GATEWAY_METRICS_BASE_PORT", 9300),
				BaseHealthPort:  envInt("WS_GATEWAY_HEALTH_BASE_PORT", 8300),
				Env: map[string]string{
					"REDIS_PASSWORD":   "${REDIS_PASSWORD}",
					"CLICKHOUSE_HOST":  "${CLICKHOUSE_HOST}",
					"AUTH_SERVICE_URL": "${AUTH_SERVICE_URL}",
					"JWT_SECRET":       "${JWT_SECRET}",
					"JAEGER_ENDPOINT":  "${JAEGER_ENDPOINT}",
				},
			},
		},
	}
	return cfg
}

// ─────────────────────────────────────────────────────────────────────────────
// Process Instance
// ─────────────────────────────────────────────────────────────────────────────

type InstanceState int

const (
	StateStarting InstanceState = iota
	StateRunning
	StateUnhealthy
	StateStopped
)

func (s InstanceState) String() string {
	return [...]string{"starting", "running", "unhealthy", "stopped"}[s]
}

// ProcessInstance represents a single supervised OS process.
type ProcessInstance struct {
	ID          string
	ServiceName string
	Replica     int
	MetricsPort int
	HealthPort  int

	mu              sync.RWMutex
	cmd             *exec.Cmd
	state           InstanceState
	startedAt       time.Time
	unhealthyCount  int
	restartCount    int
	lastRestartAt   time.Time

	cancelFn context.CancelFunc
}

func (pi *ProcessInstance) State() InstanceState {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.state
}

func (pi *ProcessInstance) MetricsURL() string {
	return fmt.Sprintf("http://localhost:%d/metrics", pi.MetricsPort)
}

func (pi *ProcessInstance) HealthURL() string {
	return fmt.Sprintf("http://localhost:%d/health", pi.HealthPort)
}

// ─────────────────────────────────────────────────────────────────────────────
// Service Pool
// ─────────────────────────────────────────────────────────────────────────────

// ServicePool manages a horizontally scaled set of process instances for a
// single service type.
type ServicePool struct {
	name   string
	cfg    ServiceConfig
	orchCfg OrchestratorConfig

	mu        sync.RWMutex
	instances []*ProcessInstance

	metrics *PoolMetrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewServicePool(ctx context.Context, name string, cfg ServiceConfig, orchCfg OrchestratorConfig, metrics *PoolMetrics) *ServicePool {
	poolCtx, poolCancel := context.WithCancel(ctx)
	return &ServicePool{
		name:    name,
		cfg:     cfg,
		orchCfg: orchCfg,
		metrics: metrics,
		ctx:     poolCtx,
		cancel:  poolCancel,
	}
}

// Start spawns MinReplicas instances and begins the supervision loop.
func (sp *ServicePool) Start() error {
	log.Printf("[orchestrator] starting pool %s with %d replica(s)", sp.name, sp.cfg.MinReplicas)
	for i := 0; i < sp.cfg.MinReplicas; i++ {
		if err := sp.addReplica(); err != nil {
			return fmt.Errorf("pool %s: initial replica %d: %w", sp.name, i, err)
		}
	}
	sp.wg.Add(1)
	go sp.supervisionLoop()
	return nil
}

// Stop gracefully terminates all instances, waiting for them to exit.
func (sp *ServicePool) Stop(timeout time.Duration) {
	log.Printf("[orchestrator] stopping pool %s", sp.name)
	sp.cancel()

	deadline := time.After(timeout)
	sp.mu.RLock()
	instances := make([]*ProcessInstance, len(sp.instances))
	copy(instances, sp.instances)
	sp.mu.RUnlock()

	var wg sync.WaitGroup
	for _, inst := range instances {
		wg.Add(1)
		go func(pi *ProcessInstance) {
			defer wg.Done()
			pi.mu.Lock()
			if pi.cmd != nil && pi.cmd.Process != nil {
				_ = pi.cmd.Process.Signal(syscall.SIGTERM)
			}
			pi.mu.Unlock()
		}(inst)
	}
	wg.Wait()

	done := make(chan struct{})
	go func() {
		sp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[orchestrator] pool %s stopped cleanly", sp.name)
	case <-deadline:
		log.Printf("[orchestrator] pool %s stop timeout — forcing SIGKILL", sp.name)
		sp.mu.RLock()
		for _, inst := range sp.instances {
			inst.mu.Lock()
			if inst.cmd != nil && inst.cmd.Process != nil {
				_ = inst.cmd.Process.Kill()
			}
			inst.mu.Unlock()
		}
		sp.mu.RUnlock()
	}
}

// ScaleTo adjusts the pool to exactly n replicas (within Min/Max bounds).
func (sp *ServicePool) ScaleTo(n int) {
	if n < sp.cfg.MinReplicas {
		n = sp.cfg.MinReplicas
	}
	if n > sp.cfg.MaxReplicas {
		n = sp.cfg.MaxReplicas
	}

	sp.mu.RLock()
	current := len(sp.instances)
	sp.mu.RUnlock()

	if n == current {
		return
	}

	if n > current {
		log.Printf("[orchestrator] scaling %s up: %d → %d", sp.name, current, n)
		for i := current; i < n; i++ {
			if err := sp.addReplica(); err != nil {
				log.Printf("[orchestrator] scale-up %s replica %d: %v", sp.name, i, err)
			}
		}
		sp.metrics.ReplicaCount.WithLabelValues(sp.name).Set(float64(n))
		return
	}

	// Scale down — remove the most recently added replicas.
	log.Printf("[orchestrator] scaling %s down: %d → %d", sp.name, current, n)
	sp.mu.Lock()
	toStop := sp.instances[n:]
	sp.instances = sp.instances[:n]
	sp.mu.Unlock()

	for _, inst := range toStop {
		go func(pi *ProcessInstance) {
			pi.cancelFn()
			pi.mu.Lock()
			if pi.cmd != nil && pi.cmd.Process != nil {
				_ = pi.cmd.Process.Signal(syscall.SIGTERM)
			}
			pi.mu.Unlock()
		}(inst)
	}
	sp.metrics.ReplicaCount.WithLabelValues(sp.name).Set(float64(n))
}

// ReplicaCount returns the number of currently active instances.
func (sp *ServicePool) ReplicaCount() int {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return len(sp.instances)
}

// HealthySummary returns (healthy, total) counts.
func (sp *ServicePool) HealthySummary() (int, int) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	healthy := 0
	for _, inst := range sp.instances {
		if inst.State() == StateRunning {
			healthy++
		}
	}
	return healthy, len(sp.instances)
}

func (sp *ServicePool) addReplica() error {
	sp.mu.Lock()
	replica := len(sp.instances)
	sp.mu.Unlock()

	instCtx, instCancel := context.WithCancel(sp.ctx)
	inst := &ProcessInstance{
		ID:          fmt.Sprintf("%s-%d", sp.name, replica),
		ServiceName: sp.name,
		Replica:     replica,
		MetricsPort: sp.cfg.BaseMetricsPort + replica,
		HealthPort:  sp.cfg.BaseHealthPort + replica,
		cancelFn:    instCancel,
		state:       StateStarting,
	}

	if err := sp.spawnProcess(instCtx, inst); err != nil {
		instCancel()
		return err
	}

	sp.mu.Lock()
	sp.instances = append(sp.instances, inst)
	sp.mu.Unlock()

	sp.wg.Add(1)
	go sp.watchProcess(instCtx, inst)

	return nil
}

func (sp *ServicePool) spawnProcess(ctx context.Context, inst *ProcessInstance) error {
	env := os.Environ()

	// Inject per-replica port overrides.
	env = append(env,
		fmt.Sprintf("METRICS_PORT=%d", inst.MetricsPort),
		fmt.Sprintf("HEALTH_PORT=%d", inst.HealthPort),
		fmt.Sprintf("INSTANCE_ID=%s", inst.ID),
		fmt.Sprintf("REPLICA=%d", inst.Replica),
	)

	// Merge service-specific env, expanding ${VAR} references.
	for k, v := range sp.cfg.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, os.ExpandEnv(v)))
	}

	var cmd *exec.Cmd
	if sp.orchCfg.UseGoRun {
		// Development mode: go run ./service-dir
		cmd = exec.CommandContext(ctx, "go", "run", "./"+sp.cfg.Binary)
	} else {
		binary := filepath.Join(sp.orchCfg.BinaryDir, sp.cfg.Binary)
		cmd = exec.CommandContext(ctx, binary)
	}

	cmd.Env = env
	cmd.Stdout = newPrefixWriter(os.Stdout, "["+inst.ID+"] ")
	cmd.Stderr = newPrefixWriter(os.Stderr, "["+inst.ID+"] ")

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn %s: %w", inst.ID, err)
	}

	inst.mu.Lock()
	inst.cmd = cmd
	inst.startedAt = time.Now()
	inst.state = StateStarting
	inst.mu.Unlock()

	log.Printf("[orchestrator] spawned %s (pid %d) metrics:%d health:%d",
		inst.ID, cmd.Process.Pid, inst.MetricsPort, inst.HealthPort)

	return nil
}

// watchProcess waits for a process to exit and handles restart with backoff.
func (sp *ServicePool) watchProcess(ctx context.Context, inst *ProcessInstance) {
	defer sp.wg.Done()

	backoff := 1 * time.Second

	for {
		// Wait for the process to exit.
		inst.mu.RLock()
		cmd := inst.cmd
		inst.mu.RUnlock()

		if cmd != nil {
			_ = cmd.Wait()
		}

		select {
		case <-ctx.Done():
			inst.mu.Lock()
			inst.state = StateStopped
			inst.mu.Unlock()
			log.Printf("[orchestrator] %s stopped (context cancelled)", inst.ID)
			sp.metrics.ReplicaState.WithLabelValues(inst.ID, "stopped").Set(1)
			return
		default:
		}

		inst.mu.Lock()
		inst.state = StateStarting
		inst.restartCount++
		inst.lastRestartAt = time.Now()
		inst.mu.Unlock()

		log.Printf("[orchestrator] %s exited unexpectedly — restart #%d in %s",
			inst.ID, inst.restartCount, backoff)

		sp.metrics.RestartTotal.WithLabelValues(sp.name).Inc()

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Exponential back-off with jitter.
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > sp.orchCfg.RestartBackoffMax {
			backoff = sp.orchCfg.RestartBackoffMax
		}
		backoff += time.Duration(rand.Int63n(int64(backoff / 4)))

		if err := sp.spawnProcess(ctx, inst); err != nil {
			log.Printf("[orchestrator] respawn %s failed: %v", inst.ID, err)
		}
	}
}

// supervisionLoop periodically health-checks all instances and replaces any
// that exceed the unhealthy threshold.
func (sp *ServicePool) supervisionLoop() {
	defer sp.wg.Done()

	ticker := time.NewTicker(sp.orchCfg.HealthCheckInterval)
	defer ticker.Stop()

	httpClient := &http.Client{Timeout: sp.orchCfg.HealthCheckTimeout}

	for {
		select {
		case <-sp.ctx.Done():
			return
		case <-ticker.C:
			sp.checkHealth(httpClient)
		}
	}
}

func (sp *ServicePool) checkHealth(client *http.Client) {
	sp.mu.RLock()
	instances := make([]*ProcessInstance, len(sp.instances))
	copy(instances, sp.instances)
	sp.mu.RUnlock()

	for _, inst := range instances {
		if inst.State() == StateStopped {
			continue
		}

		// Prefer /health; fall back to /metrics presence.
		healthy := false
		if resp, err := client.Get(inst.HealthURL()); err == nil {
			healthy = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		} else if resp, err := client.Get(inst.MetricsURL()); err == nil {
			healthy = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		}

		inst.mu.Lock()
		if healthy {
			inst.state = StateRunning
			inst.unhealthyCount = 0
			sp.metrics.HealthStatus.WithLabelValues(inst.ID).Set(1)
		} else {
			inst.unhealthyCount++
			sp.metrics.HealthStatus.WithLabelValues(inst.ID).Set(0)
			if inst.unhealthyCount >= sp.orchCfg.UnhealthyThreshold {
				log.Printf("[orchestrator] %s unhealthy for %d checks — signalling restart",
					inst.ID, inst.unhealthyCount)
				inst.state = StateUnhealthy
				if inst.cmd != nil && inst.cmd.Process != nil {
					_ = inst.cmd.Process.Signal(syscall.SIGTERM)
				}
				inst.unhealthyCount = 0
			}
		}
		inst.mu.Unlock()
	}

	healthy, total := sp.HealthySummary()
	sp.metrics.ReplicaCount.WithLabelValues(sp.name).Set(float64(total))
	log.Printf("[orchestrator] pool %s health: %d/%d healthy", sp.name, healthy, total)
}

// ─────────────────────────────────────────────────────────────────────────────
// Auto-Scaler
// ─────────────────────────────────────────────────────────────────────────────

// AutoScaler reads the Redis stream lag metric from the batch-writer pool's
// /metrics endpoints and adjusts the MQTT consumer pool size accordingly.
//
// Scaling policy (tuned for 100 000 vehicles / ~20 000 events/sec):
//
//	lag > scaleUpThreshold  → add 2 MQTT consumer replicas (up to MaxReplicas)
//	lag < scaleDownThreshold → remove 1 MQTT consumer replica (down to MinReplicas)
type AutoScaler struct {
	mqttPool    *ServicePool
	batchPool   *ServicePool
	wsPool      *ServicePool
	cfg         OrchestratorConfig
	metrics     *ScalerMetrics

	// Thresholds are in number of Redis stream messages.
	scaleUpThreshold   int64
	scaleDownThreshold int64

	httpClient *http.Client
}

func NewAutoScaler(mqttPool, batchPool, wsPool *ServicePool, cfg OrchestratorConfig, metrics *ScalerMetrics) *AutoScaler {
	return &AutoScaler{
		mqttPool:           mqttPool,
		batchPool:          batchPool,
		wsPool:             wsPool,
		cfg:                cfg,
		metrics:            metrics,
		scaleUpThreshold:   envInt64("SCALE_UP_LAG", 50000),
		scaleDownThreshold: envInt64("SCALE_DOWN_LAG", 5000),
		httpClient:         &http.Client{Timeout: 3 * time.Second},
	}
}

// Run starts the scaling evaluation loop. It blocks until ctx is cancelled.
func (as *AutoScaler) Run(ctx context.Context) {
	ticker := time.NewTicker(as.cfg.ScaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			as.evaluate()
		}
	}
}

func (as *AutoScaler) evaluate() {
	lag := as.sampleStreamLag()
	as.metrics.StreamLag.Set(float64(lag))
	log.Printf("[autoscaler] Redis batch stream lag: %d messages", lag)

	currentMQTT := as.mqttPool.ReplicaCount()

	if lag > as.scaleUpThreshold {
		target := currentMQTT + 2
		log.Printf("[autoscaler] lag=%d > threshold=%d — scaling mqtt-consumer %d → %d",
			lag, as.scaleUpThreshold, currentMQTT, target)
		as.mqttPool.ScaleTo(target)
	} else if lag < as.scaleDownThreshold && currentMQTT > as.cfg.Services.MQTTConsumer.MinReplicas {
		target := currentMQTT - 1
		log.Printf("[autoscaler] lag=%d < threshold=%d — scaling mqtt-consumer %d → %d",
			lag, as.scaleDownThreshold, currentMQTT, target)
		as.mqttPool.ScaleTo(target)
	}

	// Also scale WebSocket gateways proportionally to MQTT consumers.
	// Target ratio: 1 WS gateway per 3 MQTT consumers, min 2 gateways.
	currentMQTT = as.mqttPool.ReplicaCount()
	targetWS := currentMQTT/3 + 1
	if targetWS < as.cfg.Services.WebSocketGateway.MinReplicas {
		targetWS = as.cfg.Services.WebSocketGateway.MinReplicas
	}
	as.wsPool.ScaleTo(targetWS)
}

// sampleStreamLag reads the batch_writer_redis_stream_lag gauge from all
// batch-writer /metrics endpoints and returns the maximum observed value.
func (as *AutoScaler) sampleStreamLag() int64 {
	as.batchPool.mu.RLock()
	instances := make([]*ProcessInstance, len(as.batchPool.instances))
	copy(instances, as.batchPool.instances)
	as.batchPool.mu.RUnlock()

	var maxLag int64
	for _, inst := range instances {
		if inst.State() != StateRunning {
			continue
		}
		lag := as.fetchMetricGauge(inst.MetricsURL(), "batch_writer_redis_stream_lag")
		if lag > maxLag {
			maxLag = lag
		}
	}
	return maxLag
}

// fetchMetricGauge scrapes a Prometheus text endpoint and returns the value
// of the named gauge metric.
func (as *AutoScaler) fetchMetricGauge(url, metricName string) int64 {
	resp, err := as.httpClient.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, metricName) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		f, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		return int64(f)
	}
	return 0
}

// ─────────────────────────────────────────────────────────────────────────────
// Control Plane HTTP API
// ─────────────────────────────────────────────────────────────────────────────

// ControlPlane exposes an admin HTTP API for the orchestrator.
type ControlPlane struct {
	addr      string
	mqtt      *ServicePool
	batch     *ServicePool
	ws        *ServicePool
	scaler    *AutoScaler
	server    *http.Server
	startedAt time.Time
}

func NewControlPlane(addr string, mqtt, batch, ws *ServicePool, scaler *AutoScaler) *ControlPlane {
	cp := &ControlPlane{
		addr:      addr,
		mqtt:      mqtt,
		batch:     batch,
		ws:        ws,
		scaler:    scaler,
		startedAt: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", cp.handleHealth)
	mux.HandleFunc("/status", cp.handleStatus)
	mux.HandleFunc("/scale", cp.handleScale)
	mux.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)

	cp.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return cp
}

func (cp *ControlPlane) Start() {
	log.Printf("[control-plane] listening on %s", cp.addr)
	go func() {
		if err := cp.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[control-plane] server error: %v", err)
		}
	}()
}

func (cp *ControlPlane) Shutdown(ctx context.Context) {
	_ = cp.server.Shutdown(ctx)
}

// GET /health  — returns 200 if at least one replica per pool is healthy.
func (cp *ControlPlane) handleHealth(w http.ResponseWriter, _ *http.Request) {
	type poolHealth struct {
		Healthy int `json:"healthy"`
		Total   int `json:"total"`
	}
	type response struct {
		Status string                 `json:"status"`
		Pools  map[string]poolHealth  `json:"pools"`
	}

	mqttH, mqttT := cp.mqtt.HealthySummary()
	batchH, batchT := cp.batch.HealthySummary()
	wsH, wsT := cp.ws.HealthySummary()

	allOK := mqttH > 0 && batchH > 0 && wsH > 0
	status := "ok"
	httpStatus := http.StatusOK
	if !allOK {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	resp := response{
		Status: status,
		Pools: map[string]poolHealth{
			"mqtt-consumer":    {mqttH, mqttT},
			"batch-writer":     {batchH, batchT},
			"websocket-gateway": {wsH, wsT},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(resp)
}

// GET /status  — detailed per-instance status.
func (cp *ControlPlane) handleStatus(w http.ResponseWriter, _ *http.Request) {
	type instanceStatus struct {
		ID            string `json:"id"`
		State         string `json:"state"`
		MetricsPort   int    `json:"metrics_port"`
		Restarts      int    `json:"restarts"`
		UptimeSeconds int64  `json:"uptime_seconds"`
	}
	type poolStatus struct {
		Name      string           `json:"name"`
		Replicas  int              `json:"replicas"`
		Instances []instanceStatus `json:"instances"`
	}
	type response struct {
		OrchestratorUptimeSeconds int64        `json:"orchestrator_uptime_seconds"`
		Pools                     []poolStatus `json:"pools"`
		StreamLagMessages         int64        `json:"stream_lag_messages"`
	}

	makePool := func(pool *ServicePool) poolStatus {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		ps := poolStatus{Name: pool.name, Replicas: len(pool.instances)}
		for _, inst := range pool.instances {
			inst.mu.RLock()
			ps.Instances = append(ps.Instances, instanceStatus{
				ID:           inst.ID,
				State:        inst.state.String(),
				MetricsPort:  inst.MetricsPort,
				Restarts:     inst.restartCount,
				UptimeSeconds: int64(time.Since(inst.startedAt).Seconds()),
			})
			inst.mu.RUnlock()
		}
		return ps
	}

	resp := response{
		OrchestratorUptimeSeconds: int64(time.Since(cp.startedAt).Seconds()),
		Pools: []poolStatus{
			makePool(cp.mqtt),
			makePool(cp.batch),
			makePool(cp.ws),
		},
		StreamLagMessages: cp.scaler.sampleStreamLag(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// POST /scale?service=mqtt-consumer&replicas=8  — manual override.
func (cp *ControlPlane) handleScale(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	svc := r.URL.Query().Get("service")
	nStr := r.URL.Query().Get("replicas")
	n, err := strconv.Atoi(nStr)
	if err != nil || n < 1 {
		http.Error(w, "invalid replicas parameter", http.StatusBadRequest)
		return
	}

	var pool *ServicePool
	switch svc {
	case "mqtt-consumer":
		pool = cp.mqtt
	case "batch-writer":
		pool = cp.batch
	case "websocket-gateway":
		pool = cp.ws
	default:
		http.Error(w, "unknown service; use mqtt-consumer, batch-writer, or websocket-gateway", http.StatusBadRequest)
		return
	}

	pool.ScaleTo(n)
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"service":%q,"requested_replicas":%d}`, svc, n)
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus Metrics
// ─────────────────────────────────────────────────────────────────────────────

type PoolMetrics struct {
	ReplicaCount  *prometheus.GaugeVec
	HealthStatus  *prometheus.GaugeVec
	RestartTotal  *prometheus.CounterVec
	ReplicaState  *prometheus.GaugeVec
}

func NewPoolMetrics() *PoolMetrics {
	m := &PoolMetrics{
		ReplicaCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "orchestrator_pool_replicas",
			Help: "Number of replicas in each service pool",
		}, []string{"pool"}),

		HealthStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "orchestrator_instance_healthy",
			Help: "1 if instance is healthy, 0 otherwise",
		}, []string{"instance"}),

		RestartTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orchestrator_instance_restarts_total",
			Help: "Total number of process restarts per pool",
		}, []string{"pool"}),

		ReplicaState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "orchestrator_replica_state",
			Help: "Current state of a replica (1 = in given state)",
		}, []string{"instance", "state"}),
	}
	prometheus.MustRegister(m.ReplicaCount, m.HealthStatus, m.RestartTotal, m.ReplicaState)
	return m
}

type ScalerMetrics struct {
	StreamLag prometheus.Gauge
}

func NewScalerMetrics() *ScalerMetrics {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "orchestrator_stream_lag_messages",
		Help: "Current Redis batch stream lag observed by the auto-scaler",
	})
	prometheus.MustRegister(g)
	return &ScalerMetrics{StreamLag: g}
}

// ─────────────────────────────────────────────────────────────────────────────
// Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

// Orchestrator ties all components together and manages the overall lifecycle.
type Orchestrator struct {
	cfg     OrchestratorConfig
	metrics *PoolMetrics

	mqtt  *ServicePool
	batch *ServicePool
	ws    *ServicePool

	scaler       *AutoScaler
	controlPlane *ControlPlane

	startedAt time.Time
}

func NewOrchestrator(cfg OrchestratorConfig) *Orchestrator {
	return &Orchestrator{
		cfg:       cfg,
		metrics:   NewPoolMetrics(),
		startedAt: time.Now(),
	}
}

// Start brings up the full pipeline in dependency order:
//  1. Batch Writer  (drains cold path; must be up before consumers)
//  2. MQTT Consumer pool
//  3. WebSocket Gateway pool
//  4. Auto-scaler
//  5. Control plane
func (o *Orchestrator) Start(ctx context.Context) error {
	log.Println("[orchestrator] ═══════════════════════════════════════════")
	log.Println("[orchestrator]  Matatu Pulse — GPS Pipeline Orchestrator")
	log.Printf ("[orchestrator]  Target: 100 000 vehicles / ~20 000 events/sec")
	log.Println("[orchestrator] ═══════════════════════════════════════════")

	o.batch = NewServicePool(ctx, "batch-writer", o.cfg.Services.BatchWriter, o.cfg, o.metrics)
	o.mqtt  = NewServicePool(ctx, "mqtt-consumer", o.cfg.Services.MQTTConsumer, o.cfg, o.metrics)
	o.ws    = NewServicePool(ctx, "websocket-gateway", o.cfg.Services.WebSocketGateway, o.cfg, o.metrics)

	// 1. Batch Writer first — consumers will start sending to the cold path
	//    immediately; the batch writer must already be listening.
	if err := o.batch.Start(); err != nil {
		return fmt.Errorf("batch-writer pool: %w", err)
	}
	log.Println("[orchestrator] batch-writer pool started — waiting for ready...")
	o.waitForPoolReady(o.batch, 30*time.Second)

	// 2. MQTT Consumers.
	if err := o.mqtt.Start(); err != nil {
		return fmt.Errorf("mqtt-consumer pool: %w", err)
	}

	// 3. WebSocket Gateway.
	if err := o.ws.Start(); err != nil {
		return fmt.Errorf("websocket-gateway pool: %w", err)
	}

	// 4. Auto-scaler.
	scalerMetrics := NewScalerMetrics()
	o.scaler = NewAutoScaler(o.mqtt, o.batch, o.ws, o.cfg, scalerMetrics)
	go o.scaler.Run(ctx)

	// 5. Control plane.
	o.controlPlane = NewControlPlane(o.cfg.ControlPlaneAddr, o.mqtt, o.batch, o.ws, o.scaler)
	o.controlPlane.Start()

	log.Println("[orchestrator] all pools started successfully")
	o.printStartupSummary()
	return nil
}

// Shutdown tears down the pipeline in reverse dependency order with a 30 s
// timeout per pool.
func (o *Orchestrator) Shutdown() {
	log.Println("[orchestrator] initiating graceful shutdown...")

	const poolTimeout = 30 * time.Second

	// 1. Stop WebSocket Gateway — no new clients, drain existing connections.
	if o.ws != nil {
		o.ws.Stop(poolTimeout)
	}

	// 2. Stop MQTT Consumers — no more events enter the pipeline.
	if o.mqtt != nil {
		o.mqtt.Stop(poolTimeout)
	}

	// 3. Stop Batch Writer last — allows remaining Redis stream messages to
	//    be flushed to ClickHouse before exiting.
	if o.batch != nil {
		// Give it extra time to drain.
		o.batch.Stop(poolTimeout * 2)
	}

	// 4. Shutdown control plane.
	if o.controlPlane != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		o.controlPlane.Shutdown(ctx)
	}

	log.Println("[orchestrator] shutdown complete")
}

// waitForPoolReady polls until at least one instance in the pool is healthy or
// the timeout expires.
func (o *Orchestrator) waitForPoolReady(pool *ServicePool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		pool.checkHealth(client)
		h, _ := pool.HealthySummary()
		if h > 0 {
			return
		}
	}
	log.Printf("[orchestrator] WARNING: pool %s did not become ready within %s", pool.name, timeout)
}

func (o *Orchestrator) printStartupSummary() {
	log.Println("[orchestrator] ───────────────────────────────────────────")
	log.Printf("[orchestrator]  mqtt-consumer  : %d replicas (max %d)",
		o.mqtt.ReplicaCount(), o.cfg.Services.MQTTConsumer.MaxReplicas)
	log.Printf("[orchestrator]  batch-writer   : %d replicas (max %d)",
		o.batch.ReplicaCount(), o.cfg.Services.BatchWriter.MaxReplicas)
	log.Printf("[orchestrator]  websocket-gw   : %d replicas (max %d)",
		o.ws.ReplicaCount(), o.cfg.Services.WebSocketGateway.MaxReplicas)
	log.Printf("[orchestrator]  control-plane  : %s", o.cfg.ControlPlaneAddr)
	log.Printf("[orchestrator]  scale-interval : %s", o.cfg.ScaleInterval)
	log.Println("[orchestrator] ───────────────────────────────────────────")
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

// prefixWriter wraps an io.Writer and prepends every line with a prefix.
type prefixWriter struct {
	dst    io.Writer
	prefix string
	buf    []byte
}

func newPrefixWriter(dst io.Writer, prefix string) *prefixWriter {
	return &prefixWriter{dst: dst, prefix: prefix}
}

func (pw *prefixWriter) Write(p []byte) (int, error) {
	for _, b := range p {
		pw.buf = append(pw.buf, b)
		if b == '\n' {
			line := append([]byte(pw.prefix), pw.buf...)
			_, _ = pw.dst.Write(line)
			pw.buf = pw.buf[:0]
		}
	}
	return len(p), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return strings.EqualFold(v, "true") || v == "1"
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry Point
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	cfg := loadOrchestratorConfig()
	orch := NewOrchestrator(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	// Intercept SIGINT / SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[orchestrator] received signal %s", sig)
		cancel()
	}()

	if err := orch.Start(ctx); err != nil {
		log.Fatalf("[orchestrator] startup failed: %v", err)
	}

	// Block until the context is cancelled (signal received).
	<-ctx.Done()
	orch.Shutdown()
}
