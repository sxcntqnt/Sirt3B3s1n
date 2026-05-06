// Matatu Pulse — GPS Pipeline Orchestrator
//
// Manages mqtt-consumer, batch-writer, and websocket-gateway as supervised
// subprocesses. Provides auto-scaling, health monitoring, circuit-breaking,
// and a control-plane HTTP API. Designed for 100 000 concurrent vehicles
// (~20 000 GPS events/sec at the default 5-second update interval).
//
// Startup dependency order (enforced by BootstrapGate):
//   batch-writer → mqtt-consumer pool → websocket-gateway
//
// Shutdown order (reverse dependency, enforced by Orchestrator.Shutdown):
//   websocket-gateway → mqtt-consumer pool → batch-writer (extra drain time)
//
// Auto-scaling signal:
//   Reads batch_writer_redis_stream_lag from each batch-writer /metrics
//   endpoint. Lag is smoothed with an EWMA (α=0.3) before scaling decisions
//   to prevent single-sample thrash.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration — Viper-backed, atomic Holder, 12-factor layering
// ─────────────────────────────────────────────────────────────────────────────

// OrchestratorConfig holds all tuneable parameters.
// Precedence (highest → lowest): ENV → config file → coded defaults.
type OrchestratorConfig struct {
	BinaryDir           string
	UseGoRun            bool
	ControlPlaneAddr    string
	ScaleInterval       time.Duration
	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
	UnhealthyThreshold  int
	RestartBackoffMax   time.Duration

	// Bootstrap tuning
	BootstrapPollInterval time.Duration // how often to probe during startup
	BootstrapTimeout      time.Duration // per-pool deadline before fatal

	// Auto-scaler thresholds (in Redis stream messages)
	ScaleUpLag   int64
	ScaleDownLag int64

	// EWMA smoothing factor for lag samples (0 < α ≤ 1).
	// Lower = smoother but slower to react; 0.3 is a good default.
	LagEWMAAlpha float64

	Services ServicesConfig
}

type ServicesConfig struct {
	MQTTConsumer     ServiceConfig
	BatchWriter      ServiceConfig
	WebSocketGateway ServiceConfig
}

type ServiceConfig struct {
	Binary          string
	MinReplicas     int
	MaxReplicas     int
	BaseMetricsPort int
	BaseHealthPort  int
	Env             map[string]string
}

// ─── config Holder — immutable snapshot + atomic swap, no mutexes ───────────

type ConfigHolder struct {
	val atomic.Value // stores *OrchestratorConfig
}

func NewConfigHolder(cfg *OrchestratorConfig) *ConfigHolder {
	h := &ConfigHolder{}
	h.val.Store(cfg)
	return h
}

func (h *ConfigHolder) Get() *OrchestratorConfig {
	return h.val.Load().(*OrchestratorConfig)
}

func (h *ConfigHolder) Set(cfg *OrchestratorConfig) {
	h.val.Store(cfg)
}

// ─── Viper loader ────────────────────────────────────────────────────────────

func loadConfig() (*OrchestratorConfig, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix("ORCHESTRATOR")

	// Optional config file — silently skipped when absent.
	v.SetConfigName("orchestrator")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/matatu-pulse")
	_ = v.ReadInConfig()

	// ── defaults ────────────────────────────────────────────────────────────
	v.SetDefault("binary_dir", ".")
	v.SetDefault("use_go_run", false)
	v.SetDefault("control_plane_addr", ":7070")
	v.SetDefault("scale_interval", "15s")
	v.SetDefault("health_check_interval", "10s")
	v.SetDefault("health_check_timeout", "3s")
	v.SetDefault("unhealthy_threshold", 3)
	v.SetDefault("restart_backoff_max", "60s")
	v.SetDefault("bootstrap_poll_interval", "2s")
	v.SetDefault("bootstrap_timeout", "60s")
	v.SetDefault("scale_up_lag", 50000)
	v.SetDefault("scale_down_lag", 5000)
	v.SetDefault("lag_ewma_alpha", 0.3)

	// services — mqtt-consumer
	v.SetDefault("services.mqtt_consumer.binary", "mqtt-consumer")
	v.SetDefault("services.mqtt_consumer.min_replicas", 4)
	v.SetDefault("services.mqtt_consumer.max_replicas", 32)
	v.SetDefault("services.mqtt_consumer.base_metrics_port", 9100)
	v.SetDefault("services.mqtt_consumer.base_health_port", 8100)

	// services — batch-writer
	v.SetDefault("services.batch_writer.binary", "batch-writer")
	v.SetDefault("services.batch_writer.min_replicas", 2)
	v.SetDefault("services.batch_writer.max_replicas", 8)
	v.SetDefault("services.batch_writer.base_metrics_port", 9200)
	v.SetDefault("services.batch_writer.base_health_port", 8200)

	// services — websocket-gateway
	v.SetDefault("services.websocket_gateway.binary", "websocket-gateway")
	v.SetDefault("services.websocket_gateway.min_replicas", 2)
	v.SetDefault("services.websocket_gateway.max_replicas", 10)
	v.SetDefault("services.websocket_gateway.base_metrics_port", 9300)
	v.SetDefault("services.websocket_gateway.base_health_port", 8300)

	cfg := &OrchestratorConfig{
		BinaryDir:             v.GetString("binary_dir"),
		UseGoRun:              v.GetBool("use_go_run"),
		ControlPlaneAddr:      v.GetString("control_plane_addr"),
		ScaleInterval:         v.GetDuration("scale_interval"),
		HealthCheckInterval:   v.GetDuration("health_check_interval"),
		HealthCheckTimeout:    v.GetDuration("health_check_timeout"),
		UnhealthyThreshold:    v.GetInt("unhealthy_threshold"),
		RestartBackoffMax:     v.GetDuration("restart_backoff_max"),
		BootstrapPollInterval: v.GetDuration("bootstrap_poll_interval"),
		BootstrapTimeout:      v.GetDuration("bootstrap_timeout"),
		ScaleUpLag:            v.GetInt64("scale_up_lag"),
		ScaleDownLag:          v.GetInt64("scale_down_lag"),
		LagEWMAAlpha:          v.GetFloat64("lag_ewma_alpha"),
		Services: ServicesConfig{
			MQTTConsumer: ServiceConfig{
				Binary:          v.GetString("services.mqtt_consumer.binary"),
				MinReplicas:     v.GetInt("services.mqtt_consumer.min_replicas"),
				MaxReplicas:     v.GetInt("services.mqtt_consumer.max_replicas"),
				BaseMetricsPort: v.GetInt("services.mqtt_consumer.base_metrics_port"),
				BaseHealthPort:  v.GetInt("services.mqtt_consumer.base_health_port"),
				Env: map[string]string{
					"MQTT_BROKER":     "${MQTT_BROKER}",
					"MQTT_USERNAME":   "${MQTT_USERNAME}",
					"MQTT_PASSWORD":   "${MQTT_PASSWORD}",
					"REDIS_PASSWORD":  "${REDIS_PASSWORD}",
					"JAEGER_ENDPOINT": "${JAEGER_ENDPOINT}",
				},
			},
			BatchWriter: ServiceConfig{
				Binary:          v.GetString("services.batch_writer.binary"),
				MinReplicas:     v.GetInt("services.batch_writer.min_replicas"),
				MaxReplicas:     v.GetInt("services.batch_writer.max_replicas"),
				BaseMetricsPort: v.GetInt("services.batch_writer.base_metrics_port"),
				BaseHealthPort:  v.GetInt("services.batch_writer.base_health_port"),
				Env: map[string]string{
					"REDIS_PASSWORD":      "${REDIS_PASSWORD}",
					"CLICKHOUSE_HOST":     "${CLICKHOUSE_HOST}",
					"CLICKHOUSE_USERNAME": "${CLICKHOUSE_USERNAME}",
					"CLICKHOUSE_PASSWORD": "${CLICKHOUSE_PASSWORD}",
					"JAEGER_ENDPOINT":     "${JAEGER_ENDPOINT}",
				},
			},
			WebSocketGateway: ServiceConfig{
				Binary:          v.GetString("services.websocket_gateway.binary"),
				MinReplicas:     v.GetInt("services.websocket_gateway.min_replicas"),
				MaxReplicas:     v.GetInt("services.websocket_gateway.max_replicas"),
				BaseMetricsPort: v.GetInt("services.websocket_gateway.base_metrics_port"),
				BaseHealthPort:  v.GetInt("services.websocket_gateway.base_health_port"),
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

	// ── validation ───────────────────────────────────────────────────────────
	if cfg.LagEWMAAlpha <= 0 || cfg.LagEWMAAlpha > 1 {
		return nil, fmt.Errorf("lag_ewma_alpha must be in (0, 1]; got %f", cfg.LagEWMAAlpha)
	}
	if cfg.ScaleUpLag <= cfg.ScaleDownLag {
		return nil, fmt.Errorf("scale_up_lag (%d) must be greater than scale_down_lag (%d)",
			cfg.ScaleUpLag, cfg.ScaleDownLag)
	}
	for _, s := range []struct {
		name string
		sc   ServiceConfig
	}{
		{"mqtt_consumer", cfg.Services.MQTTConsumer},
		{"batch_writer", cfg.Services.BatchWriter},
		{"websocket_gateway", cfg.Services.WebSocketGateway},
	} {
		if s.sc.MinReplicas < 1 {
			return nil, fmt.Errorf("services.%s.min_replicas must be >= 1", s.name)
		}
		if s.sc.MaxReplicas < s.sc.MinReplicas {
			return nil, fmt.Errorf("services.%s.max_replicas must be >= min_replicas", s.name)
		}
	}

	return cfg, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Bootstrap Gate — deterministic startup state machine
// ─────────────────────────────────────────────────────────────────────────────

// bootstrapPhase tracks one pool through its startup lifecycle.
type bootstrapPhase int32

const (
	bpPending bootstrapPhase = iota // pool Start() not yet called
	bpProbing                       // probing health endpoint
	bpReady                         // at least one replica is healthy
	bpFailed                        // timeout or crash before ready
)

func (p bootstrapPhase) String() string {
	return [...]string{"pending", "probing", "ready", "failed"}[p]
}

// BootstrapGate enforces ordered, deterministic pool startup.
// Each pool transitions: Pending → Probing → Ready (or Failed).
// Failure in any pool halts the gate and returns an error — the orchestrator
// treats this as fatal because a partially-started pipeline is worse than none.
type BootstrapGate struct {
	cfg    *OrchestratorConfig
	client *http.Client
}

func NewBootstrapGate(cfg *OrchestratorConfig) *BootstrapGate {
	return &BootstrapGate{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HealthCheckTimeout},
	}
}

// WaitReady blocks until at least one instance in pool is healthy, or until
// ctx is cancelled / the bootstrap timeout elapses.
//
// It distinguishes three outcomes:
//   - ready (healthy instance found)
//   - context cancelled (orchestrator is shutting down)
//   - timeout (pool never became healthy → fatal)
func (bg *BootstrapGate) WaitReady(ctx context.Context, pool *ServicePool) error {
	deadline := time.Now().Add(bg.cfg.BootstrapTimeout)
	ticker := time.NewTicker(bg.cfg.BootstrapPollInterval)
	defer ticker.Stop()

	log.Printf("[bootstrap] probing pool %s (timeout %s)", pool.name, bg.cfg.BootstrapTimeout)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("pool %s: context cancelled during bootstrap", pool.name)

		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("pool %s: did not become ready within %s",
					pool.name, bg.cfg.BootstrapTimeout)
			}

			pool.checkHealth(bg.client)
			h, total := pool.HealthySummary()

			log.Printf("[bootstrap] pool %s — healthy: %d/%d", pool.name, h, total)

			if h > 0 {
				log.Printf("[bootstrap] pool %s is READY ✓", pool.name)
				return nil
			}

			// Detect early: if every instance is in StateStopped, the
			// processes crashed immediately — no point waiting for timeout.
			if bg.allStopped(pool) {
				return fmt.Errorf("pool %s: all replicas exited before becoming healthy", pool.name)
			}
		}
	}
}

func (bg *BootstrapGate) allStopped(pool *ServicePool) bool {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	if len(pool.instances) == 0 {
		return false
	}
	for _, inst := range pool.instances {
		if inst.State() != StateStopped {
			return false
		}
	}
	return true
}

// ─────────────────────────────────────────────────────────────────────────────
// Process Instance
// ─────────────────────────────────────────────────────────────────────────────

type InstanceState int32

const (
	StateStarting  InstanceState = iota
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

	mu             sync.RWMutex
	cmd            *exec.Cmd
	state          InstanceState
	startedAt      time.Time
	unhealthyCount int
	restartCount   int
	lastRestartAt  time.Time

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

type ServicePool struct {
	name    string
	cfg     ServiceConfig
	orchCfg *OrchestratorConfig

	mu        sync.RWMutex
	instances []*ProcessInstance

	metrics *PoolMetrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewServicePool(
	ctx context.Context,
	name string,
	cfg ServiceConfig,
	orchCfg *OrchestratorConfig,
	metrics *PoolMetrics,
) *ServicePool {
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

// Stop gracefully terminates all instances.
// It sends SIGTERM and waits up to timeout before SIGKILL.
func (sp *ServicePool) Stop(timeout time.Duration) {
	log.Printf("[orchestrator] stopping pool %s (timeout %s)", sp.name, timeout)
	sp.cancel()

	sp.mu.RLock()
	instances := make([]*ProcessInstance, len(sp.instances))
	copy(instances, sp.instances)
	sp.mu.RUnlock()

	for _, inst := range instances {
		inst.mu.Lock()
		if inst.cmd != nil && inst.cmd.Process != nil {
			_ = inst.cmd.Process.Signal(syscall.SIGTERM)
		}
		inst.mu.Unlock()
	}

	done := make(chan struct{})
	go func() {
		sp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[orchestrator] pool %s stopped cleanly", sp.name)
	case <-time.After(timeout):
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

// ScaleTo adjusts the pool to exactly n replicas (clamped to Min/Max).
func (sp *ServicePool) ScaleTo(n int) {
	n = clamp(n, sp.cfg.MinReplicas, sp.cfg.MaxReplicas)

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
	} else {
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
	}
	sp.metrics.ReplicaCount.WithLabelValues(sp.name).Set(float64(n))
}

func (sp *ServicePool) ReplicaCount() int {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return len(sp.instances)
}

func (sp *ServicePool) HealthySummary() (healthy, total int) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
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
	env = append(env,
		fmt.Sprintf("METRICS_PORT=%d", inst.MetricsPort),
		fmt.Sprintf("HEALTH_PORT=%d", inst.HealthPort),
		fmt.Sprintf("INSTANCE_ID=%s", inst.ID),
		fmt.Sprintf("REPLICA=%d", inst.Replica),
	)
	for k, v := range sp.cfg.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, os.ExpandEnv(v)))
	}

	var cmd *exec.Cmd
	if sp.orchCfg.UseGoRun {
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

// watchProcess waits for a process to exit and restarts it with exponential
// backoff + jitter. Stops when ctx is cancelled (pool shutdown or scale-down).
func (sp *ServicePool) watchProcess(ctx context.Context, inst *ProcessInstance) {
	defer sp.wg.Done()

	backoff := time.Second

	for {
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
		count := inst.restartCount
		inst.mu.Unlock()

		log.Printf("[orchestrator] %s exited unexpectedly — restart #%d in %s",
			inst.ID, count, backoff)
		sp.metrics.RestartTotal.WithLabelValues(sp.name).Inc()

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Exponential backoff with ±25% jitter.
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > sp.orchCfg.RestartBackoffMax {
			backoff = sp.orchCfg.RestartBackoffMax
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 4)))
		if rand.Intn(2) == 0 {
			backoff += jitter
		} else {
			backoff -= jitter
		}

		if err := sp.spawnProcess(ctx, inst); err != nil {
			log.Printf("[orchestrator] respawn %s failed: %v", inst.ID, err)
		}
	}
}

func (sp *ServicePool) supervisionLoop() {
	defer sp.wg.Done()

	ticker := time.NewTicker(sp.orchCfg.HealthCheckInterval)
	defer ticker.Stop()

	client := &http.Client{Timeout: sp.orchCfg.HealthCheckTimeout}

	for {
		select {
		case <-sp.ctx.Done():
			return
		case <-ticker.C:
			sp.checkHealth(client)
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

	h, total := sp.HealthySummary()
	sp.metrics.ReplicaCount.WithLabelValues(sp.name).Set(float64(total))
	log.Printf("[orchestrator] pool %s health: %d/%d healthy", sp.name, h, total)
}

// ─────────────────────────────────────────────────────────────────────────────
// Auto-Scaler — EWMA-smoothed lag, hysteresis, proportional WS scaling
// ─────────────────────────────────────────────────────────────────────────────

// AutoScaler reads Redis stream lag from the batch-writer pool's /metrics
// endpoints, smooths it with an EWMA, and adjusts the MQTT consumer pool.
//
// EWMA prevents single-sample spikes from causing scale thrash.
// Hysteresis (separate up/down thresholds) prevents oscillation at the boundary.
type AutoScaler struct {
	mqttPool  *ServicePool
	batchPool *ServicePool
	wsPool    *ServicePool
	cfgHolder *ConfigHolder
	metrics   *ScalerMetrics

	httpClient *http.Client

	// EWMA state — only touched inside evaluate(), no lock needed.
	ewmaLag      float64
	ewmaInitDone bool
}

func NewAutoScaler(
	mqttPool, batchPool, wsPool *ServicePool,
	cfgHolder *ConfigHolder,
	metrics *ScalerMetrics,
) *AutoScaler {
	return &AutoScaler{
		mqttPool:   mqttPool,
		batchPool:  batchPool,
		wsPool:     wsPool,
		cfgHolder:  cfgHolder,
		metrics:    metrics,
		httpClient: &http.Client{Timeout: 3 * time.Second},
	}
}

func (as *AutoScaler) Run(ctx context.Context) {
	cfg := as.cfgHolder.Get()
	ticker := time.NewTicker(cfg.ScaleInterval)
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
	cfg := as.cfgHolder.Get()
	raw := as.sampleStreamLag()

	// EWMA: initialise to first sample, then smooth subsequent samples.
	if !as.ewmaInitDone {
		as.ewmaLag = float64(raw)
		as.ewmaInitDone = true
	} else {
		as.ewmaLag = cfg.LagEWMAAlpha*float64(raw) + (1-cfg.LagEWMAAlpha)*as.ewmaLag
	}

	smoothed := int64(as.ewmaLag)
	as.metrics.StreamLag.Set(float64(smoothed))
	as.metrics.RawStreamLag.Set(float64(raw))
	log.Printf("[autoscaler] lag raw=%d smoothed=%d", raw, smoothed)

	currentMQTT := as.mqttPool.ReplicaCount()

	switch {
	case smoothed > cfg.ScaleUpLag:
		target := currentMQTT + 2
		log.Printf("[autoscaler] smoothed lag %d > %d — scaling mqtt-consumer %d → %d",
			smoothed, cfg.ScaleUpLag, currentMQTT, target)
		as.mqttPool.ScaleTo(target)

	case smoothed < cfg.ScaleDownLag && currentMQTT > cfg.Services.MQTTConsumer.MinReplicas:
		target := currentMQTT - 1
		log.Printf("[autoscaler] smoothed lag %d < %d — scaling mqtt-consumer %d → %d",
			smoothed, cfg.ScaleDownLag, currentMQTT, target)
		as.mqttPool.ScaleTo(target)
	}

	// Scale WebSocket gateways proportionally: 1 WS gateway per 3 MQTT consumers.
	currentMQTT = as.mqttPool.ReplicaCount()
	targetWS := clamp(
		currentMQTT/3+1,
		cfg.Services.WebSocketGateway.MinReplicas,
		cfg.Services.WebSocketGateway.MaxReplicas,
	)
	as.wsPool.ScaleTo(targetWS)
}

// sampleStreamLag scrapes all batch-writer /metrics endpoints and returns the
// maximum observed batch_writer_redis_stream_lag value.
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
	mux.Handle("/metrics", promhttp.Handler())

	cp.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
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

func (cp *ControlPlane) handleHealth(w http.ResponseWriter, _ *http.Request) {
	type poolHealth struct {
		Healthy int `json:"healthy"`
		Total   int `json:"total"`
	}
	type response struct {
		Status string                `json:"status"`
		Pools  map[string]poolHealth `json:"pools"`
	}

	mqttH, mqttT := cp.mqtt.HealthySummary()
	batchH, batchT := cp.batch.HealthySummary()
	wsH, wsT := cp.ws.HealthySummary()

	allOK := mqttH > 0 && batchH > 0 && wsH > 0
	status := "ok"
	code := http.StatusOK
	if !allOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	resp := response{
		Status: status,
		Pools: map[string]poolHealth{
			"mqtt-consumer":     {mqttH, mqttT},
			"batch-writer":      {batchH, batchT},
			"websocket-gateway": {wsH, wsT},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

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
		Replicas  int              `json:"instances"`
		Instances []instanceStatus `json:"instances_detail"`
	}
	type response struct {
		OrchestratorUptimeSeconds int64        `json:"orchestrator_uptime_seconds"`
		Pools                     []poolStatus `json:"pools"`
		StreamLagSmoothed         int64        `json:"stream_lag_smoothed_messages"`
		StreamLagRaw              int64        `json:"stream_lag_raw_messages"`
	}

	makePool := func(pool *ServicePool) poolStatus {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		ps := poolStatus{Name: pool.name, Replicas: len(pool.instances)}
		for _, inst := range pool.instances {
			inst.mu.RLock()
			ps.Instances = append(ps.Instances, instanceStatus{
				ID:            inst.ID,
				State:         inst.state.String(),
				MetricsPort:   inst.MetricsPort,
				Restarts:      inst.restartCount,
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
		StreamLagSmoothed: int64(cp.scaler.ewmaLag),
		StreamLagRaw:      cp.scaler.sampleStreamLag(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

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

	pools := map[string]*ServicePool{
		"mqtt-consumer":     cp.mqtt,
		"batch-writer":      cp.batch,
		"websocket-gateway": cp.ws,
	}
	pool, ok := pools[svc]
	if !ok {
		http.Error(w, "unknown service; valid: mqtt-consumer, batch-writer, websocket-gateway", http.StatusBadRequest)
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
	ReplicaCount *prometheus.GaugeVec
	HealthStatus *prometheus.GaugeVec
	RestartTotal *prometheus.CounterVec
	ReplicaState *prometheus.GaugeVec
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
			Help: "Total process restarts per pool",
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
	StreamLag    prometheus.Gauge
	RawStreamLag prometheus.Gauge
}

func NewScalerMetrics() *ScalerMetrics {
	smoothed := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "orchestrator_stream_lag_smoothed_messages",
		Help: "EWMA-smoothed Redis batch stream lag observed by the auto-scaler",
	})
	raw := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "orchestrator_stream_lag_raw_messages",
		Help: "Raw (unsmoothed) Redis batch stream lag from the latest sample",
	})
	prometheus.MustRegister(smoothed, raw)
	return &ScalerMetrics{StreamLag: smoothed, RawStreamLag: raw}
}

// ─────────────────────────────────────────────────────────────────────────────
// Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

type Orchestrator struct {
	cfgHolder *ConfigHolder
	metrics   *PoolMetrics

	mqtt  *ServicePool
	batch *ServicePool
	ws    *ServicePool

	scaler       *AutoScaler
	controlPlane *ControlPlane
	gate         *BootstrapGate

	startedAt time.Time
}

func NewOrchestrator(cfgHolder *ConfigHolder) *Orchestrator {
	return &Orchestrator{
		cfgHolder: cfgHolder,
		metrics:   NewPoolMetrics(),
		startedAt: time.Now(),
	}
}

// Start brings up the full pipeline with deterministic dependency gating:
//
//  1. Batch Writer  → WaitReady (must be accepting Redis streams before consumers start)
//  2. MQTT Consumer pool → WaitReady
//  3. WebSocket Gateway pool → WaitReady
//  4. Auto-scaler
//  5. Control plane
//
// If any pool fails to become ready, Start returns an error and the caller
// must invoke Shutdown to clean up whatever did start.
func (o *Orchestrator) Start(ctx context.Context) error {
	cfg := o.cfgHolder.Get()

	log.Println("[orchestrator] ═══════════════════════════════════════════")
	log.Println("[orchestrator]  Matatu Pulse — GPS Pipeline Orchestrator")
	log.Printf("[orchestrator]  Target: 100 000 vehicles / ~20 000 events/sec")
	log.Println("[orchestrator] ═══════════════════════════════════════════")

	o.gate = NewBootstrapGate(cfg)

	o.batch = NewServicePool(ctx, "batch-writer", cfg.Services.BatchWriter, cfg, o.metrics)
	o.mqtt = NewServicePool(ctx, "mqtt-consumer", cfg.Services.MQTTConsumer, cfg, o.metrics)
	o.ws = NewServicePool(ctx, "websocket-gateway", cfg.Services.WebSocketGateway, cfg, o.metrics)

	// ── 1. Batch Writer ──────────────────────────────────────────────────────
	// Must be ready before consumers send events — otherwise the Redis
	// batch stream fills unboundedly with no consumer.
	if err := o.batch.Start(); err != nil {
		return fmt.Errorf("batch-writer pool start: %w", err)
	}
	if err := o.gate.WaitReady(ctx, o.batch); err != nil {
		return fmt.Errorf("batch-writer pool bootstrap: %w", err)
	}

	// ── 2. MQTT Consumers ────────────────────────────────────────────────────
	if err := o.mqtt.Start(); err != nil {
		return fmt.Errorf("mqtt-consumer pool start: %w", err)
	}
	if err := o.gate.WaitReady(ctx, o.mqtt); err != nil {
		return fmt.Errorf("mqtt-consumer pool bootstrap: %w", err)
	}

	// ── 3. WebSocket Gateway ─────────────────────────────────────────────────
	if err := o.ws.Start(); err != nil {
		return fmt.Errorf("websocket-gateway pool start: %w", err)
	}
	if err := o.gate.WaitReady(ctx, o.ws); err != nil {
		return fmt.Errorf("websocket-gateway pool bootstrap: %w", err)
	}

	// ── 4. Auto-scaler ───────────────────────────────────────────────────────
	scalerMetrics := NewScalerMetrics()
	o.scaler = NewAutoScaler(o.mqtt, o.batch, o.ws, o.cfgHolder, scalerMetrics)
	go o.scaler.Run(ctx)

	// ── 5. Control plane ─────────────────────────────────────────────────────
	o.controlPlane = NewControlPlane(cfg.ControlPlaneAddr, o.mqtt, o.batch, o.ws, o.scaler)
	o.controlPlane.Start()

	log.Println("[orchestrator] all pools ready")
	o.printStartupSummary()
	return nil
}

// Shutdown tears down the pipeline in reverse dependency order.
// Batch Writer gets extra drain time so in-flight Redis stream messages can
// be flushed to ClickHouse before the process exits.
func (o *Orchestrator) Shutdown() {
	log.Println("[orchestrator] initiating graceful shutdown...")

	const (
		poolTimeout       = 30 * time.Second
		batchDrainTimeout = 60 * time.Second // extra time for ClickHouse flush
	)

	// Reverse startup order: WS → MQTT → Batch.
	if o.ws != nil {
		o.ws.Stop(poolTimeout)
	}
	if o.mqtt != nil {
		o.mqtt.Stop(poolTimeout)
	}
	if o.batch != nil {
		log.Printf("[orchestrator] giving batch-writer extra drain time (%s)", batchDrainTimeout)
		o.batch.Stop(batchDrainTimeout)
	}
	if o.controlPlane != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		o.controlPlane.Shutdown(ctx)
	}

	log.Println("[orchestrator] shutdown complete")
}

func (o *Orchestrator) printStartupSummary() {
	cfg := o.cfgHolder.Get()
	log.Println("[orchestrator] ───────────────────────────────────────────")
	log.Printf("[orchestrator]  mqtt-consumer  : %d replicas (max %d)",
		o.mqtt.ReplicaCount(), cfg.Services.MQTTConsumer.MaxReplicas)
	log.Printf("[orchestrator]  batch-writer   : %d replicas (max %d)",
		o.batch.ReplicaCount(), cfg.Services.BatchWriter.MaxReplicas)
	log.Printf("[orchestrator]  websocket-gw   : %d replicas (max %d)",
		o.ws.ReplicaCount(), cfg.Services.WebSocketGateway.MaxReplicas)
	log.Printf("[orchestrator]  control-plane  : %s", cfg.ControlPlaneAddr)
	log.Printf("[orchestrator]  scale-interval : %s", cfg.ScaleInterval)
	log.Printf("[orchestrator]  ewma-alpha     : %.2f", cfg.LagEWMAAlpha)
	log.Println("[orchestrator] ───────────────────────────────────────────")
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

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

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry Point
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[orchestrator] config error: %v", err)
	}

	cfgHolder := NewConfigHolder(cfg)
	orch := NewOrchestrator(cfgHolder)

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[orchestrator] received signal %s — initiating shutdown", sig)
		cancel()
	}()

	if err := orch.Start(ctx); err != nil {
		log.Printf("[orchestrator] startup failed: %v", err)
		orch.Shutdown() // clean up whatever did start
		os.Exit(1)
	}

	<-ctx.Done()
	orch.Shutdown()
}
