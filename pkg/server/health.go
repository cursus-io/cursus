package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/cursus-io/cursus/util"
)

type readinessCheck func(context.Context) error

type healthResult struct {
	checks  map[string]string
	version uint64
}

// HealthState publishes dependency observations independently of HTTP requests.
// Start and Close are owned by the broker/diagnostics server lifecycle.
type HealthState struct {
	ready   atomic.Bool
	mu      sync.RWMutex // registration only; never held while invoking a check
	checks  map[string]readinessCheck
	version uint64
	worker  *observation.Worker[healthResult]
}

func NewHealthState() *HealthState {
	return &HealthState{checks: make(map[string]readinessCheck), worker: observation.New[healthResult](observation.Interval, observation.Timeout, observation.MaxAge)}
}

func (state *HealthState) Start() { state.worker.Start(state.sample) }
func (state *HealthState) Close() { state.SetReady(false); state.worker.Close() }

func (state *HealthState) SetReady(ready bool) {
	if state != nil {
		state.ready.Store(ready)
	}
}

func (state *HealthState) AddCheck(name string, check readinessCheck) {
	if state == nil || strings.TrimSpace(name) == "" || check == nil {
		return
	}
	state.mu.Lock()
	state.checks[name] = check
	state.version++ // An earlier result cannot certify a newly registered check.
	state.mu.Unlock()
}

func (state *HealthState) ObservationStatus() observation.Status {
	snapshot := state.worker.Snapshot()
	state.mu.RLock()
	version := state.version
	state.mu.RUnlock()
	if snapshot.Value.version != version {
		snapshot.Stale = true
	}
	return snapshot.Status
}

func (state *HealthState) IsReady() bool {
	ready, _ := state.report()
	return ready
}

func (state *HealthState) report() (bool, map[string]string) {
	if state == nil || !state.ready.Load() {
		return false, map[string]string{"broker": "starting"}
	}
	snapshot := state.worker.Snapshot()
	state.mu.RLock()
	version := state.version
	state.mu.RUnlock()
	if snapshot.Value.version != version || snapshot.CompletedAt.IsZero() {
		return false, map[string]string{"observation": "not yet checked"}
	}
	if snapshot.Stopped {
		return false, map[string]string{"observation": "stopped"}
	}
	if snapshot.TimedOut {
		return false, map[string]string{"observation": "check deadline exceeded"}
	}
	if snapshot.Stale {
		return false, map[string]string{"observation": "last completed check expired"}
	}
	checks := make(map[string]string, len(snapshot.Value.checks)+1)
	for name, result := range snapshot.Value.checks {
		checks[name] = result
	}
	if snapshot.Error != "" {
		checks["observation"] = snapshot.Error
	}
	return snapshot.Valid(), checks
}

func (state *HealthState) sample(ctx context.Context) (healthResult, error) {
	state.mu.RLock()
	names := make([]string, 0, len(state.checks))
	checks := make(map[string]readinessCheck, len(state.checks))
	for name, check := range state.checks {
		names = append(names, name)
		checks[name] = check
	}
	version := state.version
	state.mu.RUnlock()
	sort.Strings(names)
	result := healthResult{checks: make(map[string]string, len(names)), version: version}
	var failure error
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(failure, err)
		}
		if err := checks[name](ctx); err != nil {
			result.checks[name] = err.Error()
			failure = errors.Join(failure, fmt.Errorf("%s: %w", name, err))
		} else {
			result.checks[name] = "ok"
		}
	}
	return result, failure
}

type healthResponse struct {
	Status      string              `json:"status"`
	Checks      map[string]string   `json:"checks,omitempty"`
	Observation *observation.Status `json:"observation,omitempty"`
}

func newHealthHandler(state *HealthState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		if !allowHealthMethod(w, r) {
			return
		}
		writeHealthJSON(w, http.StatusOK, healthResponse{Status: "live"})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !allowHealthMethod(w, r) {
			return
		}
		ready, checks := state.report()
		status := http.StatusOK
		label := "ready"
		if !ready {
			status = http.StatusServiceUnavailable
			label = "not_ready"
		}
		var observed *observation.Status
		if state != nil {
			status := state.ObservationStatus()
			observed = &status
		}
		writeHealthJSON(w, status, healthResponse{Status: label, Checks: checks, Observation: observed})
	})
	return mux
}

func allowHealthMethod(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeHealthJSON(w http.ResponseWriter, status int, response healthResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		util.Error("health check JSON write error: %v", err)
	}
}

// startHealthCheckServer binds synchronously so startup cannot report ready on bind failure.
func startHealthCheckServer(port int, state *HealthState) (*http.Server, error) {
	return startHealthCheckServerAddress(fmt.Sprintf(":%d", port), state)
}

func startHealthCheckServerAddress(addr string, state *HealthState) (*http.Server, error) {
	server := &http.Server{
		Addr:              addr,
		Handler:           newHealthHandler(state),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for health checks on %s: %w", addr, err)
	}
	server.Addr = listener.Addr().String()
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			util.Error("health check server stopped: %v", serveErr)
		}
	}()
	util.Info("Health check endpoint started on %s", listener.Addr())
	return server, nil
}

func shutdownHTTPServer(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		util.Warn("HTTP server shutdown failed: %v", err)
	}
}
