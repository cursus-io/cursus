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

	"github.com/cursus-io/cursus/util"
)

const healthCheckTimeout = 2 * time.Second

type readinessCheck func(context.Context) error

// HealthState tracks startup readiness and dynamic dependency checks.
type HealthState struct {
	ready  atomic.Bool
	mu     sync.RWMutex
	checks map[string]readinessCheck
}

func NewHealthState() *HealthState {
	return &HealthState{checks: make(map[string]readinessCheck)}
}

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
	state.mu.Unlock()
}

// IsReady reports startup state plus all current dependency checks.
func (state *HealthState) IsReady() bool {
	ready, _ := state.report()
	return ready
}

func (state *HealthState) report() (bool, map[string]string) {
	if state == nil || !state.ready.Load() {
		return false, map[string]string{"broker": "starting"}
	}

	state.mu.RLock()
	names := make([]string, 0, len(state.checks))
	checks := make(map[string]readinessCheck, len(state.checks))
	for name, check := range state.checks {
		names = append(names, name)
		checks[name] = check
	}
	state.mu.RUnlock()
	sort.Strings(names)

	result := make(map[string]string, len(names))
	ready := true
	for _, name := range names {
		ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
		err := checks[name](ctx)
		cancel()
		if err != nil {
			ready = false
			result[name] = err.Error()
			continue
		}
		result[name] = "ok"
	}
	return ready, result
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
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
		writeHealthJSON(w, status, healthResponse{Status: label, Checks: checks})
	})
	compatibilityHandler := func(w http.ResponseWriter, r *http.Request) {
		if !allowHealthMethod(w, r) {
			return
		}
		ready, checks := state.report()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := fmt.Fprintf(w, "Broker not ready: %s", failedChecks(checks)); err != nil {
				util.Error("health check response write error: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			util.Error("health check response write error: %v", err)
		}
	}
	mux.HandleFunc("/health", compatibilityHandler)
	mux.HandleFunc("/", compatibilityHandler)
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

func failedChecks(checks map[string]string) string {
	names := make([]string, 0, len(checks))
	for name, result := range checks {
		if result != "ok" {
			names = append(names, name+"="+result)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "unknown"
	}
	return strings.Join(names, ",")
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
