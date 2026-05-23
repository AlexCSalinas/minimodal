package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// HTTPServer is the orchestrator's observability surface — /metrics for JSON
// telemetry, /healthz for liveness, and / for the dashboard (resolved from
// MINIMODAL_DASHBOARD_PATH env or `../dashboard` relative to the binary).
type HTTPServer struct {
	srv *http.Server
	mux *http.ServeMux
	app *Server
}

func NewHTTPServer(app *Server, port int) *HTTPServer {
	h := &HTTPServer{app: app, mux: http.NewServeMux()}
	h.mux.HandleFunc("/metrics", h.handleMetrics)
	h.mux.HandleFunc("/healthz", h.handleHealthz)

	// Opt-in pprof endpoints for production debugging. Off by default because
	// /debug/pprof/* exposes goroutine stacks + heap profile + CPU traces —
	// useful for an operator, but not safe to leave open on a public-facing
	// orchestrator. Set MINIMODAL_PPROF=1 to enable.
	if os.Getenv("MINIMODAL_PPROF") == "1" {
		slog.Info("pprof endpoints enabled", "prefix", "/debug/pprof/")
		h.mux.HandleFunc("/debug/pprof/", pprof.Index)
		h.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		h.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		h.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		h.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	if dashPath := resolveDashboardPath(); dashPath != "" {
		slog.Info("dashboard mounted", "path", dashPath)
		h.mux.Handle("/", http.FileServer(http.Dir(dashPath)))
	} else {
		slog.Warn("dashboard not found; set MINIMODAL_DASHBOARD_PATH to enable")
		h.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("minimodal orchestrator — GET /metrics for JSON\n"))
		})
	}

	h.srv = &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           h.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return h
}

func (h *HTTPServer) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		slog.Info("http server listening", "addr", h.srv.Addr)
		err := h.srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		} else {
			errCh <- nil
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return h.srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// CORS so the dashboard can be served from anywhere (e.g. file:// or
	// a separate static-server during dev).
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	snap := h.app.MetricsSnapshot()
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *HTTPServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if err := h.app.jobStore.Ping(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unhealthy: " + err.Error() + "\n"))
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

// resolveDashboardPath looks for the dashboard directory in (1) the
// MINIMODAL_DASHBOARD_PATH env var, (2) ../dashboard relative to the binary,
// (3) ./dashboard relative to cwd. Returns "" if none have index.html.
func resolveDashboardPath() string {
	candidates := []string{}
	if env := os.Getenv("MINIMODAL_DASHBOARD_PATH"); env != "" {
		candidates = append(candidates, env)
	}
	if exe, err := os.Executable(); err == nil {
		base := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(base, "..", "dashboard"),
			filepath.Join(base, "dashboard"),
		)
	}
	candidates = append(candidates, "dashboard", "./dashboard")
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Join(c, "index.html")); err == nil {
			abs, _ := filepath.Abs(c)
			return abs
		}
	}
	return ""
}
