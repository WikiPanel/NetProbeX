package httpx

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"netprobex/internal/download"
	"netprobex/internal/logger"
	"netprobex/internal/stats"
	"netprobex/internal/websocketx"
)

type Server struct {
	Port  int
	Log   *logger.Logger
	Stats *stats.ServerStats
}

func (s *Server) Run(ctx context.Context, wg *sync.WaitGroup) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", s.simple("pong"))
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/stats", s.stats)
	mux.HandleFunc("/download/", s.download)
	mux.HandleFunc("/ws", s.ws)
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.Port),
		Handler:           logMiddleware(s.Port, s.Stats, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Log.Event("http_listener_started", map[string]any{"protocol": "http", "port": s.Port})
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.Log.Event("http_server_error", map[string]any{"protocol": "http", "port": s.Port, "error": err.Error()})
		}
	}()
	return nil
}

func (s *Server) simple(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(body + "\n"))
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Stats.Snapshot())
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	size := download.SizeFromPath(r.URL.RequestURI())
	if size == 0 {
		mb, _ := strconv.Atoi(r.URL.Query().Get("size_mb"))
		if mb > 0 {
			size = int64(mb) << 20
		}
	}
	if size <= 0 {
		http.Error(w, "invalid download size", http.StatusBadRequest)
		return
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	ua := r.UserAgent()
	start := time.Now()
	s.Log.Event("download_started", map[string]any{"client_ip": host, "user_agent": ua, "path": r.URL.RequestURI(), "bytes_expected": size})
	written, err := download.Stream(w, r, size)
	dur := time.Since(start)
	failed := err != nil || written < size
	s.Stats.Download(s.Port, failed)
	avg := 0.0
	if dur > 0 {
		avg = float64(written) / dur.Seconds()
	}
	fields := map[string]any{"client_ip": host, "user_agent": ua, "path": r.URL.RequestURI(), "transferred_bytes": written, "duration_ms": dur.Milliseconds(), "average_bytes_per_second": avg}
	if err != nil {
		fields["error"] = err.Error()
		s.Log.Event("download_interrupted", fields)
		return
	}
	s.Log.Event("download_completed", fields)
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	websocketx.Serve(w, r, s.Port, s.Log, s.Stats)
}

func logMiddleware(port int, st *stats.ServerStats, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.HTTPRequest(port)
		next.ServeHTTP(w, r)
	})
}
