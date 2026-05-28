package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"netprobex/internal/config"
	"netprobex/internal/httpx"
	"netprobex/internal/logger"
	"netprobex/internal/stats"
	"netprobex/internal/tcp"
	"netprobex/internal/tlsprobe"
	"netprobex/internal/udp"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to server JSON config")
	flag.Parse()

	cfg, err := config.LoadServer(configPath)
	if err != nil {
		fatal(err)
	}
	log, err := logger.New(cfg.LogFile)
	if err != nil {
		fatal(err)
	}
	defer log.Close()
	st := stats.NewServerStats()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup

	tcpPorts := cfg.TCPPorts
	if cfg.TLSPort > 0 {
		tcpPorts = withoutPort(cfg.TCPPorts, cfg.TLSPort)
		if len(tcpPorts) != len(cfg.TCPPorts) {
			log.Event("tcp_port_reserved_for_tls", map[string]any{"protocol": "tls", "port": cfg.TLSPort})
		}
	}
	log.Event("server_starting", map[string]any{"tcp_ports": tcpPorts, "udp_ports": cfg.UDPPorts, "http_port": cfg.HTTPPort, "websocket_port": cfg.WebSocketPort, "tls_port": cfg.TLSPort})
	if err := (&tcp.Server{Ports: tcpPorts, Log: log, Stats: st}).Run(ctx, &wg); err != nil {
		fatal(err)
	}
	if err := (&udp.Server{Ports: cfg.UDPPorts, Log: log, Stats: st}).Run(ctx, &wg); err != nil {
		fatal(err)
	}
	if cfg.HTTPPort > 0 {
		if err := (&httpx.Server{Port: cfg.HTTPPort, Log: log, Stats: st}).Run(ctx, &wg); err != nil {
			fatal(err)
		}
	}
	if cfg.WebSocketPort > 0 && cfg.WebSocketPort != cfg.HTTPPort {
		if err := (&httpx.Server{Port: cfg.WebSocketPort, Log: log, Stats: st}).Run(ctx, &wg); err != nil {
			fatal(err)
		}
	}
	if cfg.TLSPort > 0 {
		if err := (&tlsprobe.Server{Port: cfg.TLSPort, CertFile: cfg.TLSCertFile, KeyFile: cfg.TLSKeyFile, Log: log, Stats: st}).Run(ctx, &wg); err != nil {
			log.Event("tls_listener_skipped", map[string]any{"protocol": "tls", "port": cfg.TLSPort, "error": err.Error()})
		}
	}
	fmt.Println("NetProbeX server-agent running. Press Ctrl+C to stop.")
	<-ctx.Done()
	log.Event("server_stopping", nil)
	wg.Wait()
	log.Event("server_stopped", nil)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "server-agent:", err)
	os.Exit(1)
}

func withoutPort(ports []int, skip int) []int {
	out := make([]int, 0, len(ports))
	for _, p := range ports {
		if p != skip {
			out = append(out, p)
		}
	}
	return out
}
