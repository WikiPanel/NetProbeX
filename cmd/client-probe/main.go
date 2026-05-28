package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"netprobex/internal/config"
	"netprobex/internal/download"
	"netprobex/internal/httpx"
	"netprobex/internal/report"
	"netprobex/internal/tcp"
	"netprobex/internal/tlsprobe"
	"netprobex/internal/udp"
	"netprobex/internal/websocketx"
)

func main() {
	var configPath, target, tcpFlag, udpFlag, dlFlag string
	var durationFlag time.Duration
	flag.StringVar(&configPath, "config", "", "path to client JSON config")
	flag.StringVar(&target, "target", "", "target host or domain")
	flag.DurationVar(&durationFlag, "duration", 0, "test duration, for example 60s")
	flag.StringVar(&tcpFlag, "tcp", "", "comma-separated TCP ports")
	flag.StringVar(&udpFlag, "udp", "", "comma-separated UDP ports")
	flag.StringVar(&dlFlag, "download", "", "download test name/path, for example 10mb or /download/10mb")
	flag.Parse()

	cfg, err := config.LoadClient(configPath)
	if err != nil {
		fatal(err)
	}
	if target != "" {
		cfg.Target = target
	}
	if durationFlag > 0 {
		cfg.DurationSeconds = int(durationFlag.Seconds())
	}
	if tcpFlag != "" {
		ports, err := config.ParsePorts(tcpFlag)
		if err != nil { fatal(err) }
		cfg.TCPPorts = ports
	}
	if udpFlag != "" {
		ports, err := config.ParsePorts(udpFlag)
		if err != nil { fatal(err) }
		cfg.UDPPorts = ports
	}
	if dlFlag != "" {
		cfg.DownloadTests = []string{normalizeDownload(dlFlag)}
	}
	if cfg.Target == "" {
		fatal(fmt.Errorf("target is required"))
	}
	duration := time.Duration(cfg.DurationSeconds) * time.Second
	if duration <= 0 {
		duration = 60 * time.Second
	}
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if cfg.HTTPBaseURL == "" {
		cfg.HTTPBaseURL = "http://" + cfg.Target + ":8080"
	}
	if cfg.WebSocketURL == "" {
		cfg.WebSocketURL = "ws://" + cfg.Target + ":8081/ws"
	}

	fmt.Printf("NetProbeX client-probe testing %s for %s...\n", cfg.Target, duration)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	rep := report.ClientReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Target: cfg.Target,
		DurationSeconds: int64(duration.Seconds()),
		TCPConnect: map[string]tcp.ConnectResult{},
		TCPStability: map[string]tcp.StabilityResult{},
		UDP: map[string]udp.Result{},
	}
	rep.DNS = runDNS(cfg.Target, timeout)
	httpClient := httpx.NewClient(timeout, cfg.AllowInsecureTLS)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, port := range cfg.TCPPorts {
		port := port
		key := strconv.Itoa(port)
		wg.Add(2)
		go func() {
			defer wg.Done()
			r := tcp.RunConnectLoop(ctx, cfg.Target, port, timeout, interval(cfg.TCPIntervalSeconds, 5))
			mu.Lock(); rep.TCPConnect[key] = r; mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			r := tcp.RunStability(ctx, cfg.Target, port, timeout)
			mu.Lock(); rep.TCPStability[key] = r; mu.Unlock()
		}()
	}
	for _, port := range cfg.UDPPorts {
		port := port
		key := strconv.Itoa(port)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := udp.RunEcho(ctx, cfg.Target, port, timeout)
			mu.Lock(); rep.UDP[key] = r; mu.Unlock()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval(cfg.HTTPIntervalSeconds, 5))
		defer ticker.Stop()
		for {
			for _, p := range []string{"/ping", "/health"} {
				r := httpx.Get(ctx, httpClient, cfg.HTTPBaseURL, p)
				mu.Lock(); rep.HTTP = append(rep.HTTP, r); mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := websocketx.RunStability(ctx, cfg.WebSocketURL, timeout, cfg.AllowInsecureTLS)
		mu.Lock(); rep.WebSocket = r; mu.Unlock()
	}()
	if cfg.TLSAddress != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := tlsprobe.Run(ctx, cfg.TLSAddress, timeout, cfg.AllowInsecureTLS)
			mu.Lock(); rep.TLS = r; mu.Unlock()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for _, p := range cfg.DownloadTests {
				r := download.Run(ctx, httpClient, cfg.HTTPBaseURL, normalizeDownload(p), timeout)
				mu.Lock(); rep.Downloads = append(rep.Downloads, r); mu.Unlock()
			}
			if !cfg.DownloadRepeat {
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	wg.Wait()
	rep.BestCandidate, rep.Warnings = assess(rep)
	if err := report.WriteJSON(cfg.OutputJSON, rep); err != nil {
		fatal(err)
	}
	report.Print(rep)
	fmt.Printf("\nJSON report written to %s\n", cfg.OutputJSON)
}

func runDNS(target string, timeout time.Duration) report.DNSResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	ips, err := net.DefaultResolver.LookupHost(ctx, target)
	res := report.DNSResult{Target: target, ResolveTime: time.Since(start)}
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Success = true
	res.IPs = ips
	return res
}

func assess(rep report.ClientReport) (string, []string) {
	var warnings []string
	best := "none"
	for port, s := range rep.TCPStability {
		c := rep.TCPConnect[port]
		if c.Successes > 0 && s.Received > 0 && s.Drops == 0 && s.Failures == 0 {
			best = "TCP " + port
			break
		}
		if s.Drops > 0 {
			warnings = append(warnings, "TCP "+port+" dropped after real traffic started")
		}
	}
	for port, u := range rep.UDP {
		if u.Sent > 0 && u.PacketLossPct > 5 {
			warnings = append(warnings, "UDP "+port+" is unstable")
		}
	}
	if rep.TLS.Success && (best == "none" || strings.Contains(rep.TLS.Address, ":443")) {
		best = "TCP/TLS " + rep.TLS.Address
	}
	if rep.WebSocket.Disconnects > 0 {
		warnings = append(warnings, "WebSocket dropped after real traffic started")
	}
	for _, d := range rep.Downloads {
		if d.Stalled || d.Interrupted || d.SpeedDropAfterBurst {
			warnings = append(warnings, "Download "+d.Path+" showed instability")
		}
	}
	return best, warnings
}

func normalizeDownload(s string) string {
	if strings.HasPrefix(s, "/") {
		return s
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if strings.HasSuffix(s, "mb") {
		return "/download/" + s
	}
	return s
}

func interval(sec int, def int) time.Duration {
	if sec <= 0 {
		sec = def
	}
	return time.Duration(sec) * time.Second
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "client-probe:", err)
	os.Exit(1)
}
