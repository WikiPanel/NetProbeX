package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
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
		if err != nil {
			fatal(err)
		}
		cfg.TCPPorts = ports
	}
	if udpFlag != "" {
		ports, err := config.ParsePorts(udpFlag)
		if err != nil {
			fatal(err)
		}
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
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Target:          cfg.Target,
		DurationSeconds: int64(duration.Seconds()),
		TCPConnect:      map[string]tcp.ConnectResult{},
		TCPStability:    map[string]tcp.StabilityResult{},
		UDP:             map[string]udp.Result{},
	}
	rep.DNS = runDNS(cfg.Target, timeout)
	httpClient := httpx.NewClient(timeout, cfg.AllowInsecureTLS)
	downloadClient := httpx.NewClient(0, cfg.AllowInsecureTLS)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, port := range cfg.TCPPorts {
		port := port
		key := strconv.Itoa(port)
		wg.Add(2)
		go func() {
			defer wg.Done()
			r := tcp.RunConnectLoop(ctx, cfg.Target, port, timeout, interval(cfg.TCPIntervalSeconds, 5))
			mu.Lock()
			rep.TCPConnect[key] = r
			mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			r := tcp.RunStability(ctx, cfg.Target, port, timeout)
			mu.Lock()
			rep.TCPStability[key] = r
			mu.Unlock()
		}()
	}
	for _, port := range cfg.UDPPorts {
		port := port
		key := strconv.Itoa(port)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := udp.RunEcho(ctx, cfg.Target, port, timeout)
			mu.Lock()
			rep.UDP[key] = r
			mu.Unlock()
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
				mu.Lock()
				rep.HTTP = append(rep.HTTP, r)
				mu.Unlock()
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
		mu.Lock()
		rep.WebSocket = r
		mu.Unlock()
	}()
	if cfg.TLSAddress != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := tlsprobe.Run(ctx, cfg.TLSAddress, timeout, cfg.AllowInsecureTLS)
			mu.Lock()
			rep.TLS = r
			mu.Unlock()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for _, p := range cfg.DownloadTests {
				r := download.Run(ctx, downloadClient, cfg.HTTPBaseURL, normalizeDownload(p), timeout)
				mu.Lock()
				rep.Downloads = append(rep.Downloads, r)
				mu.Unlock()
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
	rep.BestCandidate, rep.BestReason, rep.RankedCandidates, rep.Warnings = assess(rep)
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

func assess(rep report.ClientReport) (string, string, []report.TransportScore, []string) {
	var warnings []string
	for port, s := range rep.TCPStability {
		if s.Drops > 0 || s.Failures > 0 {
			warnings = append(warnings, "TCP "+port+" dropped after real traffic started")
		}
	}
	for port, u := range rep.UDP {
		if u.Sent > 0 && u.PacketLossPct > 5 {
			warnings = append(warnings, "UDP "+port+" is unstable")
		}
	}
	if rep.WebSocket.Disconnects > 0 {
		warnings = append(warnings, "WebSocket dropped after real traffic started")
	}
	for _, d := range rep.Downloads {
		if d.Stalled || d.Interrupted || d.SpeedDropAfterBurst {
			warnings = append(warnings, "Download "+d.Path+" showed instability")
		} else if d.Partial {
			warnings = append(warnings, "Download "+d.Path+" completed partially but transferred most expected bytes")
		}
	}

	ranked := []report.TransportScore{
		scoreWebSocket(rep),
		scoreHTTP(rep),
		scoreTLS(rep),
		scoreRawTCP(rep),
		scoreUDP(rep),
	}
	preference := map[string]int{
		"WebSocket": 0,
		"HTTP":      1,
		"TLS":       2,
		"Raw TCP":   3,
		"UDP":       4,
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Score == ranked[j].Score {
			return preference[ranked[i].Transport] < preference[ranked[j].Transport]
		}
		return ranked[i].Score > ranked[j].Score
	})
	for i := range ranked {
		ranked[i].Rank = i + 1
	}
	if len(ranked) == 0 || ranked[0].Score == 0 {
		return "none", "No transport produced stable measured traffic.", ranked, warnings
	}
	return ranked[0].Candidate, ranked[0].Reason, ranked, warnings
}

func scoreWebSocket(rep report.ClientReport) report.TransportScore {
	ws := rep.WebSocket
	score := 0
	reason := "No WebSocket messages were exchanged."
	if ws.Sent > 0 {
		delivery := ratio(ws.Received, ws.Sent)
		score = int(delivery * 80)
		if ws.Received >= int(rep.DurationSeconds/2) {
			score += 15
		} else if ws.Received > 0 {
			score += 8
		}
		if ws.Disconnects == 0 && ws.Failed == 0 {
			score += 5
		}
		score -= ws.Disconnects * 20
		score -= ws.Failed * 10
		score -= ws.Reconnects * 5
		score = clamp(score)
		reason = fmt.Sprintf("WebSocket delivered %d/%d long-running messages with %d disconnects.", ws.Received, ws.Sent, ws.Disconnects)
	}
	return report.TransportScore{
		Transport: "WebSocket",
		Candidate: "WebSocket",
		Score:     score,
		Stable:    score >= 70 && ws.Sent > 0 && ws.Disconnects == 0 && ratio(ws.Received, ws.Sent) >= 0.95,
		Reason:    reason,
	}
}

func scoreHTTP(rep report.ClientReport) report.TransportScore {
	ok, total := 0, len(rep.HTTP)
	for _, h := range rep.HTTP {
		if h.Error == "" && h.StatusCode >= 200 && h.StatusCode < 300 {
			ok++
		}
	}
	pingScore := 0
	if total > 0 {
		pingScore = int(ratio(ok, total) * 40)
	}
	downloadScore := 0
	stableDownloads := 0
	var avgCompletion float64
	if len(rep.Downloads) > 0 {
		for _, d := range rep.Downloads {
			completion := d.CompletionPercent
			if completion == 0 && d.ExpectedBytes == 0 && d.BytesReceived > 0 {
				completion = 100
			}
			if completion > 100 {
				completion = 100
			}
			avgCompletion += completion
			if !d.Interrupted && !d.Stalled && (d.Error == "" || d.Partial) {
				stableDownloads++
			}
		}
		avgCompletion /= float64(len(rep.Downloads))
		downloadScore = int(avgCompletion/100*35) + int(ratio(stableDownloads, len(rep.Downloads))*25)
	}
	score := clamp(pingScore + downloadScore)
	reason := fmt.Sprintf("HTTP passed %d/%d ping/health checks", ok, total)
	if len(rep.Downloads) > 0 {
		reason += fmt.Sprintf(" and had %d/%d stable downloads", stableDownloads, len(rep.Downloads))
	}
	reason += "."
	return report.TransportScore{
		Transport: "HTTP",
		Candidate: "HTTP",
		Score:     score,
		Stable:    score >= 70,
		Reason:    reason,
	}
}

func scoreTLS(rep report.ClientReport) report.TransportScore {
	if !rep.TLS.Success {
		reason := "TLS handshake failed."
		if rep.TLS.Error != "" {
			reason = "TLS handshake failed: " + rep.TLS.Error
		}
		return report.TransportScore{Transport: "TLS", Candidate: "TLS", Score: 0, Stable: false, Reason: reason}
	}
	return report.TransportScore{
		Transport: "TLS",
		Candidate: "TLS " + rep.TLS.Address,
		Score:     62,
		Stable:    false,
		Reason:    "TLS handshake succeeded, but no long-running TLS traffic stability was measured.",
	}
}

func scoreRawTCP(rep report.ClientReport) report.TransportScore {
	best := report.TransportScore{Transport: "Raw TCP", Candidate: "Raw TCP", Score: 0, Reason: "No raw TCP port completed stable echo traffic."}
	for port, c := range rep.TCPConnect {
		s := rep.TCPStability[port]
		score := 0
		if c.Attempts > 0 {
			score += int(ratio(c.Successes, c.Attempts) * 25)
		}
		if s.Sent > 0 {
			score += int(ratio(s.Received, s.Sent) * 55)
			if s.Drops == 0 && s.Failures == 0 {
				score += 15
			}
		}
		score -= s.Drops * 12
		score -= s.Failures * 8
		score -= c.Timeouts * 4
		score -= c.Resets * 4
		score = clamp(score)
		candidate := "Raw TCP " + port
		reason := fmt.Sprintf("Raw TCP %s had %d/%d connect successes and %d/%d echo replies with %d drops.", port, c.Successes, c.Attempts, s.Received, s.Sent, s.Drops)
		if score > best.Score {
			best = report.TransportScore{
				Transport: "Raw TCP",
				Candidate: candidate,
				Score:     score,
				Stable:    score >= 70 && s.Sent > 0 && s.Drops == 0 && s.Failures == 0,
				Reason:    reason,
			}
		}
	}
	return best
}

func scoreUDP(rep report.ClientReport) report.TransportScore {
	best := report.TransportScore{Transport: "UDP", Candidate: "UDP", Score: 0, Reason: "No UDP echo replies were received."}
	for port, u := range rep.UDP {
		score := 0
		if u.Sent > 0 && u.Received > 0 {
			score = int(ratio(u.Received, u.Sent) * 80)
			if u.PacketLossPct <= 1 {
				score += 15
			} else if u.PacketLossPct <= 5 {
				score += 8
			}
			score -= u.Timeouts * 5
		}
		score = clamp(score)
		candidate := "UDP " + port
		reason := fmt.Sprintf("UDP %s received %d/%d packets with %.1f%% loss.", port, u.Received, u.Sent, u.PacketLossPct)
		if score > best.Score {
			best = report.TransportScore{
				Transport: "UDP",
				Candidate: candidate,
				Score:     score,
				Stable:    score >= 70 && u.PacketLossPct <= 5,
				Reason:    reason,
			}
		}
	}
	return best
}

func ratio(good, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(good) / float64(total)
}

func clamp(n int) int {
	if n < 0 {
		return 0
	}
	if n > 100 {
		return 100
	}
	return n
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
