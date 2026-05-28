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
	rep.BestCandidate, rep.BestCandidateInfo, rep.BestReason, rep.TransportScores, rep.RankedCandidates, rep.Warnings = assess(rep)
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

func assess(rep report.ClientReport) (string, report.BestCandidate, string, map[string]report.TransportScore, []report.TransportScore, []string) {
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

	dnsScore := scoreDNS(rep)
	downloadScore := scoreDownload(rep)
	websocketScore := scoreWebSocket(rep)
	httpScore := scoreHTTP(rep, downloadScore)
	tlsScore := scoreTLS(rep, websocketScore)
	tcpScore := scoreRawTCP(rep)
	udpScore := scoreUDP(rep)
	transportScores := map[string]report.TransportScore{
		"dns":       dnsScore,
		"tcp":       tcpScore,
		"udp":       udpScore,
		"http":      httpScore,
		"websocket": websocketScore,
		"tls":       tlsScore,
		"download":  downloadScore,
	}
	ranked := []report.TransportScore{
		websocketScore,
		httpScore,
		tlsScore,
		tcpScore,
		udpScore,
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
		best := report.BestCandidate{
			Transport: "none",
			Candidate: "none",
			Score:     0,
			Status:    "failed",
			Reason:    "No transport produced stable measured traffic.",
		}
		return "none", best, best.Reason, transportScores, ranked, warnings
	}
	best := report.BestCandidate{
		Transport: transportKey(ranked[0].Transport),
		Candidate: ranked[0].Candidate,
		Score:     ranked[0].Score,
		Status:    ranked[0].Status,
		Reason:    bestReason(ranked[0], ranked[1:], transportScores),
	}
	return ranked[0].Candidate, best, best.Reason, transportScores, ranked, warnings
}

func scoreDNS(rep report.ClientReport) report.TransportScore {
	score := 0
	status := "failed"
	reason := "DNS resolution failed."
	if rep.DNS.Success {
		score = 85
		status = "ok"
		if rep.DNS.ResolveTime <= 100*time.Millisecond {
			score = 100
			status = "stable"
		} else if rep.DNS.ResolveTime <= 500*time.Millisecond {
			score = 92
			status = "stable"
		} else if rep.DNS.ResolveTime > 2*time.Second {
			score = 65
			status = "slow"
		}
		reason = fmt.Sprintf("Resolved %d address(es) in %s.", len(rep.DNS.IPs), shortDuration(rep.DNS.ResolveTime))
	} else if rep.DNS.Error != "" {
		reason = "DNS resolution failed: " + rep.DNS.Error
	}
	return report.TransportScore{
		Transport: "DNS",
		Candidate: "DNS",
		Score:     score,
		Stable:    score >= 80,
		Status:    status,
		Reason:    reason,
	}
}

func scoreWebSocket(rep report.ClientReport) report.TransportScore {
	ws := rep.WebSocket
	score := 0
	status := "failed"
	reason := "No WebSocket messages were exchanged."
	if ws.Sent > 0 {
		delivery := ratio(ws.Received, ws.Sent)
		score = int(delivery * 72)
		expected := int(rep.DurationSeconds)
		if expected < 1 {
			expected = ws.Sent
		}
		durationRatio := ratio(ws.Received, expected)
		if durationRatio > 1 {
			durationRatio = 1
		}
		score += int(durationRatio * 18)
		if latencyStable(ws.AvgLatency, ws.MaxLatency) {
			score += 10
		} else if ws.Received > 0 {
			score += 4
		}
		score -= ws.Disconnects * 22
		score -= ws.Failed * 12
		score -= ws.Reconnects * 7
		score = clamp(score)
		status = statusFor(score)
		reason = fmt.Sprintf("WebSocket exchanged %d/%d messages over %s with %d disconnects and avg latency %s.", ws.Received, ws.Sent, shortDuration(ws.Duration), ws.Disconnects, shortDuration(ws.AvgLatency))
	}
	return report.TransportScore{
		Transport: "WebSocket",
		Candidate: websocketCandidate(ws.URL),
		Score:     score,
		Stable:    score >= 70 && ws.Sent > 0 && ws.Disconnects == 0 && ratio(ws.Received, ws.Sent) >= 0.95,
		Status:    status,
		Reason:    reason,
	}
}

func scoreHTTP(rep report.ClientReport, dl report.TransportScore) report.TransportScore {
	ok, pingOK, pingTotal, healthOK, healthTotal, timeoutCount := 0, 0, 0, 0, 0, 0
	total := len(rep.HTTP)
	var latencies []time.Duration
	for _, h := range rep.HTTP {
		if h.Error == "" && h.StatusCode >= 200 && h.StatusCode < 300 {
			ok++
			latencies = append(latencies, h.Latency)
			if h.Path == "/ping" {
				pingOK++
			}
			if h.Path == "/health" {
				healthOK++
			}
		}
		if h.Path == "/ping" {
			pingTotal++
		}
		if h.Path == "/health" {
			healthTotal++
		}
		if isTimeout(h.Error) {
			timeoutCount++
		}
	}
	requestScore := 0
	if total > 0 {
		requestScore = int(ratio(ok, total) * 52)
	}
	latencyScore := latencyScore(latencies, 18)
	downloadContribution := 0
	if len(rep.Downloads) > 0 {
		downloadContribution = dl.Score * 30 / 100
	}
	score := clamp(requestScore + latencyScore + downloadContribution - timeoutCount*8)
	status := statusFor(score)
	reason := fmt.Sprintf("HTTP passed %d/%d requests (ping %d/%d, health %d/%d)", ok, total, pingOK, pingTotal, healthOK, healthTotal)
	if len(rep.Downloads) > 0 {
		reason += fmt.Sprintf(" and download score contributed %d/30", downloadContribution)
	}
	if timeoutCount > 0 {
		reason += fmt.Sprintf("; %d timeout(s)", timeoutCount)
	}
	reason += "."
	return report.TransportScore{
		Transport: "HTTP",
		Candidate: "HTTP",
		Score:     score,
		Stable:    score >= 70,
		Status:    status,
		Reason:    reason,
	}
}

func scoreTLS(rep report.ClientReport, ws report.TransportScore) report.TransportScore {
	if !rep.TLS.Success {
		reason := "TLS handshake failed."
		if rep.TLS.Error != "" {
			reason = "TLS handshake failed: " + rep.TLS.Error
		}
		return report.TransportScore{Transport: "TLS", Candidate: "TLS handshake", Score: 0, Stable: false, Status: "failed", Reason: reason}
	}
	score := 50
	if rep.TLS.TLSVersion == "TLS 1.3" {
		score += 12
	} else if rep.TLS.TLSVersion == "TLS 1.2" {
		score += 8
	}
	if rep.TLS.HandshakeTime <= 300*time.Millisecond {
		score += 10
	} else if rep.TLS.HandshakeTime <= time.Second {
		score += 6
	} else if rep.TLS.HandshakeTime > 3*time.Second {
		score -= 10
	}
	backedByStableWSS := strings.HasPrefix(strings.ToLower(rep.WebSocket.URL), "wss://") && ws.Stable
	if backedByStableWSS {
		score += 12
	}
	if !backedByStableWSS && score > 74 {
		score = 74
	}
	score = clamp(score)
	reason := fmt.Sprintf("TLS handshake succeeded with %s/%s in %s; handshake alone does not prove sustained traffic stability.", rep.TLS.TLSVersion, rep.TLS.CipherSuite, shortDuration(rep.TLS.HandshakeTime))
	return report.TransportScore{
		Transport: "TLS",
		Candidate: "TLS handshake " + rep.TLS.Address,
		Score:     score,
		Stable:    score >= 80 && backedByStableWSS,
		Status:    statusFor(score),
		Reason:    reason,
	}
}

func scoreRawTCP(rep report.ClientReport) report.TransportScore {
	best := report.TransportScore{Transport: "Raw TCP", Candidate: "Raw TCP", Score: 0, Status: "failed", Reason: "No raw TCP port completed stable echo traffic."}
	for port, c := range rep.TCPConnect {
		s := rep.TCPStability[port]
		score := 0
		if c.Attempts > 0 {
			score += int(ratio(c.Successes, c.Attempts) * 25)
		}
		if s.Sent > 0 {
			score += int(ratio(s.Received, s.Sent) * 50)
			if s.Drops == 0 && s.Failures == 0 {
				score += 14
			}
			if latencyStable(s.AvgLatency, s.MaxLatency) {
				score += 8
			}
		}
		score -= s.Drops * 14
		score -= s.Reconnects * 8
		score -= s.LatencySpikes * 5
		score -= s.Failures * 8
		score -= c.Timeouts * 4
		score -= c.Resets * 4
		score = clamp(score)
		candidate := "Raw TCP " + port
		reason := fmt.Sprintf("Raw TCP %s had %d/%d connect successes, %d/%d echo replies, %d drops, %d reconnects, and %d latency spikes.", port, c.Successes, c.Attempts, s.Received, s.Sent, s.Drops, s.Reconnects, s.LatencySpikes)
		if score > best.Score {
			best = report.TransportScore{
				Transport: "Raw TCP",
				Candidate: candidate,
				Score:     score,
				Stable:    score >= 70 && s.Sent > 0 && s.Drops == 0 && s.Failures == 0,
				Status:    statusFor(score),
				Reason:    reason,
			}
		}
	}
	return best
}

func scoreUDP(rep report.ClientReport) report.TransportScore {
	best := report.TransportScore{Transport: "UDP", Candidate: "UDP", Score: 0, Status: "failed", Reason: "No UDP echo replies were received."}
	for port, u := range rep.UDP {
		score := 0
		if u.Sent > 0 && u.Received > 0 {
			score = int(ratio(u.Received, u.Sent) * 82)
			if u.PacketLossPct <= 1 {
				score += 15
			} else if u.PacketLossPct <= 5 {
				score += 8
			}
			if latencyStable(u.AvgLatency, u.MaxLatency) {
				score += 3
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
				Status:    statusFor(score),
				Reason:    reason,
			}
		}
	}
	return best
}

func scoreDownload(rep report.ClientReport) report.TransportScore {
	if len(rep.Downloads) == 0 {
		return report.TransportScore{
			Transport: "Download",
			Candidate: "Download",
			Score:     0,
			Status:    "not_tested",
			Reason:    "No download tests were configured.",
		}
	}
	completed, partial, interrupted, stalled, speedDrops := 0, 0, 0, 0, 0
	var avgCompletion float64
	for _, d := range rep.Downloads {
		completion := d.CompletionPercent
		if completion == 0 && d.ExpectedBytes == 0 && d.BytesReceived > 0 {
			completion = 100
		}
		if completion > 100 {
			completion = 100
		}
		avgCompletion += completion
		if completion >= 100 && d.Error == "" && !d.Stalled {
			completed++
		}
		if d.Partial {
			partial++
		}
		if d.Interrupted {
			interrupted++
		}
		if d.Stalled {
			stalled++
		}
		if d.SpeedDropAfterBurst {
			speedDrops++
		}
	}
	avgCompletion /= float64(len(rep.Downloads))
	score := int(avgCompletion * 0.7)
	score += int(ratio(completed, len(rep.Downloads)) * 25)
	score -= interrupted * 8
	score -= stalled * 12
	score -= speedDrops * 5
	score = clamp(score)
	status := statusFor(score)
	if partial > 0 && score >= 50 && completed == 0 {
		status = "partial"
	}
	reason := fmt.Sprintf("Downloads completed %d/%d, partial %d/%d, average completion %.1f%%.", completed, len(rep.Downloads), partial, len(rep.Downloads), avgCompletion)
	if interrupted > 0 {
		reason += fmt.Sprintf(" %d interrupted.", interrupted)
	}
	return report.TransportScore{
		Transport: "Download",
		Candidate: "Download",
		Score:     score,
		Stable:    score >= 75 && interrupted == 0 && stalled == 0,
		Status:    status,
		Reason:    reason,
	}
}

func latencyScore(latencies []time.Duration, maxPoints int) int {
	if len(latencies) == 0 {
		return 0
	}
	var total time.Duration
	max := latencies[0]
	for _, d := range latencies {
		total += d
		if d > max {
			max = d
		}
	}
	avg := total / time.Duration(len(latencies))
	if latencyStable(avg, max) {
		return maxPoints
	}
	if max <= 3*time.Second {
		return maxPoints / 2
	}
	return maxPoints / 4
}

func latencyStable(avg, max time.Duration) bool {
	if avg <= 0 || max <= 0 {
		return false
	}
	return max <= 2*avg || max <= 750*time.Millisecond
}

func statusFor(score int) string {
	switch {
	case score >= 85:
		return "stable"
	case score >= 70:
		return "mostly_stable"
	case score >= 40:
		return "unstable"
	case score > 0:
		return "poor"
	default:
		return "failed"
	}
}

func bestReason(best report.TransportScore, others []report.TransportScore, scores map[string]report.TransportScore) string {
	reason := best.Reason
	if best.Transport == "WebSocket" {
		tcpScore := scores["tcp"]
		udpScore := scores["udp"]
		reason += fmt.Sprintf(" It ranked above raw TCP (%d/100) and UDP (%d/100) because sustained message exchange is weighted higher than initial connection success.", tcpScore.Score, udpScore.Score)
	}
	for _, other := range others {
		if other.Score > 0 && best.Score-other.Score <= 5 {
			reason += fmt.Sprintf(" It narrowly beat %s (%d/100).", other.Candidate, other.Score)
			break
		}
	}
	return reason
}

func transportKey(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, " ", "_"))
}

func websocketCandidate(raw string) string {
	lower := strings.ToLower(raw)
	label := "WebSocket"
	if strings.HasPrefix(lower, "wss://") {
		label = "WebSocket over TLS"
	}
	host := strings.TrimPrefix(strings.TrimPrefix(lower, "wss://"), "ws://")
	if slash := strings.IndexByte(host, '/'); slash >= 0 {
		host = host[:slash]
	}
	if colon := strings.LastIndexByte(host, ':'); colon >= 0 && colon < len(host)-1 {
		return label + "/" + host[colon+1:]
	}
	return label
}

func isTimeout(err string) bool {
	err = strings.ToLower(err)
	return strings.Contains(err, "timeout") || strings.Contains(err, "deadline")
}

func shortDuration(d time.Duration) string {
	if d <= 0 {
		return "n/a"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Truncate(time.Millisecond).String()
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
