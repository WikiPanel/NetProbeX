package report

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"netprobex/internal/download"
	"netprobex/internal/httpx"
	"netprobex/internal/tcp"
	"netprobex/internal/tlsprobe"
	"netprobex/internal/udp"
	"netprobex/internal/websocketx"
)

type DNSResult struct {
	Target      string        `json:"target"`
	Success     bool          `json:"success"`
	IPs         []string      `json:"ips"`
	ResolveTime time.Duration `json:"resolve_time_ns"`
	Error       string        `json:"error,omitempty"`
}

type ClientReport struct {
	GeneratedAt       string                         `json:"generated_at"`
	Target            string                         `json:"target"`
	DurationSeconds   int64                          `json:"duration_seconds"`
	DNS               DNSResult                      `json:"dns"`
	TCPConnect        map[string]tcp.ConnectResult   `json:"tcp_connect"`
	TCPStability      map[string]tcp.StabilityResult `json:"tcp_stability"`
	UDP               map[string]udp.Result          `json:"udp"`
	HTTP              []httpx.Result                 `json:"http"`
	WebSocket         websocketx.Result              `json:"websocket"`
	TLS               tlsprobe.Result                `json:"tls"`
	Downloads         []download.Result              `json:"downloads"`
	BestCandidate     string                         `json:"best_candidate"`
	Warnings          []string                       `json:"warnings"`
}

func WriteJSON(path string, rep ClientReport) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

func Print(rep ClientReport) {
	fmt.Println("NetProbeX Client Report")
	fmt.Println()
	fmt.Printf("Target: %s\n", rep.Target)
	fmt.Printf("Duration: %ds\n\n", rep.DurationSeconds)
	fmt.Println("DNS:")
	if rep.DNS.Success {
		fmt.Printf("Status: OK\nResolve time: %s\nIPs: %s\n\n", fmtDur(rep.DNS.ResolveTime), strings.Join(rep.DNS.IPs, ", "))
	} else {
		fmt.Printf("Status: FAILED, %s\n\n", rep.DNS.Error)
	}
	fmt.Println("TCP:")
	for _, k := range sortedKeys(rep.TCPConnect) {
		c := rep.TCPConnect[k]
		s := rep.TCPStability[k]
		status := "FAILED"
		if c.Successes > 0 && s.Drops == 0 && s.Failures == 0 {
			status = "OK"
		} else if c.Successes > 0 {
			status = "OK but unstable"
		}
		fmt.Printf("%s: %s, avg latency %s, drops %d", k, status, fmtDur(c.AvgLatency), s.Drops)
		if c.LastError != "" && c.Successes == 0 {
			fmt.Printf(", %s", c.LastError)
		}
		fmt.Println()
	}
	fmt.Println()
	fmt.Println("UDP:")
	for _, k := range sortedKeys(rep.UDP) {
		u := rep.UDP[k]
		status := "OK"
		if u.Received == 0 {
			status = "failed"
		} else if u.PacketLossPct > 5 {
			status = "unstable"
		}
		fmt.Printf("%s: packet loss %.1f%%, %s, avg latency %s\n", k, u.PacketLossPct, status, fmtDur(u.AvgLatency))
	}
	fmt.Println()
	fmt.Println("HTTP:")
	for _, h := range rep.HTTP {
		status := "FAILED"
		if h.StatusCode >= 200 && h.StatusCode < 300 {
			status = "OK"
		}
		fmt.Printf("%s: %s, status %d, latency %s, TTFB %s", h.Path, status, h.StatusCode, fmtDur(h.Latency), fmtDur(h.TimeToFirstByte))
		if h.Error != "" {
			fmt.Printf(", %s", h.Error)
		}
		fmt.Println()
	}
	fmt.Println()
	fmt.Println("WebSocket:")
	if rep.WebSocket.Received > 0 && rep.WebSocket.Disconnects == 0 {
		fmt.Printf("connected, stable, messages %d, avg latency %s\n\n", rep.WebSocket.Received, fmtDur(rep.WebSocket.AvgLatency))
	} else {
		fmt.Printf("unstable, disconnects %d, reconnects %d, error %s\n\n", rep.WebSocket.Disconnects, rep.WebSocket.Reconnects, rep.WebSocket.Error)
	}
	fmt.Println("TLS:")
	if rep.TLS.Success {
		fmt.Printf("OK, handshake %s, %s, %s\n\n", fmtDur(rep.TLS.HandshakeTime), rep.TLS.TLSVersion, rep.TLS.CipherSuite)
	} else if rep.TLS.Address != "" {
		fmt.Printf("FAILED, %s\n\n", rep.TLS.Error)
	}
	fmt.Println("Download:")
	for _, d := range rep.Downloads {
		state := "completed"
		if d.Interrupted || d.Stalled || d.Error != "" {
			state = "unstable/interrupted"
		}
		fmt.Printf("%s: %s, %.2f MB, avg %s, TTFB %s, interrupted: %v\n", d.Path, state, float64(d.BytesReceived)/(1<<20), fmtSpeed(d.AverageBytesPerSecond), fmtDur(d.TimeToFirstByte), d.Interrupted)
	}
	fmt.Println()
	fmt.Printf("Best candidate: %s\n\n", rep.BestCandidate)
	if len(rep.Warnings) > 0 {
		fmt.Println("Warnings:")
		for _, w := range rep.Warnings {
			fmt.Println("- " + w)
		}
	}
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "n/a"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Truncate(time.Millisecond).String()
}

func fmtSpeed(bps float64) string {
	if bps <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f MB/s", bps/(1<<20))
}
