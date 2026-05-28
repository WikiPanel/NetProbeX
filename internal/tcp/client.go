package tcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type ConnectResult struct {
	Port       int           `json:"port"`
	Attempts   int           `json:"attempts"`
	Successes  int           `json:"successes"`
	Timeouts   int           `json:"timeouts"`
	Refused    int           `json:"refused"`
	Resets     int           `json:"resets"`
	Errors     int           `json:"errors"`
	AvgLatency time.Duration `json:"avg_latency_ns"`
	MinLatency time.Duration `json:"min_latency_ns"`
	MaxLatency time.Duration `json:"max_latency_ns"`
	LastError  string        `json:"last_error,omitempty"`
}

type StabilityResult struct {
	Port          int           `json:"port"`
	Duration      time.Duration `json:"duration_ns"`
	Sent          int           `json:"sent"`
	Received      int           `json:"received"`
	Drops         int           `json:"drops"`
	Reconnects    int           `json:"reconnects"`
	Failures      int           `json:"failures"`
	AvgLatency    time.Duration `json:"avg_latency_ns"`
	MinLatency    time.Duration `json:"min_latency_ns"`
	MaxLatency    time.Duration `json:"max_latency_ns"`
	LatencySpikes int           `json:"latency_spikes"`
	LastError     string        `json:"last_error,omitempty"`
}

func RunConnectLoop(ctx context.Context, target string, port int, timeout, interval time.Duration) ConnectResult {
	res := ConnectResult{Port: port}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		attemptConnect(target, port, timeout, &res)
		select {
		case <-ctx.Done():
			return res
		case <-ticker.C:
		}
	}
}

func attemptConnect(target string, port int, timeout time.Duration, res *ConnectResult) {
	res.Attempts++
	start := time.Now()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", target, port), timeout)
	lat := time.Since(start)
	if err != nil {
		classifyTCPError(err, res)
		return
	}
	_ = conn.Close()
	res.Successes++
	addLatency(&res.AvgLatency, &res.MinLatency, &res.MaxLatency, lat, res.Successes)
}

func RunStability(ctx context.Context, target string, port int, timeout time.Duration) StabilityResult {
	res := StabilityResult{Port: port}
	payloadSeq := 0
	var conn net.Conn
	connect := func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", target, port), timeout)
		if err != nil {
			res.Failures++
			res.LastError = err.Error()
			return false
		}
		conn = c
		return true
	}
	if !connect() {
		return res
	}
	defer conn.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	started := time.Now()
	for {
		select {
		case <-ctx.Done():
			res.Duration = time.Since(started)
			return res
		case <-ticker.C:
			payloadSeq++
			payload := []byte(fmt.Sprintf("netprobex-tcp-%d-%d", port, payloadSeq))
			begin := time.Now()
			_ = conn.SetDeadline(time.Now().Add(timeout))
			if _, err := conn.Write(payload); err != nil {
				res.Drops++
				res.Failures++
				res.LastError = err.Error()
				_ = conn.Close()
				if connect() {
					res.Reconnects++
				}
				continue
			}
			res.Sent++
			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != string(payload) {
				res.Drops++
				res.Failures++
				if err != nil {
					res.LastError = err.Error()
				} else {
					res.LastError = "echo mismatch"
				}
				_ = conn.Close()
				if connect() {
					res.Reconnects++
				}
				continue
			}
			lat := time.Since(begin)
			res.Received++
			if lat > 2*time.Second {
				res.LatencySpikes++
			}
			addLatency(&res.AvgLatency, &res.MinLatency, &res.MaxLatency, lat, res.Received)
		}
	}
}

func classifyTCPError(err error, res *ConnectResult) {
	res.LastError = err.Error()
	res.Errors++
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		res.Timeouts++
	case strings.Contains(msg, "refused"):
		res.Refused++
	case strings.Contains(msg, "reset"):
		res.Resets++
	}
}

func addLatency(avg, min, max *time.Duration, lat time.Duration, count int) {
	if count == 1 || lat < *min {
		*min = lat
	}
	if lat > *max {
		*max = lat
	}
	*avg = time.Duration((int64(*avg)*int64(count-1) + int64(lat)) / int64(count))
}
