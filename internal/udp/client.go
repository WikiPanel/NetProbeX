package udp

import (
	"context"
	"fmt"
	"net"
	"time"
)

type Result struct {
	Port          int           `json:"port"`
	Sent          int           `json:"sent_packets"`
	Received      int           `json:"received_packets"`
	PacketLossPct float64       `json:"packet_loss_percent"`
	Timeouts      int           `json:"timeout_count"`
	AvgLatency    time.Duration `json:"avg_latency_ns"`
	MinLatency    time.Duration `json:"min_latency_ns"`
	MaxLatency    time.Duration `json:"max_latency_ns"`
	LastError     string        `json:"last_error,omitempty"`
}

func RunEcho(ctx context.Context, target string, port int, timeout time.Duration) Result {
	res := Result{Port: port}
	addr := fmt.Sprintf("%s:%d", target, port)
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		res.LastError = err.Error()
		return res
	}
	defer conn.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	seq := 0
	buf := make([]byte, 2048)
	for {
		select {
		case <-ctx.Done():
			if res.Sent > 0 {
				res.PacketLossPct = float64(res.Sent-res.Received) / float64(res.Sent) * 100
			}
			return res
		case <-ticker.C:
			seq++
			payload := []byte(fmt.Sprintf("netprobex-udp-%d-%d", port, seq))
			start := time.Now()
			_ = conn.SetDeadline(time.Now().Add(timeout))
			if _, err := conn.Write(payload); err != nil {
				res.LastError = err.Error()
				continue
			}
			res.Sent++
			n, err := conn.Read(buf)
			if err != nil {
				res.Timeouts++
				res.LastError = err.Error()
				continue
			}
			if string(buf[:n]) != string(payload) {
				res.LastError = "echo mismatch"
				continue
			}
			res.Received++
			addLatency(&res.AvgLatency, &res.MinLatency, &res.MaxLatency, time.Since(start), res.Received)
		}
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
