package download

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Result struct {
	Path                  string        `json:"path"`
	StatusCode            int           `json:"status_code"`
	TimeToFirstByte       time.Duration `json:"time_to_first_byte_ns"`
	TotalDuration         time.Duration `json:"total_duration_ns"`
	BytesReceived         int64         `json:"bytes_received"`
	ExpectedBytes         int64         `json:"expected_bytes"`
	CompletionPercent     float64       `json:"completion_percent"`
	Partial               bool          `json:"partial_completion"`
	AverageBytesPerSecond float64       `json:"average_bytes_per_second"`
	MinBytesPerSecond     float64       `json:"minimum_observed_bytes_per_second"`
	MaxBytesPerSecond     float64       `json:"maximum_observed_bytes_per_second"`
	Stalled               bool          `json:"stalled"`
	Interrupted           bool          `json:"interrupted"`
	SpeedDropAfterBurst   bool          `json:"speed_drop_after_initial_burst"`
	Error                 string        `json:"error,omitempty"`
}

func SizeFromPath(path string) int64 {
	switch {
	case strings.Contains(path, "/download/1mb"):
		return 1 << 20
	case strings.Contains(path, "/download/10mb"):
		return 10 << 20
	case strings.Contains(path, "/download/50mb"):
		return 50 << 20
	case strings.Contains(path, "size_mb="):
		idx := strings.Index(path, "size_mb=")
		raw := path[idx+8:]
		if amp := strings.IndexByte(raw, '&'); amp >= 0 {
			raw = raw[:amp]
		}
		mb, _ := strconv.Atoi(raw)
		if mb > 0 {
			return int64(mb) << 20
		}
	}
	return 0
}

func Stream(w http.ResponseWriter, r *http.Request, size int64) (written int64, err error) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("Cache-Control", "no-store")
	buf := make([]byte, 64*1024)
	rng := rand.New(rand.NewSource(42))
	for written < size {
		n := int64(len(buf))
		if remain := size - written; remain < n {
			n = remain
		}
		for i := int64(0); i < n; i++ {
			buf[i] = byte(rng.Intn(256))
		}
		c, werr := w.Write(buf[:n])
		written += int64(c)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if werr != nil {
			return written, werr
		}
	}
	return written, nil
}

func Run(ctx context.Context, client *http.Client, baseURL, path string, timeout time.Duration) Result {
	res := Result{Path: path, ExpectedBytes: SizeFromPath(path)}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer resp.Body.Close()
	res.StatusCode = resp.StatusCode
	buf := make([]byte, 64*1024)
	var total int64
	var first bool
	windowStart := time.Now()
	var windowBytes int64
	var firstWindow, lastWindow float64
	stallTimer := time.NewTimer(timeout)
	defer stallTimer.Stop()
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if !first {
				first = true
				res.TimeToFirstByte = time.Since(start)
			}
			total += int64(n)
			windowBytes += int64(n)
			if !stallTimer.Stop() {
				select {
				case <-stallTimer.C:
				default:
				}
			}
			stallTimer.Reset(timeout)
		}
		if elapsed := time.Since(windowStart); elapsed >= time.Second {
			speed := float64(windowBytes) / elapsed.Seconds()
			if speed > 0 {
				if res.MinBytesPerSecond == 0 || speed < res.MinBytesPerSecond {
					res.MinBytesPerSecond = speed
				}
				if speed > res.MaxBytesPerSecond {
					res.MaxBytesPerSecond = speed
				}
				if firstWindow == 0 {
					firstWindow = speed
				}
				lastWindow = speed
			}
			windowStart = time.Now()
			windowBytes = 0
		}
		if rerr != nil {
			if rerr != io.EOF {
				res.Error = rerr.Error()
			}
			break
		}
		select {
		case <-stallTimer.C:
			res.Stalled = true
			res.Error = fmt.Sprintf("no bytes received for %s", timeout)
			return finish(res, start, total, firstWindow, lastWindow)
		default:
		}
	}
	return finish(res, start, total, firstWindow, lastWindow)
}

func finish(res Result, start time.Time, total int64, firstWindow, lastWindow float64) Result {
	res.TotalDuration = time.Since(start)
	res.BytesReceived = total
	if res.TotalDuration > 0 {
		res.AverageBytesPerSecond = float64(total) / res.TotalDuration.Seconds()
	}
	if res.ExpectedBytes > 0 {
		res.CompletionPercent = float64(total) / float64(res.ExpectedBytes) * 100
		if total < res.ExpectedBytes {
			res.Partial = true
		}
		if total < int64(float64(res.ExpectedBytes)*0.95) || res.Stalled {
			res.Interrupted = true
		}
	}
	if firstWindow > 0 && lastWindow > 0 && lastWindow < firstWindow*0.5 {
		res.SpeedDropAfterBurst = true
	}
	return res
}
