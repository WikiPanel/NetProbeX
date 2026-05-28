package httpx

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"
)

type Result struct {
	Path            string        `json:"path"`
	StatusCode      int           `json:"status_code"`
	Latency         time.Duration `json:"latency_ns"`
	TimeToFirstByte time.Duration `json:"time_to_first_byte_ns"`
	Error           string        `json:"error,omitempty"`
}

func NewClient(timeout time.Duration, insecure bool) *http.Client {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure}} //nolint:gosec
	return &http.Client{Timeout: timeout, Transport: tr}
}

func Get(ctx context.Context, client *http.Client, baseURL, path string) Result {
	res := Result{Path: path}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, trim(baseURL)+path, nil)
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
	res.TimeToFirstByte = time.Since(start)
	res.StatusCode = resp.StatusCode
	buf := make([]byte, 1)
	_, _ = resp.Body.Read(buf)
	res.Latency = time.Since(start)
	return res
}

func trim(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
