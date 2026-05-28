package tlsprobe

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

type Result struct {
	Address       string        `json:"address"`
	Success       bool          `json:"success"`
	HandshakeTime time.Duration `json:"handshake_time_ns"`
	TLSVersion    string        `json:"tls_version,omitempty"`
	CipherSuite   string        `json:"cipher_suite,omitempty"`
	Error         string        `json:"error,omitempty"`
}

func Run(ctx context.Context, address string, timeout time.Duration, insecure bool) Result {
	res := Result{Address: address}
	host, _, _ := net.SplitHostPort(address)
	dialer := &net.Dialer{Timeout: timeout}
	cfg := &tls.Config{ServerName: host, InsecureSkipVerify: insecure} //nolint:gosec
	start := time.Now()
	conn, err := tls.DialWithDialer(dialer, "tcp", address, cfg)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err := conn.HandshakeContext(ctx); err != nil {
		res.Error = err.Error()
		return res
	}
	state := conn.ConnectionState()
	res.Success = true
	res.HandshakeTime = time.Since(start)
	res.TLSVersion = versionName(state.Version)
	res.CipherSuite = tls.CipherSuiteName(state.CipherSuite)
	return res
}

func versionName(v uint16) string {
	switch v {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("0x%x", v)
	}
}
