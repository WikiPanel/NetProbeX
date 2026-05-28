package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type ServerConfig struct {
	TCPPorts          []int  `json:"tcp_ports"`
	UDPPorts          []int  `json:"udp_ports"`
	HTTPPort          int    `json:"http_port"`
	WebSocketPort     int    `json:"websocket_port"`
	TLSPort           int    `json:"tls_port"`
	TLSCertFile       string `json:"tls_cert_file"`
	TLSKeyFile        string `json:"tls_key_file"`
	LogFile           string `json:"log_file"`
	ReadTimeoutSec    int    `json:"read_timeout_seconds"`
	WriteTimeoutSec   int    `json:"write_timeout_seconds"`
	ShutdownTimeoutSec int   `json:"shutdown_timeout_seconds"`
}

type ClientConfig struct {
	Target             string   `json:"target"`
	DurationSeconds    int      `json:"duration_seconds"`
	TimeoutSeconds     int      `json:"timeout_seconds"`
	TCPPorts           []int    `json:"tcp_ports"`
	UDPPorts           []int    `json:"udp_ports"`
	HTTPBaseURL        string   `json:"http_base_url"`
	WebSocketURL       string   `json:"websocket_url"`
	TLSAddress         string   `json:"tls_address"`
	DownloadTests      []string `json:"download_tests"`
	OutputJSON         string   `json:"output_json"`
	AllowInsecureTLS   bool     `json:"allow_insecure_tls"`
	HTTPIntervalSeconds int     `json:"http_interval_seconds"`
	TCPIntervalSeconds  int     `json:"tcp_interval_seconds"`
	DownloadRepeat     bool     `json:"download_repeat"`
}

func DefaultServer() ServerConfig {
	return ServerConfig{
		TCPPorts:           []int{443, 8443, 2053, 2083, 2096},
		UDPPorts:           []int{443, 8443, 2053},
		HTTPPort:           8080,
		WebSocketPort:      8081,
		TLSPort:            8443,
		LogFile:            "/var/log/netprobex/server.jsonl",
		ReadTimeoutSec:     10,
		WriteTimeoutSec:    10,
		ShutdownTimeoutSec: 10,
	}
}

func DefaultClient() ClientConfig {
	return ClientConfig{
		DurationSeconds:     60,
		TimeoutSeconds:      5,
		TCPPorts:            []int{443, 8443, 2053, 2083, 2096},
		UDPPorts:            []int{443, 8443, 2053},
		DownloadTests:       []string{"/download/1mb", "/download/10mb"},
		OutputJSON:          "netprobex-report.json",
		AllowInsecureTLS:    true,
		HTTPIntervalSeconds: 5,
		TCPIntervalSeconds:  5,
	}
}

func LoadServer(path string) (ServerConfig, error) {
	cfg := DefaultServer()
	if path == "" {
		return cfg, nil
	}
	if err := readJSON(path, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func LoadClient(path string) (ClientConfig, error) {
	cfg := DefaultClient()
	if path == "" {
		return cfg, nil
	}
	if err := readJSON(path, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	return nil
}

func ParsePorts(s string) ([]int, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	ports := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("invalid port %q", p)
		}
		ports = append(ports, n)
	}
	return ports, nil
}

func ParseDurationFlag(fs *flag.FlagSet, name string, def time.Duration, usage string) *time.Duration {
	v := def
	fs.Func(name, usage, func(s string) error {
		d, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		v = d
		return nil
	})
	return &v
}
