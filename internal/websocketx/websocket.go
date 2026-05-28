package websocketx

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"netprobex/internal/logger"
	"netprobex/internal/stats"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Result struct {
	URL         string        `json:"url"`
	Duration    time.Duration `json:"duration_ns"`
	Sent        int           `json:"sent_messages"`
	Received    int           `json:"successful_messages"`
	Failed      int           `json:"failed_messages"`
	Disconnects int           `json:"disconnects"`
	Reconnects  int           `json:"reconnects"`
	AvgLatency  time.Duration `json:"avg_latency_ns"`
	MinLatency  time.Duration `json:"min_latency_ns"`
	MaxLatency  time.Duration `json:"max_latency_ns"`
	CloseReason string        `json:"close_reason,omitempty"`
	Error       string        `json:"error,omitempty"`
}

type Conn struct {
	c      net.Conn
	reader *bufio.Reader
	server bool
}

func Serve(w http.ResponseWriter, r *http.Request, port int, log *logger.Logger, st *stats.ServerStats) {
	if strings.ToLower(r.Header.Get("Upgrade")) != "websocket" {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing websocket key", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	netc, rw, err := hj.Hijack()
	if err != nil {
		return
	}
	accept := acceptKey(key)
	_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	_, _ = rw.WriteString("Upgrade: websocket\r\n")
	_, _ = rw.WriteString("Connection: Upgrade\r\n")
	_, _ = rw.WriteString("Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
	_ = rw.Flush()
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	st.WSConnect(port)
	log.Event("websocket_open", map[string]any{"client_ip": host, "protocol": "websocket", "port": port})
	defer func() {
		st.WSDisconnect(port)
		log.Event("websocket_close", map[string]any{"client_ip": host, "protocol": "websocket", "port": port})
		_ = netc.Close()
	}()
	ws := &Conn{c: netc, reader: rw.Reader, server: true}
	var in, out int
	for {
		op, payload, err := ws.ReadFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Event("websocket_error", map[string]any{"client_ip": host, "protocol": "websocket", "port": port, "error": err.Error(), "messages_in": in, "messages_out": out})
			}
			return
		}
		in++
		switch op {
		case 0x8:
			_ = ws.WriteFrame(0x8, payload)
			return
		case 0x9:
			_ = ws.WriteFrame(0xA, payload)
			out++
		case 0x1, 0x2:
			_ = ws.WriteFrame(op, payload)
			out++
		}
	}
}

func Dial(ctx context.Context, raw string, timeout time.Duration, insecure bool) (*Conn, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}
	d := net.Dialer{Timeout: timeout}
	var c net.Conn
	if u.Scheme == "wss" {
		c, err = tls.DialWithDialer(&d, "tcp", host, &tls.Config{ServerName: u.Hostname(), InsecureSkipVerify: insecure}) //nolint:gosec
	} else {
		c, err = d.DialContext(ctx, "tcp", host)
	}
	if err != nil {
		return nil, err
	}
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, u.Host, key)
	_ = c.SetDeadline(time.Now().Add(timeout))
	if _, err := io.WriteString(c, req); err != nil {
		_ = c.Close()
		return nil, err
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = c.Close()
		return nil, fmt.Errorf("websocket upgrade failed: %s", resp.Status)
	}
	_ = c.SetDeadline(time.Time{})
	return &Conn{c: c, reader: br}, nil
}

func RunStability(ctx context.Context, raw string, timeout time.Duration, insecure bool) Result {
	res := Result{URL: raw}
	started := time.Now()
	var ws *Conn
	connect := func() bool {
		c, err := Dial(ctx, raw, timeout, insecure)
		if err != nil {
			res.Error = err.Error()
			res.Failed++
			return false
		}
		ws = c
		return true
	}
	if !connect() {
		return res
	}
	defer ws.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	seq := 0
	for {
		select {
		case <-ctx.Done():
			res.Duration = time.Since(started)
			return res
		case <-ticker.C:
			seq++
			payload := []byte(fmt.Sprintf("netprobex-ws-%d", seq))
			begin := time.Now()
			_ = ws.c.SetDeadline(time.Now().Add(timeout))
			if err := ws.WriteFrame(0x1, payload); err != nil {
				res.Failed++
				res.Disconnects++
				res.Error = err.Error()
				_ = ws.Close()
				if connect() {
					res.Reconnects++
				}
				continue
			}
			res.Sent++
			op, reply, err := ws.ReadFrame()
			if err != nil || op == 0x8 || string(reply) != string(payload) {
				res.Failed++
				res.Disconnects++
				if err != nil {
					res.Error = err.Error()
				} else if op == 0x8 {
					res.CloseReason = string(reply)
				} else {
					res.Error = "echo mismatch"
				}
				_ = ws.Close()
				if connect() {
					res.Reconnects++
				}
				continue
			}
			res.Received++
			addLatency(&res.AvgLatency, &res.MinLatency, &res.MaxLatency, time.Since(begin), res.Received)
		}
	}
}

func (ws *Conn) Close() error {
	if ws == nil || ws.c == nil {
		return nil
	}
	return ws.c.Close()
}

func (ws *Conn) ReadFrame() (byte, []byte, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(ws.reader, h); err != nil {
		return 0, nil, err
	}
	op := h[0] & 0x0f
	masked := h[1]&0x80 != 0
	l := uint64(h[1] & 0x7f)
	if l == 126 {
		var b [2]byte
		if _, err := io.ReadFull(ws.reader, b[:]); err != nil {
			return 0, nil, err
		}
		l = uint64(binary.BigEndian.Uint16(b[:]))
	} else if l == 127 {
		var b [8]byte
		if _, err := io.ReadFull(ws.reader, b[:]); err != nil {
			return 0, nil, err
		}
		l = binary.BigEndian.Uint64(b[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(ws.reader, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	if l > 64*1024*1024 {
		return 0, nil, fmt.Errorf("websocket frame too large: %d", l)
	}
	payload := make([]byte, int(l))
	if _, err := io.ReadFull(ws.reader, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return op, payload, nil
}

func (ws *Conn) WriteFrame(op byte, payload []byte) error {
	header := []byte{0x80 | op}
	maskBit := byte(0)
	if !ws.server {
		maskBit = 0x80
	}
	l := len(payload)
	switch {
	case l < 126:
		header = append(header, maskBit|byte(l))
	case l <= 65535:
		header = append(header, maskBit|126, byte(l>>8), byte(l))
	default:
		header = append(header, maskBit|127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(l))
		header = append(header, b[:]...)
	}
	out := payload
	if !ws.server {
		var mask [4]byte
		_, _ = rand.Read(mask[:])
		header = append(header, mask[:]...)
		out = append([]byte(nil), payload...)
		for i := range out {
			out[i] ^= mask[i%4]
		}
	}
	if _, err := ws.c.Write(header); err != nil {
		return err
	}
	_, err := ws.c.Write(out)
	return err
}

func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
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
