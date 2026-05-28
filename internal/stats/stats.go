package stats

import (
	"sync"
	"time"
)

type PortStats struct {
	Connections int64 `json:"connections,omitempty"`
	Active      int64 `json:"active,omitempty"`
	Packets     int64 `json:"packets,omitempty"`
	Requests    int64 `json:"requests,omitempty"`
	Errors      int64 `json:"errors,omitempty"`
}

type ProtocolStats struct {
	TCP       int64 `json:"tcp"`
	UDP       int64 `json:"udp"`
	HTTP      int64 `json:"http"`
	WebSocket int64 `json:"websocket"`
	TLS       int64 `json:"tls"`
	Download  int64 `json:"download"`
}

type Snapshot struct {
	UptimeSeconds               int64                `json:"uptime_seconds"`
	ActiveTCPConnections        int64                `json:"active_tcp_connections"`
	ActiveWebSocketConnections  int64                `json:"active_websocket_connections"`
	TCPConnectionCount          int64                `json:"tcp_connection_count"`
	UDPPacketCount              int64                `json:"udp_packet_count"`
	HTTPRequestCount            int64                `json:"http_request_count"`
	DownloadCount               int64                `json:"download_count"`
	FailedDownloadCount         int64                `json:"failed_interrupted_download_count"`
	PerPort                     map[string]PortStats `json:"per_port_stats"`
	PerProtocol                 ProtocolStats        `json:"per_protocol_stats"`
}

type ServerStats struct {
	mu         sync.Mutex
	started    time.Time
	activeTCP  int64
	activeWS   int64
	tcpTotal   int64
	udpPackets int64
	httpReqs   int64
	downloads  int64
	failedDL   int64
	protocol   ProtocolStats
	perPort    map[string]PortStats
}

func NewServerStats() *ServerStats {
	return &ServerStats{started: time.Now(), perPort: map[string]PortStats{}}
}

func (s *ServerStats) TCPConnect(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTCP++
	s.tcpTotal++
	s.protocol.TCP++
	p := s.pp(port)
	p.Connections++
	p.Active++
	s.set(port, p)
}

func (s *ServerStats) TCPDisconnect(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeTCP > 0 {
		s.activeTCP--
	}
	p := s.pp(port)
	if p.Active > 0 {
		p.Active--
	}
	s.set(port, p)
}

func (s *ServerStats) TCPError(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pp(port)
	p.Errors++
	s.set(port, p)
}

func (s *ServerStats) UDPPacket(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.udpPackets++
	s.protocol.UDP++
	p := s.pp(port)
	p.Packets++
	s.set(port, p)
}

func (s *ServerStats) HTTPRequest(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpReqs++
	s.protocol.HTTP++
	p := s.pp(port)
	p.Requests++
	s.set(port, p)
}

func (s *ServerStats) WSConnect(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeWS++
	s.protocol.WebSocket++
	p := s.pp(port)
	p.Connections++
	p.Active++
	s.set(port, p)
}

func (s *ServerStats) WSDisconnect(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeWS > 0 {
		s.activeWS--
	}
	p := s.pp(port)
	if p.Active > 0 {
		p.Active--
	}
	s.set(port, p)
}

func (s *ServerStats) TLSHandshake(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocol.TLS++
	p := s.pp(port)
	p.Connections++
	s.set(port, p)
}

func (s *ServerStats) Download(port int, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.downloads++
	s.protocol.Download++
	if failed {
		s.failedDL++
	}
	p := s.pp(port)
	p.Requests++
	if failed {
		p.Errors++
	}
	s.set(port, p)
}

func (s *ServerStats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	pp := make(map[string]PortStats, len(s.perPort))
	for k, v := range s.perPort {
		pp[k] = v
	}
	return Snapshot{
		UptimeSeconds:              int64(time.Since(s.started).Seconds()),
		ActiveTCPConnections:       s.activeTCP,
		ActiveWebSocketConnections: s.activeWS,
		TCPConnectionCount:         s.tcpTotal,
		UDPPacketCount:             s.udpPackets,
		HTTPRequestCount:           s.httpReqs,
		DownloadCount:              s.downloads,
		FailedDownloadCount:        s.failedDL,
		PerPort:                    pp,
		PerProtocol:                s.protocol,
	}
}

func (s *ServerStats) pp(port int) PortStats {
	return s.perPort[itoa(port)]
}

func (s *ServerStats) set(port int, p PortStats) { s.perPort[itoa(port)] = p }

func itoa(port int) string {
	if port == 0 {
		return "0"
	}
	var b [6]byte
	i := len(b)
	for port > 0 {
		i--
		b[i] = byte('0' + port%10)
		port /= 10
	}
	return string(b[i:])
}
