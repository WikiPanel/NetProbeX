package udp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"netprobex/internal/logger"
	"netprobex/internal/stats"
)

type Server struct {
	Ports []int
	Log   *logger.Logger
	Stats *stats.ServerStats
}

func (s *Server) Run(ctx context.Context, wg *sync.WaitGroup) error {
	for _, port := range s.Ports {
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
		if err != nil {
			return err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return fmt.Errorf("udp listen %d: %w", port, err)
		}
		wg.Add(1)
		go s.serve(ctx, wg, port, conn)
	}
	return nil
}

func (s *Server) serve(ctx context.Context, wg *sync.WaitGroup, port int, conn *net.UDPConn) {
	defer wg.Done()
	defer conn.Close()
	go func() { <-ctx.Done(); _ = conn.Close() }()
	counts := map[string]int64{}
	buf := make([]byte, 64*1024)
	s.Log.Event("udp_listener_started", map[string]any{"protocol": "udp", "port": port})
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				s.Log.Event("udp_idle_timeout", map[string]any{"protocol": "udp", "port": port})
				continue
			}
			s.Log.Event("udp_read_error", map[string]any{"protocol": "udp", "port": port, "error": err.Error()})
			continue
		}
		counts[addr.IP.String()]++
		s.Stats.UDPPacket(port)
		_, werr := conn.WriteToUDP(buf[:n], addr)
		if werr != nil {
			s.Log.Event("udp_write_error", map[string]any{"client_ip": addr.IP.String(), "protocol": "udp", "port": port, "error": werr.Error()})
			continue
		}
		s.Log.Event("udp_packet", map[string]any{"client_ip": addr.IP.String(), "protocol": "udp", "port": port, "client_packet_count": counts[addr.IP.String()], "bytes": n})
	}
}
