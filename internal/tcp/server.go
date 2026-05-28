package tcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return fmt.Errorf("tcp listen %d: %w", port, err)
		}
		wg.Add(1)
		go s.serve(ctx, wg, port, ln)
	}
	return nil
}

func (s *Server) serve(ctx context.Context, wg *sync.WaitGroup, port int, ln net.Listener) {
	defer wg.Done()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	s.Log.Event("tcp_listener_started", map[string]any{"protocol": "tcp", "port": port})
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.Log.Event("tcp_accept_error", map[string]any{"protocol": "tcp", "port": port, "error": err.Error()})
			continue
		}
		wg.Add(1)
		go s.handle(ctx, wg, port, conn)
	}
}

func (s *Server) handle(ctx context.Context, wg *sync.WaitGroup, port int, conn net.Conn) {
	defer wg.Done()
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(remote)
	s.Stats.TCPConnect(port)
	s.Log.Event("tcp_connected", map[string]any{"client_ip": host, "remote_addr": remote, "protocol": "tcp", "port": port})
	defer func() {
		s.Stats.TCPDisconnect(port)
		s.Log.Event("tcp_disconnected", map[string]any{"client_ip": host, "remote_addr": remote, "protocol": "tcp", "port": port})
	}()
	reader := bufio.NewReader(conn)
	buf := make([]byte, 64*1024)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := reader.Read(buf)
		if n > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, werr := conn.Write(buf[:n]); werr != nil {
				s.Stats.TCPError(port)
				s.Log.Event("tcp_write_error", map[string]any{"client_ip": host, "protocol": "tcp", "port": port, "error": werr.Error()})
				return
			}
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			if !errors.Is(err, io.EOF) {
				s.Stats.TCPError(port)
				s.Log.Event("tcp_read_error", map[string]any{"client_ip": host, "protocol": "tcp", "port": port, "error": err.Error()})
			}
			return
		}
	}
}
