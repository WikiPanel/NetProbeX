package tlsprobe

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	"netprobex/internal/logger"
	"netprobex/internal/stats"
)

type Server struct {
	Port     int
	CertFile string
	KeyFile  string
	Log      *logger.Logger
	Stats    *stats.ServerStats
}

func (s *Server) Run(ctx context.Context, wg *sync.WaitGroup) error {
	if s.Port == 0 {
		return nil
	}
	cert, err := loadOrGenerateCert(s.CertFile, s.KeyFile)
	if err != nil {
		return err
	}
	ln, err := tls.Listen("tcp", fmt.Sprintf(":%d", s.Port), &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		return fmt.Errorf("tls listen %d: %w", s.Port, err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = ln.Close()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Log.Event("tls_listener_started", map[string]any{"protocol": "tls", "port": s.Port, "self_signed": s.CertFile == "" || s.KeyFile == ""})
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.Log.Event("tls_accept_error", map[string]any{"protocol": "tls", "port": s.Port, "error": err.Error()})
				continue
			}
			wg.Add(1)
			go s.handle(wg, conn)
		}
	}()
	return nil
}

func (s *Server) handle(wg *sync.WaitGroup, c net.Conn) {
	defer wg.Done()
	defer c.Close()
	host, _, _ := net.SplitHostPort(c.RemoteAddr().String())
	tc, ok := c.(*tls.Conn)
	if !ok {
		return
	}
	start := time.Now()
	_ = tc.SetDeadline(time.Now().Add(10 * time.Second))
	if err := tc.Handshake(); err != nil {
		s.Log.Event("tls_handshake_error", map[string]any{"client_ip": host, "protocol": "tls", "port": s.Port, "error": err.Error()})
		return
	}
	state := tc.ConnectionState()
	s.Stats.TLSHandshake(s.Port)
	s.Log.Event("tls_handshake", map[string]any{"client_ip": host, "protocol": "tls", "port": s.Port, "duration_ms": time.Since(start).Milliseconds(), "tls_version": versionName(state.Version), "cipher": tls.CipherSuiteName(state.CipherSuite)})
	_, _ = tc.Write([]byte("netprobex tls ok\n"))
}

func loadOrGenerateCert(certFile, keyFile string) (tls.Certificate, error) {
	if certFile != "" && keyFile != "" {
		return tls.LoadX509KeyPair(certFile, keyFile)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{CommonName: "NetProbeX self-signed"},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(365 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return tls.X509KeyPair(certPEM, keyPEM)
}
