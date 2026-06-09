package ingesttcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/terion-name/air3/internal/ingest"
	"github.com/terion-name/air3/internal/pending"
)

const (
	DefaultCopyBufferBytes = 32 * 1024
	successAck             = byte(0)
)

type ServerOptions struct {
	Registry                   *pending.Registry
	TLSConfig                  *tls.Config
	AllowedConnectorIdentities []string
	MaxHeaderBytes             int
	CopyBufferBytes            int
}

type Server struct {
	registry        *pending.Registry
	tlsConfig       *tls.Config
	authorizer      ingest.ConnectorAuthorizer
	maxHeaderBytes  int
	copyBufferBytes int
}

func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Registry == nil {
		return nil, errors.New("pending registry is required")
	}
	if opts.TLSConfig == nil {
		return nil, errors.New("TLS config is required")
	}
	maxHeaderBytes := opts.MaxHeaderBytes
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = DefaultMaxHeaderBytes
	}
	copyBufferBytes := opts.CopyBufferBytes
	if copyBufferBytes <= 0 {
		copyBufferBytes = DefaultCopyBufferBytes
	}
	return &Server{
		registry:        opts.Registry,
		tlsConfig:       opts.TLSConfig.Clone(),
		authorizer:      ingest.NewConnectorAuthorizer(opts.AllowedConnectorIdentities),
		maxHeaderBytes:  maxHeaderBytes,
		copyBufferBytes: copyBufferBytes,
	}, nil
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	defer func() {
		close(done)
		wg.Wait()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return ctx.Err()
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.handleConn(ctx, conn)
		}()
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	tlsConn := tls.Server(conn, s.tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("TLS handshake: %w", err)
	}
	if err := s.authorizer.AuthorizePeerCertificates(tlsConn.ConnectionState().PeerCertificates); err != nil {
		return fmt.Errorf("authorize connector certificate: %w", err)
	}

	header, err := DecodeHeader(tlsConn, s.maxHeaderBytes)
	if err != nil {
		return err
	}
	stream, err := s.registry.StartIngest(header.RequestID, header.IngestToken, header.Metadata)
	if err != nil {
		return err
	}

	copyErr := s.copyBody(stream, tlsConn, header.BodyLength)
	closeErr := stream.CloseWithError(copyErr)
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if _, err := tlsConn.Write([]byte{successAck}); err != nil {
		return fmt.Errorf("write ingest TCP ack: %w", err)
	}
	return nil
}

func (s *Server) copyBody(dst io.Writer, src io.Reader, bodyLength int64) error {
	buf := make([]byte, s.copyBufferBytes)
	if bodyLength == UnknownBodyLength {
		_, err := io.CopyBuffer(dst, src, buf)
		if err != nil {
			return fmt.Errorf("copy unknown-length ingest body: %w", err)
		}
		return nil
	}

	limited := &io.LimitedReader{R: src, N: bodyLength}
	_, err := io.CopyBuffer(dst, limited, buf)
	if err != nil {
		return fmt.Errorf("copy ingest body: %w", err)
	}
	if limited.N != 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}
