package ingestquic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/quic-go/quic-go"
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
	QUICConfig                 *quic.Config
}

type Server struct {
	registry        *pending.Registry
	tlsConfig       *tls.Config
	authorizer      ingest.ConnectorAuthorizer
	maxHeaderBytes  int
	copyBufferBytes int
	quicConfig      *quic.Config
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
		tlsConfig:       cloneTLSConfigWithALPN(opts.TLSConfig),
		authorizer:      ingest.NewConnectorAuthorizer(opts.AllowedConnectorIdentities),
		maxHeaderBytes:  maxHeaderBytes,
		copyBufferBytes: copyBufferBytes,
		quicConfig:      cloneQUICConfig(opts.QUICConfig),
	}, nil
}

func (s *Server) Serve(ctx context.Context, ln *quic.Listener) error {
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
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) {
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

func (s *Server) handleConn(ctx context.Context, conn *quic.Conn) error {
	defer conn.CloseWithError(0, "")
	select {
	case <-conn.HandshakeComplete():
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Context().Done():
		return conn.Context().Err()
	}
	if err := s.authorizer.AuthorizePeerCertificates(conn.ConnectionState().TLS.PeerCertificates); err != nil {
		_ = conn.CloseWithError(1, "unauthorized connector certificate")
		return fmt.Errorf("authorize connector certificate: %w", err)
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil || conn.Context().Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept QUIC stream: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.handleStream(stream)
		}()
	}
}

func (s *Server) handleStream(stream *quic.Stream) error {
	header, err := DecodeHeader(stream, s.maxHeaderBytes)
	if err != nil {
		_ = stream.Close()
		return err
	}
	ingestStream, err := s.registry.StartIngest(header.RequestID, header.IngestToken, header.Metadata)
	if err != nil {
		_ = stream.Close()
		return err
	}

	copyErr := s.copyBody(ingestStream, stream, header.BodyLength)
	closeErr := ingestStream.CloseWithError(copyErr)
	if copyErr != nil {
		_ = stream.Close()
		return copyErr
	}
	if closeErr != nil {
		_ = stream.Close()
		return closeErr
	}
	if _, err := stream.Write([]byte{successAck}); err != nil {
		_ = stream.Close()
		return fmt.Errorf("write ingest QUIC ack: %w", err)
	}
	if err := stream.Close(); err != nil {
		return fmt.Errorf("close ingest QUIC write side: %w", err)
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
