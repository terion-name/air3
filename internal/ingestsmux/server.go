package ingestsmux

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
	"github.com/xtaci/smux"
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
	SmuxConfig                 *smux.Config
}

type Server struct {
	registry        *pending.Registry
	tlsConfig       *tls.Config
	authorizer      ingest.ConnectorAuthorizer
	maxHeaderBytes  int
	copyBufferBytes int
	smuxConfig      *smux.Config
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
	smuxConfig := cloneSmuxConfig(opts.SmuxConfig)
	if err := smux.VerifyConfig(smuxConfigOrDefault(smuxConfig)); err != nil {
		return nil, fmt.Errorf("smux config: %w", err)
	}
	return &Server{
		registry:        opts.Registry,
		tlsConfig:       opts.TLSConfig.Clone(),
		authorizer:      ingest.NewConnectorAuthorizer(opts.AllowedConnectorIdentities),
		maxHeaderBytes:  maxHeaderBytes,
		copyBufferBytes: copyBufferBytes,
		smuxConfig:      smuxConfig,
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
	_ = tlsConn.SetDeadline(noDeadline)

	session, err := smux.Server(tlsConn, cloneSmuxConfig(s.smuxConfig))
	if err != nil {
		return fmt.Errorf("start smux server session: %w", err)
	}
	defer session.Close()

	stopCancelWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = session.Close()
		case <-stopCancelWatch:
		}
	}()
	defer close(stopCancelWatch)

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			if ctx.Err() != nil || session.IsClosed() {
				return ctx.Err()
			}
			return fmt.Errorf("accept smux stream: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.handleStream(stream)
		}()
	}
}

func (s *Server) handleStream(stream *smux.Stream) error {
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
		return fmt.Errorf("write ingest smux ack: %w", err)
	}
	// The client closes the stream after reading the ack. Closing the server
	// write side immediately can race smux stream cleanup against that read for
	// unknown-length bodies, where the client already half-closed its writer.
	return nil
}

func (s *Server) copyBody(dst io.Writer, src io.Reader, bodyLength int64) error {
	buf := make([]byte, s.copyBufferBytes)
	if bodyLength == UnknownBodyLength {
		_, err := io.CopyBuffer(dst, src, buf)
		if err != nil && !errors.Is(err, io.EOF) {
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
