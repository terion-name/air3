package ingestquic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/terion-name/air3/internal/pending"
)

const alpn = "air3-ingest-quic/1"

type ClientRequest struct {
	RequestID   string
	IngestToken string
	Metadata    pending.Metadata
	Body        io.Reader
	BodyLength  int64
}

type Option func(*senderOptions)

type senderOptions struct {
	quicConfig *quic.Config
}

func WithQUICConfig(config *quic.Config) Option {
	return func(opts *senderOptions) {
		opts.quicConfig = cloneQUICConfig(config)
	}
}

func DialAndSend(ctx context.Context, address string, tlsConfig *tls.Config, req ClientRequest, opts ...Option) error {
	sender, err := NewSender(address, tlsConfig, opts...)
	if err != nil {
		return err
	}
	defer sender.Close()
	return sender.Send(ctx, req)
}

type Sender struct {
	address    string
	tlsConfig  *tls.Config
	quicConfig *quic.Config

	mu     sync.Mutex
	conn   *quic.Conn
	closed bool
}

func NewSender(address string, tlsConfig *tls.Config, opts ...Option) (*Sender, error) {
	if tlsConfig == nil {
		return nil, errors.New("TLS config is required")
	}
	options := senderOptions{}
	for _, option := range opts {
		if option != nil {
			option(&options)
		}
	}
	return &Sender{address: address, tlsConfig: cloneTLSConfigWithALPN(tlsConfig), quicConfig: options.quicConfig}, nil
}

func (s *Sender) Send(ctx context.Context, req ClientRequest) error {
	if req.Body == nil {
		req.Body = emptyReader{}
	}
	header, err := validateHeader(Header{
		RequestID:   req.RequestID,
		IngestToken: req.IngestToken,
		BodyLength:  req.BodyLength,
		Metadata:    req.Metadata,
	})
	if err != nil {
		return err
	}

	stream, err := s.openStream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	clearDeadline := setStreamDeadlineFromContext(ctx, stream)
	defer clearDeadline()

	if err := EncodeHeader(stream, header); err != nil {
		return err
	}
	if err := copyClientBody(stream, req.Body, header.BodyLength); err != nil {
		return err
	}
	return readAck(ctx, stream)
}

func (s *Sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.closeConnLocked()
}

func (s *Sender) openStream(ctx context.Context) (*quic.Stream, error) {
	stream, retryable, err := s.openStreamOnce(ctx)
	if err == nil {
		return stream, nil
	}
	if !retryable {
		return nil, err
	}
	s.resetConn()
	stream, _, retryErr := s.openStreamOnce(ctx)
	if retryErr != nil {
		return nil, fmt.Errorf("retry opening QUIC stream after %v: %w", err, retryErr)
	}
	return stream, nil
}

func (s *Sender) openStreamOnce(ctx context.Context) (*quic.Stream, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, errors.New("ingest QUIC sender is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.conn == nil || s.conn.Context().Err() != nil {
		_ = s.closeConnLocked()
		if err := s.dialLocked(ctx); err != nil {
			return nil, false, err
		}
	}
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, false, ctxErr
		}
		return nil, true, fmt.Errorf("open QUIC stream: %w", err)
	}
	return stream, false, nil
}

func (s *Sender) dialLocked(ctx context.Context) error {
	conn, err := quic.DialAddr(ctx, s.address, s.tlsConfig.Clone(), cloneQUICConfig(s.quicConfig))
	if err != nil {
		return fmt.Errorf("dial ingest QUIC server: %w", err)
	}
	s.conn = conn
	return nil
}

func (s *Sender) resetConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.closeConnLocked()
}

func (s *Sender) closeConnLocked() error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.CloseWithError(0, "")
	s.conn = nil
	return err
}

func copyClientBody(stream *quic.Stream, body io.Reader, bodyLength int64) error {
	if bodyLength == UnknownBodyLength {
		if _, err := io.Copy(stream, body); err != nil {
			return fmt.Errorf("copy unknown-length ingest body: %w", err)
		}
		if err := stream.Close(); err != nil {
			return fmt.Errorf("close ingest QUIC write side: %w", err)
		}
		return nil
	}

	limited := &io.LimitedReader{R: body, N: bodyLength}
	if _, err := io.Copy(stream, limited); err != nil {
		return fmt.Errorf("copy ingest body: %w", err)
	}
	if limited.N != 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func readAck(ctx context.Context, stream *quic.Stream) error {
	var ack [1]byte
	_, err := io.ReadFull(stream, ack[:])
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("read ingest QUIC ack: %w", err)
	}
	if ack[0] != successAck {
		return fmt.Errorf("ingest QUIC server returned non-success ack %d", ack[0])
	}
	return nil
}

func setStreamDeadlineFromContext(ctx context.Context, stream interface{ SetDeadline(time.Time) error }) func() {
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	stopCancelWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.SetDeadline(time.Now())
		case <-stopCancelWatch:
		}
	}()
	return func() {
		close(stopCancelWatch)
		_ = stream.SetDeadline(time.Time{})
	}
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func cloneTLSConfigWithALPN(config *tls.Config) *tls.Config {
	clone := config.Clone()
	for _, proto := range clone.NextProtos {
		if proto == alpn {
			return clone
		}
	}
	clone.NextProtos = append([]string{alpn}, clone.NextProtos...)
	return clone
}

func cloneQUICConfig(config *quic.Config) *quic.Config {
	if config == nil {
		return nil
	}
	return config.Clone()
}
