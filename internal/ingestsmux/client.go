package ingestsmux

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/terion-name/air3/internal/pending"
	"github.com/xtaci/smux"
)

type ClientRequest struct {
	RequestID   string
	IngestToken string
	Metadata    pending.Metadata
	Body        io.Reader
	BodyLength  int64
}

type Option func(*senderOptions)

type senderOptions struct {
	smuxConfig *smux.Config
}

func WithSmuxConfig(config *smux.Config) Option {
	return func(opts *senderOptions) {
		opts.smuxConfig = cloneSmuxConfig(config)
	}
}

func DialAndSend(ctx context.Context, network, address string, tlsConfig *tls.Config, req ClientRequest, opts ...Option) error {
	sender, err := NewSender(network, address, tlsConfig, opts...)
	if err != nil {
		return err
	}
	defer sender.Close()
	return sender.Send(ctx, req)
}

type Sender struct {
	network    string
	address    string
	tlsConfig  *tls.Config
	smuxConfig *smux.Config

	mu      sync.Mutex
	session *smux.Session
	closed  bool
}

func NewSender(network, address string, tlsConfig *tls.Config, opts ...Option) (*Sender, error) {
	if tlsConfig == nil {
		return nil, errors.New("TLS config is required")
	}
	options := senderOptions{}
	for _, option := range opts {
		if option != nil {
			option(&options)
		}
	}
	if err := smux.VerifyConfig(smuxConfigOrDefault(options.smuxConfig)); err != nil {
		return nil, fmt.Errorf("smux config: %w", err)
	}
	return &Sender{network: network, address: address, tlsConfig: tlsConfig.Clone(), smuxConfig: options.smuxConfig}, nil
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
	return s.closeSessionLocked()
}

func (s *Sender) openStream(ctx context.Context) (*smux.Stream, error) {
	stream, err := s.openStreamOnce(ctx)
	if err == nil {
		return stream, nil
	}
	s.resetSession()
	stream, retryErr := s.openStreamOnce(ctx)
	if retryErr != nil {
		return nil, fmt.Errorf("retry opening smux stream after %v: %w", err, retryErr)
	}
	return stream, nil
}

func (s *Sender) openStreamOnce(ctx context.Context) (*smux.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("ingest smux sender is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.session == nil || s.session.IsClosed() {
		if err := s.dialLocked(ctx); err != nil {
			return nil, err
		}
	}
	stream, err := s.session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open smux stream: %w", err)
	}
	return stream, nil
}

func (s *Sender) dialLocked(ctx context.Context) error {
	dialer := &tls.Dialer{Config: s.tlsConfig.Clone()}
	conn, err := dialer.DialContext(ctx, s.network, s.address)
	if err != nil {
		return fmt.Errorf("dial ingest smux server: %w", err)
	}
	session, err := smux.Client(conn, cloneSmuxConfig(s.smuxConfig))
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("start smux client session: %w", err)
	}
	s.session = session
	return nil
}

func (s *Sender) resetSession() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.closeSessionLocked()
}

func (s *Sender) closeSessionLocked() error {
	if s.session == nil {
		return nil
	}
	err := s.session.Close()
	s.session = nil
	return err
}

type closeWriter interface {
	CloseWrite() error
}

func copyClientBody(conn io.ReadWriteCloser, body io.Reader, bodyLength int64) error {
	if bodyLength == UnknownBodyLength {
		if _, err := io.Copy(conn, body); err != nil {
			return fmt.Errorf("copy unknown-length ingest body: %w", err)
		}
		cw, ok := conn.(closeWriter)
		if !ok {
			return errors.New("unknown-length ingest body requires a stream with CloseWrite")
		}
		if err := cw.CloseWrite(); err != nil {
			return fmt.Errorf("close ingest smux write side: %w", err)
		}
		return nil
	}

	limited := &io.LimitedReader{R: body, N: bodyLength}
	if _, err := io.Copy(conn, limited); err != nil {
		return fmt.Errorf("copy ingest body: %w", err)
	}
	if limited.N != 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func readAck(ctx context.Context, conn net.Conn) error {
	var ack [1]byte
	_, err := io.ReadFull(conn, ack[:])
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("read ingest smux ack: %w", err)
	}
	if ack[0] != successAck {
		return fmt.Errorf("ingest smux server returned non-success ack %d", ack[0])
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

var noDeadline time.Time

func cloneSmuxConfig(config *smux.Config) *smux.Config {
	if config == nil {
		return nil
	}
	clone := *config
	return &clone
}

func smuxConfigOrDefault(config *smux.Config) *smux.Config {
	if config != nil {
		return config
	}
	return smux.DefaultConfig()
}
