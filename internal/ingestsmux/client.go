package ingestsmux

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
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
	smuxConfig      *smux.Config
	sessionPoolSize int
}

func WithSmuxConfig(config *smux.Config) Option {
	return func(opts *senderOptions) {
		opts.smuxConfig = cloneSmuxConfig(config)
	}
}

// WithSessionPoolSize sets the number of lazily created smux sessions used by a Sender.
// Size must be at least 1.
func WithSessionPoolSize(size int) Option {
	return func(opts *senderOptions) {
		opts.sessionPoolSize = size
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

	nextSlot atomic.Uint64
	closed   atomic.Bool
	slots    []smuxSessionSlot
}

type smuxSessionSlot struct {
	mu      sync.Mutex
	session *smux.Session
}

func NewSender(network, address string, tlsConfig *tls.Config, opts ...Option) (*Sender, error) {
	if tlsConfig == nil {
		return nil, errors.New("TLS config is required")
	}
	options := senderOptions{sessionPoolSize: 1}
	for _, option := range opts {
		if option != nil {
			option(&options)
		}
	}
	if options.sessionPoolSize < 1 {
		return nil, fmt.Errorf("session pool size must be at least 1: %d", options.sessionPoolSize)
	}
	if err := smux.VerifyConfig(smuxConfigOrDefault(options.smuxConfig)); err != nil {
		return nil, fmt.Errorf("smux config: %w", err)
	}
	return &Sender{
		network:    network,
		address:    address,
		tlsConfig:  tlsConfig.Clone(),
		smuxConfig: options.smuxConfig,
		slots:      make([]smuxSessionSlot, options.sessionPoolSize),
	}, nil
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
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	var closeErr error
	for i := range s.slots {
		slot := &s.slots[i]
		slot.mu.Lock()
		closeErr = errors.Join(closeErr, closeSessionSlotLocked(slot))
		slot.mu.Unlock()
	}
	return closeErr
}

func (s *Sender) openStream(ctx context.Context) (*smux.Stream, error) {
	slot := s.nextSessionSlot()
	stream, err := s.openStreamOnce(ctx, slot)
	if err == nil {
		return stream, nil
	}
	s.resetSession(slot)
	stream, retryErr := s.openStreamOnce(ctx, slot)
	if retryErr != nil {
		return nil, fmt.Errorf("retry opening smux stream after %v: %w", err, retryErr)
	}
	return stream, nil
}

func (s *Sender) nextSessionSlot() *smuxSessionSlot {
	idx := s.nextSlot.Add(1) - 1
	return &s.slots[idx%uint64(len(s.slots))]
}

func (s *Sender) openStreamOnce(ctx context.Context, slot *smuxSessionSlot) (*smux.Stream, error) {
	if s.closed.Load() {
		return nil, errors.New("ingest smux sender is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	slot.mu.Lock()
	defer slot.mu.Unlock()
	if s.closed.Load() {
		return nil, errors.New("ingest smux sender is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if slot.session == nil || slot.session.IsClosed() {
		if err := s.dialSlotLocked(ctx, slot); err != nil {
			return nil, err
		}
		if s.closed.Load() {
			_ = closeSessionSlotLocked(slot)
			return nil, errors.New("ingest smux sender is closed")
		}
	}
	stream, err := slot.session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open smux stream: %w", err)
	}
	return stream, nil
}

func (s *Sender) dialSlotLocked(ctx context.Context, slot *smuxSessionSlot) error {
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
	slot.session = session
	return nil
}

func (s *Sender) resetSession(slot *smuxSessionSlot) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	_ = closeSessionSlotLocked(slot)
}

func closeSessionSlotLocked(slot *smuxSessionSlot) error {
	if slot.session == nil {
		return nil
	}
	err := slot.session.Close()
	slot.session = nil
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
