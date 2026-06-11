package natsclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/mtls"
	"github.com/terion-name/air3/internal/tickets"
)

const defaultIOTimeout = 2 * time.Second

var (
	ErrInvalidConfig = errors.New("invalid nats config")
	ErrNotConnected  = errors.New("nats client is not connected")
)

type TicketHandler func(context.Context, tickets.Ticket) error

type ErrorHandler func(error)

type Client struct {
	conn       *nats.Conn
	subject    string
	queueGroup string
	now        func() time.Time
}

type Subscription struct {
	sub *nats.Subscription
}

// Connect opens a Core NATS connection for ticket publish/subscribe operations.
func Connect(ctx context.Context, cfg config.NATSConfig) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := options(ctx, cfg)
	if err != nil {
		return nil, err
	}
	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	if err := ctx.Err(); err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{
		conn:       conn,
		subject:    cfg.Subject,
		queueGroup: cfg.QueueGroup,
		now:        time.Now,
	}, nil
}

// PublishTicket publishes a validated ticket to the configured subject and flushes within ctx or a short default timeout.
func (c *Client) PublishTicket(ctx context.Context, ticket tickets.Ticket) error {
	subject := ""
	if c != nil {
		subject = c.subject
	}
	return c.PublishTicketTo(ctx, subject, ticket)
}

// PublishTicketTo publishes a validated ticket to subject and flushes within ctx or a short default timeout.
func (c *Client) PublishTicketTo(ctx context.Context, subject string, ticket tickets.Ticket) error {
	if ctx == nil {
		ctx = context.Background()
	}
	data, err := tickets.Marshal(ticket, c.currentTime())
	if err != nil {
		return fmt.Errorf("marshal ticket: %w", err)
	}
	if err := validatePublishSubject(subject); err != nil {
		return err
	}
	if c == nil || c.conn == nil || c.conn.IsClosed() {
		return ErrNotConnected
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("publish ticket: %w", err)
	}
	if err := c.conn.FlushTimeout(timeoutFor(ctx)); err != nil {
		return fmt.Errorf("flush ticket publish: %w", err)
	}
	return nil
}

// QueueSubscribeTickets subscribes to tickets through the configured Core NATS queue group.
func (c *Client) QueueSubscribeTickets(ctx context.Context, handle TicketHandler, onError ErrorHandler) (*Subscription, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.conn == nil || c.conn.IsClosed() {
		return nil, ErrNotConnected
	}
	if c.queueGroup == "" {
		return nil, fmt.Errorf("%w: queue group is required for ticket subscriptions", ErrInvalidConfig)
	}
	if handle == nil {
		return nil, fmt.Errorf("%w: ticket handler is required", ErrInvalidConfig)
	}

	sub, err := c.conn.QueueSubscribe(c.subject, c.queueGroup, func(msg *nats.Msg) {
		ticket, err := tickets.Unmarshal(msg.Data, c.currentTime())
		if err != nil {
			report(onError, fmt.Errorf("decode ticket on %s: %w", msg.Subject, err))
			return
		}
		if err := handle(ctx, ticket); err != nil {
			report(onError, fmt.Errorf("handle ticket %s: %w", ticket.RequestID, err))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("queue subscribe tickets: %w", err)
	}
	if err := c.conn.FlushTimeout(timeoutFor(ctx)); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flush ticket subscription: %w", err)
	}
	return &Subscription{sub: sub}, nil
}

func (s *Subscription) Drain(ctx context.Context) error {
	if s == nil || s.sub == nil {
		return nil
	}
	return runWithContext(ctx, func() error { return s.sub.Drain() }, func() { _ = s.sub.Unsubscribe() })
}

func (c *Client) Drain(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return nil
	}
	return runWithContext(ctx, c.conn.Drain, c.conn.Close)
}

func (c *Client) Close() {
	if c == nil || c.conn == nil {
		return
	}
	c.conn.Close()
}

func (c *Client) currentTime() time.Time {
	if c != nil && c.now != nil {
		return c.now()
	}
	return time.Now()
}

func options(ctx context.Context, cfg config.NATSConfig) ([]nats.Option, error) {
	opts := []nats.Option{
		nats.Name("air3"),
		nats.Timeout(timeoutFor(ctx)),
		nats.RetryOnFailedConnect(false),
	}
	if tlsConfigured(cfg.TLS) {
		tlsCfg, err := mtls.ClientConfig(mtls.ClientOptions{Files: mtls.Files{
			CAFile:             cfg.TLS.CAFile,
			CertFile:           cfg.TLS.CertFile,
			KeyFile:            cfg.TLS.KeyFile,
			ServerName:         cfg.TLS.ServerName,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		}})
		if err != nil {
			return nil, fmt.Errorf("load nats tls config: %w", err)
		}
		opts = append(opts, nats.Secure(tlsCfg))
	}
	if cfg.User != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	if cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	}
	if cfg.NKeyFile != "" {
		nkeyOpt, err := nats.NkeyOptionFromSeed(cfg.NKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load nats nkey seed: %w", err)
		}
		opts = append(opts, nkeyOpt)
	}
	return opts, nil
}

func validateConfig(cfg config.NATSConfig) error {
	if strings.TrimSpace(cfg.URL) == "" {
		return fmt.Errorf("%w: url is required", ErrInvalidConfig)
	}
	if err := validatePublishSubject(cfg.Subject); err != nil {
		return err
	}
	if cfg.QueueGroup != "" {
		if err := validateQueueGroup(cfg.QueueGroup); err != nil {
			return err
		}
	}
	return nil
}

func validatePublishSubject(subject string) error {
	if err := config.ValidateNATSSubject(subject); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return nil
}

func validateQueueGroup(queue string) error {
	if strings.TrimSpace(queue) == "" || hasSpaceOrControl(queue) {
		return fmt.Errorf("%w: queue group contains whitespace or control characters", ErrInvalidConfig)
	}
	return nil
}

func hasSpaceOrControl(value string) bool {
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func tlsConfigured(cfg config.MTLSPaths) bool {
	return cfg.CAFile != "" || cfg.CertFile != "" || cfg.KeyFile != "" || cfg.ServerName != "" || cfg.InsecureSkipVerify
}

func timeoutFor(ctx context.Context) time.Duration {
	if ctx == nil {
		return defaultIOTimeout
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return defaultIOTimeout
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 1 * time.Nanosecond
	}
	if remaining < defaultIOTimeout {
		return remaining
	}
	return defaultIOTimeout
}

func report(onError ErrorHandler, err error) {
	if onError != nil {
		onError(err)
	}
}

func runWithContext(ctx context.Context, run func() error, cancel func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	go func() {
		done <- run()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		cancel()
		<-done
		return ctx.Err()
	}
}
