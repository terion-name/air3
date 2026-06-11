package natsclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/terion-name/air3/internal/config"
	"github.com/terion-name/air3/internal/tickets"
)

func TestValidateConfigRejectsBadSubjectsAndQueues(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.NATSConfig
	}{
		{
			name: "missing url",
			cfg:  config.NATSConfig{Subject: "air3.tickets"},
		},
		{
			name: "missing subject",
			cfg:  config.NATSConfig{URL: "nats://127.0.0.1:4222"},
		},
		{
			name: "wildcard publish subject",
			cfg:  config.NATSConfig{URL: "nats://127.0.0.1:4222", Subject: "air3.*"},
		},
		{
			name: "empty subject token",
			cfg:  config.NATSConfig{URL: "nats://127.0.0.1:4222", Subject: "air3..tickets"},
		},
		{
			name: "whitespace subject",
			cfg:  config.NATSConfig{URL: "nats://127.0.0.1:4222", Subject: "air3.bad tickets"},
		},
		{
			name: "control subject",
			cfg:  config.NATSConfig{URL: "nats://127.0.0.1:4222", Subject: "air3.bad\ntickets"},
		},
		{
			name: "whitespace queue",
			cfg:  config.NATSConfig{URL: "nats://127.0.0.1:4222", Subject: "air3.tickets", QueueGroup: "bad queue"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("validateConfig() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestOptionsRejectBadTLSFiles(t *testing.T) {
	_, err := options(context.Background(), config.NATSConfig{
		URL:     "nats://127.0.0.1:4222",
		Subject: "air3.tickets",
		TLS: config.MTLSPaths{
			CAFile: "testdata/missing-ca.pem",
		},
	})
	if err == nil {
		t.Fatal("options() error = nil, want TLS file error")
	}
}

func TestPublishTicketRejectsInvalidTicketBeforePublish(t *testing.T) {
	client := &Client{subject: "air3.tickets", now: func() time.Time { return time.Unix(100, 0) }}
	err := client.PublishTicket(context.Background(), tickets.Ticket{Version: tickets.Version})
	if !errors.Is(err, tickets.ErrInvalidTicket) {
		t.Fatalf("PublishTicket() error = %v, want invalid ticket", err)
	}
}

func TestPublishTicketToRejectsInvalidSubject(t *testing.T) {
	client := &Client{subject: "air3.tickets", now: func() time.Time { return time.Unix(100, 0) }}
	tests := []struct {
		name    string
		subject string
	}{
		{name: "blank", subject: ""},
		{name: "empty token", subject: "air3..tickets"},
		{name: "whitespace", subject: "air3.bad tickets"},
		{name: "control", subject: "air3.bad\ntickets"},
		{name: "star wildcard", subject: "air3.*.tickets"},
		{name: "greater wildcard", subject: "air3.>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := client.PublishTicketTo(context.Background(), tc.subject, validTicket("req-invalid-subject"))
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("PublishTicketTo() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestQueueSubscribeRequiresQueueGroup(t *testing.T) {
	client := &Client{conn: &nats.Conn{}, subject: "air3.tickets"}
	_, err := client.QueueSubscribeTickets(context.Background(), func(context.Context, tickets.Ticket) error { return nil }, nil)
	if !errors.Is(err, ErrNotConnected) && !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("QueueSubscribeTickets() error = %v, want connection or queue config error", err)
	}
}

func TestQueueSubscriptionHandlesOneTicket(t *testing.T) {
	url := startNATSServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cfg := testConfig(url, t.Name())
	subOne, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect subscriber one: %v", err)
	}
	t.Cleanup(subOne.Close)
	subTwo, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect subscriber two: %v", err)
	}
	t.Cleanup(subTwo.Close)
	pub, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect publisher: %v", err)
	}
	t.Cleanup(pub.Close)

	received := make(chan string, 2)
	handle := func(_ context.Context, ticket tickets.Ticket) error {
		received <- ticket.RequestID
		return nil
	}
	if _, err := subOne.QueueSubscribeTickets(ctx, handle, func(err error) { t.Errorf("subscriber one error: %v", err) }); err != nil {
		t.Fatalf("QueueSubscribeTickets subscriber one: %v", err)
	}
	if _, err := subTwo.QueueSubscribeTickets(ctx, handle, func(err error) { t.Errorf("subscriber two error: %v", err) }); err != nil {
		t.Fatalf("QueueSubscribeTickets subscriber two: %v", err)
	}

	want := "req-queue-one"
	if err := pub.PublishTicket(ctx, validTicket(want)); err != nil {
		t.Fatalf("PublishTicket(): %v", err)
	}

	select {
	case got := <-received:
		if got != want {
			t.Fatalf("received request id %q, want %q", got, want)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for ticket: %v", ctx.Err())
	}
	select {
	case extra := <-received:
		t.Fatalf("queue subscribers received duplicate ticket %q", extra)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestPublishTicketToUsesProvidedSubject(t *testing.T) {
	url := startNATSServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cfg := testConfig(url, t.Name())
	pub, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect publisher: %v", err)
	}
	t.Cleanup(pub.Close)

	raw, err := nats.Connect(url, nats.Timeout(time.Second), nats.RetryOnFailedConnect(false))
	if err != nil {
		t.Fatalf("raw nats connect: %v", err)
	}
	t.Cleanup(raw.Close)

	configuredSub, err := raw.SubscribeSync(cfg.Subject)
	if err != nil {
		t.Fatalf("subscribe configured subject: %v", err)
	}
	toSubject := cfg.Subject + ".derived"
	toSub, err := raw.SubscribeSync(toSubject)
	if err != nil {
		t.Fatalf("subscribe publish-to subject: %v", err)
	}
	if err := raw.FlushTimeout(time.Second); err != nil {
		t.Fatalf("flush subscriptions: %v", err)
	}

	toRequest := "req-publish-to"
	if err := pub.PublishTicketTo(ctx, toSubject, validTicket(toRequest)); err != nil {
		t.Fatalf("PublishTicketTo(): %v", err)
	}
	assertNextTicket(t, toSub, toSubject, toRequest)

	configuredRequest := "req-publish-configured"
	if err := pub.PublishTicket(ctx, validTicket(configuredRequest)); err != nil {
		t.Fatalf("PublishTicket(): %v", err)
	}
	assertNextTicket(t, configuredSub, cfg.Subject, configuredRequest)
}

func TestQueueSubscriptionRejectsMalformedTicket(t *testing.T) {
	url := startNATSServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cfg := testConfig(url, t.Name())
	client, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect subscriber: %v", err)
	}
	t.Cleanup(client.Close)

	invalid := make(chan error, 1)
	var handled atomic.Int32
	if _, err := client.QueueSubscribeTickets(ctx, func(context.Context, tickets.Ticket) error {
		handled.Add(1)
		return nil
	}, func(err error) {
		invalid <- err
	}); err != nil {
		t.Fatalf("QueueSubscribeTickets(): %v", err)
	}

	raw, err := nats.Connect(url, nats.Timeout(time.Second), nats.RetryOnFailedConnect(false))
	if err != nil {
		t.Fatalf("raw nats connect: %v", err)
	}
	t.Cleanup(raw.Close)
	if err := raw.Publish(cfg.Subject, []byte(`{"version":1,"request_id":"bad"}`)); err != nil {
		t.Fatalf("raw publish malformed ticket: %v", err)
	}
	if err := raw.FlushTimeout(time.Second); err != nil {
		t.Fatalf("raw flush malformed ticket: %v", err)
	}

	select {
	case err := <-invalid:
		if !errors.Is(err, tickets.ErrInvalidTicket) {
			t.Fatalf("invalid ticket error = %v, want tickets.ErrInvalidTicket", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for invalid ticket rejection: %v", ctx.Err())
	}
	if handled.Load() != 0 {
		t.Fatalf("handler called for malformed ticket %d time(s)", handled.Load())
	}
}

func assertNextTicket(t *testing.T, sub *nats.Subscription, wantSubject, wantRequestID string) {
	t.Helper()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("receive ticket on %s: %v", wantSubject, err)
	}
	if msg.Subject != wantSubject {
		t.Fatalf("received subject %q, want %q", msg.Subject, wantSubject)
	}
	ticket, err := tickets.Unmarshal(msg.Data, time.Now())
	if err != nil {
		t.Fatalf("decode ticket on %s: %v", wantSubject, err)
	}
	if ticket.RequestID != wantRequestID {
		t.Fatalf("received request id %q on %s, want %q", ticket.RequestID, wantSubject, wantRequestID)
	}
}

func testConfig(url, name string) config.NATSConfig {
	return config.NATSConfig{
		URL:        url,
		Subject:    "air3.test." + sanitizeSubjectToken(name),
		QueueGroup: "air3-test-workers",
	}
}

func validTicket(requestID string) tickets.Ticket {
	return tickets.Ticket{
		Version:        tickets.Version,
		RequestID:      requestID,
		Bucket:         "demo-bucket",
		Key:            "path/object.txt",
		Method:         "GET",
		DeadlineUnixMS: time.Now().Add(time.Minute).UnixMilli(),
		IngestURL:      "https://edge.local/ingest",
		IngestToken:    "ingest-token-1",
		TraceID:        "trace-1",
	}
}

func startNATSServer(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server binary not found")
	}
	port := freePort(t)
	cmd := exec.Command(path, "-a", "127.0.0.1", "-p", strconv.Itoa(port))
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start nats-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := nats.Connect(url, nats.Timeout(100*time.Millisecond), nats.RetryOnFailedConnect(false))
		if err == nil {
			conn.Close()
			return url
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("nats-server exited early: %s", stderr.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("nats-server did not become ready: %s", stderr.String())
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate local port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func sanitizeSubjectToken(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return strings.Trim(b.String(), "-")
}
