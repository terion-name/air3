package ingesttcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/terion-name/air3/internal/pending"
)

type ClientRequest struct {
	RequestID   string
	IngestToken string
	Metadata    pending.Metadata
	Body        io.Reader
	BodyLength  int64
}

func DialAndSend(ctx context.Context, network, address string, tlsConfig *tls.Config, req ClientRequest) error {
	if tlsConfig == nil {
		return errors.New("TLS config is required")
	}
	dialer := &tls.Dialer{Config: tlsConfig}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return fmt.Errorf("dial ingest TCP server: %w", err)
	}
	defer conn.Close()
	return Send(ctx, conn, req)
}

func Send(ctx context.Context, conn net.Conn, req ClientRequest) error {
	if req.Body == nil {
		req.Body = emptyReader{}
	}
	clearDeadline, err := setDeadlineFromContext(ctx, conn)
	if err != nil {
		return err
	}
	defer clearDeadline()

	header, err := validateHeader(Header{
		RequestID:   req.RequestID,
		IngestToken: req.IngestToken,
		BodyLength:  req.BodyLength,
		Metadata:    req.Metadata,
	})
	if err != nil {
		return err
	}
	if err := EncodeHeader(conn, header); err != nil {
		return err
	}
	if err := copyClientBody(conn, req.Body, header.BodyLength); err != nil {
		return err
	}
	return readAck(ctx, conn)
}

type closeWriter interface {
	CloseWrite() error
}

func copyClientBody(conn net.Conn, body io.Reader, bodyLength int64) error {
	if bodyLength == UnknownBodyLength {
		if _, err := io.Copy(conn, body); err != nil {
			return fmt.Errorf("copy unknown-length ingest body: %w", err)
		}
		cw, ok := conn.(closeWriter)
		if !ok {
			return errors.New("unknown-length ingest body requires a connection with CloseWrite")
		}
		if err := cw.CloseWrite(); err != nil {
			return fmt.Errorf("close ingest TCP write side: %w", err)
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
	stopCancelWatch := make(chan struct{})
	defer close(stopCancelWatch)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-stopCancelWatch:
		}
	}()

	var ack [1]byte
	_, err := io.ReadFull(conn, ack[:])
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("read ingest TCP ack: %w", err)
	}
	if ack[0] != successAck {
		return fmt.Errorf("ingest TCP server returned non-success ack %d", ack[0])
	}
	return nil
}

func setDeadlineFromContext(ctx context.Context, conn net.Conn) (func(), error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return func() {}, nil
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	return func() { _ = conn.SetDeadline(time.Time{}) }, nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }
