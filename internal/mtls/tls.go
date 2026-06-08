package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

type Files struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

type ServerOptions struct {
	Files
	RequireClientCert bool
}

type ClientOptions struct {
	Files
}

func ServerConfig(opts ServerOptions) (*tls.Config, error) {
	cert, err := loadKeyPair(opts.CertFile, opts.KeyFile, true)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if opts.RequireClientCert {
		if opts.CAFile == "" {
			return nil, errors.New("ca file is required when client certificates are required")
		}
		pool, err := loadCertPool(opts.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func ClientConfig(opts ClientOptions) (*tls.Config, error) {
	if (opts.CertFile == "") != (opts.KeyFile == "") {
		return nil, errors.New("cert file and key file must be configured together")
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         opts.ServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // Allowed only when caller explicitly opts in for local demos.
	}
	if opts.CAFile != "" {
		pool, err := loadCertPool(opts.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if opts.CertFile != "" {
		cert, err := loadKeyPair(opts.CertFile, opts.KeyFile, true)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadKeyPair(certFile, keyFile string, required bool) (tls.Certificate, error) {
	if certFile == "" || keyFile == "" {
		if required {
			return tls.Certificate{}, errors.New("cert file and key file are required")
		}
		return tls.Certificate{}, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load certificate key pair: %w", err)
	}
	return cert, nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("ca file contains no PEM certificates")
	}
	return pool, nil
}
