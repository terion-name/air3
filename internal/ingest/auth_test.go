package ingest

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"testing"
)

func TestConnectorAuthorizerEmptyAllowlistAllowsAnyPeer(t *testing.T) {
	tests := []struct {
		name    string
		allowed []string
		certs   []*x509.Certificate
	}{
		{name: "empty allowlist allows no cert"},
		{name: "whitespace allowlist allows no cert", allowed: []string{" ", "\t"}},
		{name: "empty allowlist allows cert", certs: []*x509.Certificate{certWithCommonName("connector-a")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := NewConnectorAuthorizer(tt.allowed)
			if err := authorizer.AuthorizePeerCertificates(tt.certs); err != nil {
				t.Fatalf("AuthorizePeerCertificates() error = %v, want nil", err)
			}
		})
	}
}

func TestConnectorAuthorizerConfiguredAllowlistRejectsMissingOrWrongCert(t *testing.T) {
	authorizer := NewConnectorAuthorizer([]string{"connector-a"})
	tests := []struct {
		name  string
		certs []*x509.Certificate
	}{
		{name: "nil certs"},
		{name: "empty certs", certs: []*x509.Certificate{}},
		{name: "nil leaf", certs: []*x509.Certificate{nil}},
		{name: "wrong cert", certs: []*x509.Certificate{certWithCommonName("connector-b")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := authorizer.AuthorizePeerCertificates(tt.certs); err == nil {
				t.Fatal("AuthorizePeerCertificates() error = nil, want error")
			}
		})
	}
}

func TestConnectorAuthorizerAcceptsCertificateIdentities(t *testing.T) {
	uri, err := url.Parse("spiffe://air3/connector-a")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	tests := []struct {
		name    string
		allowed string
		cert    *x509.Certificate
	}{
		{
			name:    "common name",
			allowed: "connector-cn",
			cert:    certWithCommonName("connector-cn"),
		},
		{
			name:    "dns san",
			allowed: "connector.example.test",
			cert:    &x509.Certificate{DNSNames: []string{"connector.example.test"}},
		},
		{
			name:    "email san",
			allowed: "connector@example.test",
			cert:    &x509.Certificate{EmailAddresses: []string{"connector@example.test"}},
		},
		{
			name:    "uri san",
			allowed: "spiffe://air3/connector-a",
			cert:    &x509.Certificate{URIs: []*url.URL{uri}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := NewConnectorAuthorizer([]string{" ignored ", " " + tt.allowed + " "})
			if err := authorizer.AuthorizePeerCertificates([]*x509.Certificate{tt.cert}); err != nil {
				t.Fatalf("AuthorizePeerCertificates() error = %v, want nil", err)
			}
		})
	}
}

func certWithCommonName(commonName string) *x509.Certificate {
	return &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
}
