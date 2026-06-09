package ingest

import (
	"crypto/x509"
	"errors"
	"strings"
)

// ConnectorAuthorizer authorizes connector client certificate identities.
// A zero-value authorizer allows any peer, matching HTTP ingest's default.
type ConnectorAuthorizer struct {
	allowedIdentities map[string]struct{}
}

// NewConnectorAuthorizer builds an authorizer from exact-match certificate
// identities. Empty and whitespace-only entries are ignored.
func NewConnectorAuthorizer(allowed []string) ConnectorAuthorizer {
	identities := make(map[string]struct{}, len(allowed))
	for _, identity := range allowed {
		identity = strings.TrimSpace(identity)
		if identity != "" {
			identities[identity] = struct{}{}
		}
	}
	return ConnectorAuthorizer{allowedIdentities: identities}
}

// AuthorizePeerCertificates accepts a peer when no connector identity allowlist
// is configured, or when the leaf certificate has an exact matching identity.
func (a ConnectorAuthorizer) AuthorizePeerCertificates(certs []*x509.Certificate) error {
	if len(a.allowedIdentities) == 0 {
		return nil
	}
	if len(certs) == 0 || certs[0] == nil {
		return errors.New("missing connector client certificate")
	}
	for _, identity := range certificateIdentities(certs[0]) {
		if _, ok := a.allowedIdentities[identity]; ok {
			return nil
		}
	}
	return errors.New("connector client certificate identity is not allowed")
}

func certificateIdentities(cert *x509.Certificate) []string {
	identities := make([]string, 0, 1+len(cert.DNSNames)+len(cert.EmailAddresses)+len(cert.URIs))
	if cert.Subject.CommonName != "" {
		identities = append(identities, cert.Subject.CommonName)
	}
	identities = append(identities, cert.DNSNames...)
	identities = append(identities, cert.EmailAddresses...)
	for _, uri := range cert.URIs {
		identities = append(identities, uri.String())
	}
	return identities
}
