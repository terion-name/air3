package mtls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServerConfigRequiresAndVerifiesClientCerts(t *testing.T) {
	files := writeTestPKI(t)
	cfg, err := ServerConfig(ServerOptions{Files: Files{CAFile: files.ca, CertFile: files.serverCert, KeyFile: files.serverKey}, RequireClientCert: true})
	if err != nil {
		t.Fatalf("ServerConfig() error = %v", err)
	}
	if cfg.ClientAuth == 0 || cfg.ClientCAs == nil || len(cfg.Certificates) != 1 {
		t.Fatalf("ServerConfig() = %#v", cfg)
	}
}

func TestServerConfigRejectsMissingCAWhenClientCertRequired(t *testing.T) {
	files := writeTestPKI(t)
	if _, err := ServerConfig(ServerOptions{Files: Files{CertFile: files.serverCert, KeyFile: files.serverKey}, RequireClientCert: true}); err == nil {
		t.Fatal("ServerConfig() error = nil, want missing CA error")
	}
}

func TestClientConfigLoadsCAAndCertificate(t *testing.T) {
	files := writeTestPKI(t)
	cfg, err := ClientConfig(ClientOptions{Files: Files{CAFile: files.ca, CertFile: files.clientCert, KeyFile: files.clientKey, ServerName: "edge.local"}})
	if err != nil {
		t.Fatalf("ClientConfig() error = %v", err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 || cfg.ServerName != "edge.local" {
		t.Fatalf("ClientConfig() = %#v", cfg)
	}
}

func TestClientConfigRejectsMismatchedCertAndKeyConfig(t *testing.T) {
	files := writeTestPKI(t)
	if _, err := ClientConfig(ClientOptions{Files: Files{CertFile: files.clientCert}}); err == nil {
		t.Fatal("ClientConfig() error = nil, want mismatched cert/key error")
	}
}

func TestConfigRejectsMissingAndInvalidFiles(t *testing.T) {
	if _, err := ClientConfig(ClientOptions{Files: Files{CAFile: filepath.Join(t.TempDir(), "missing.pem")}}); err == nil {
		t.Fatal("ClientConfig() error = nil, want missing CA error")
	}
	badCA := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ClientConfig(ClientOptions{Files: Files{CAFile: badCA}}); err == nil {
		t.Fatal("ClientConfig() error = nil, want invalid CA error")
	}
}

type testFiles struct {
	ca         string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

func writeTestPKI(t *testing.T) testFiles {
	t.Helper()
	dir := t.TempDir()
	caKey, caCertDER := createCA(t)
	caPath := filepath.Join(dir, "ca.pem")
	writePEM(t, caPath, "CERTIFICATE", caCertDER)

	serverCert, serverKey := createLeaf(t, caKey, caCertDER, "edge.local")
	serverCertPath := filepath.Join(dir, "server.pem")
	serverKeyPath := filepath.Join(dir, "server-key.pem")
	writePEM(t, serverCertPath, "CERTIFICATE", serverCert)
	writeKey(t, serverKeyPath, serverKey)

	clientCert, clientKey := createLeaf(t, caKey, caCertDER, "connector.local")
	clientCertPath := filepath.Join(dir, "client.pem")
	clientKeyPath := filepath.Join(dir, "client-key.pem")
	writePEM(t, clientCertPath, "CERTIFICATE", clientCert)
	writeKey(t, clientKeyPath, clientKey)

	return testFiles{ca: caPath, serverCert: serverCertPath, serverKey: serverKeyPath, clientCert: clientCertPath, clientKey: clientKeyPath}
}

func createCA(t *testing.T) (*rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	cert, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return key, cert
}

func createLeaf(t *testing.T, caKey *rsa.PrivateKey, caDER []byte, name string) ([]byte, *rsa.PrivateKey) {
	t.Helper()
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	cert, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeKey(t *testing.T, path string, key *rsa.PrivateKey) {
	t.Helper()
	writePEM(t, path, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}
