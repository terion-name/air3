package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadEdgeDefaultsWithDisabledSigning(t *testing.T) {
	env := map[string]string{"AIR3_SIGNING_DISABLED": "true"}
	cfg, err := LoadEdge(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	if cfg.PublicListenAddr != ":8080" || cfg.IngestListenAddr != ":8443" || cfg.NATS.URL != "nats://localhost:4222" {
		t.Fatalf("LoadEdge() defaults = %#v", cfg)
	}
	if !cfg.Signing.Disabled || cfg.Signing.TTL != 15*time.Minute {
		t.Fatalf("LoadEdge() signing = %#v", cfg.Signing)
	}
	if !reflect.DeepEqual(cfg.AllowedBuckets, []string{"demo"}) {
		t.Fatalf("AllowedBuckets = %#v", cfg.AllowedBuckets)
	}
}

func TestLoadEdgeRequiresSigningSecretWhenEnabled(t *testing.T) {
	if _, err := LoadEdge(testOptions(nil, nil)); err == nil || !strings.Contains(err.Error(), "signing secret") {
		t.Fatalf("LoadEdge() error = %v, want signing secret error", err)
	}
}

func TestLoadConnectorParsesS3AndDoesNotRequireSigning(t *testing.T) {
	env := map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":     "access",
		"AIR3_S3_SECRET_ACCESS_KEY": "secret",
		"AIR3_ALLOWED_BUCKETS":      "demo,logs",
		"AIR3_S3_USE_PATH_STYLE":    "false",
	}
	cfg, err := LoadConnector(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadConnector() error = %v", err)
	}
	if cfg.S3.AccessKeyID != "access" || cfg.S3.SecretAccessKey != "secret" {
		t.Fatalf("S3 credentials not parsed: %#v", cfg.S3)
	}
	if cfg.S3.UsePathStyle {
		t.Fatal("S3 UsePathStyle = true, want false")
	}
	if !reflect.DeepEqual(cfg.S3.AllowedBuckets, []string{"demo", "logs"}) {
		t.Fatalf("S3 buckets = %#v", cfg.S3.AllowedBuckets)
	}
}

func TestLoadEdgeParsesAllowedConnectorIdentities(t *testing.T) {
	env := map[string]string{
		"AIR3_SIGNING_DISABLED":                  "true",
		"AIR3_EDGE_ALLOWED_CONNECTOR_IDENTITIES": "connector-ingest-client, spiffe://air3/demo/connector ,connector-ingest-client",
	}
	cfg, err := LoadEdge(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	want := []string{"connector-ingest-client", "spiffe://air3/demo/connector"}
	if !reflect.DeepEqual(cfg.AllowedConnectorIdentities, want) {
		t.Fatalf("AllowedConnectorIdentities = %#v, want %#v", cfg.AllowedConnectorIdentities, want)
	}
}

func TestLoadNATSUserPassword(t *testing.T) {
	env := map[string]string{
		"AIR3_SIGNING_DISABLED": "true",
		"AIR3_NATS_USER":        "edge",
		"AIR3_NATS_PASSWORD":    "secret",
	}
	cfg, err := LoadEdge(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	if cfg.NATS.User != "edge" || cfg.NATS.Password != "secret" {
		t.Fatalf("NATS user/password not parsed: %#v", cfg.NATS)
	}

	_, err = LoadEdge(testOptions(map[string]string{
		"AIR3_SIGNING_DISABLED": "true",
		"AIR3_NATS_USER":        "edge",
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "user and password") {
		t.Fatalf("LoadEdge() error = %v, want NATS user/password pair error", err)
	}
}

func TestLoadConnectorRequiresS3Credentials(t *testing.T) {
	if _, err := LoadConnector(testOptions(nil, nil)); err == nil || !strings.Contains(err.Error(), "s3 access key") {
		t.Fatalf("LoadConnector() error = %v, want S3 credential error", err)
	}
}

func TestInvalidDurationAndAllowlist(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"duration", map[string]string{"AIR3_SIGNING_DISABLED": "true", "AIR3_PENDING_TTL": "soon"}},
		{"allowlist empty", map[string]string{"AIR3_SIGNING_DISABLED": "true", "AIR3_ALLOWED_BUCKETS": "demo,,logs"}},
		{"allowlist invalid", map[string]string{"AIR3_SIGNING_DISABLED": "true", "AIR3_ALLOWED_BUCKETS": "Demo"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadEdge(testOptions(tc.env, nil)); err == nil {
				t.Fatal("LoadEdge() error = nil, want error")
			}
		})
	}
}

func TestTLSFileValidationHook(t *testing.T) {
	env := map[string]string{
		"AIR3_SIGNING_DISABLED":     "true",
		"AIR3_EDGE_MTLS_CA_FILE":    "/ca.pem",
		"AIR3_EDGE_MTLS_CERT_FILE":  "/cert.pem",
		"AIR3_EDGE_MTLS_KEY_FILE":   "/key.pem",
		"AIR3_NATS_CREDS_FILE":      "/creds",
		"AIR3_NATS_TLS_CERT_FILE":   "/nats-cert.pem",
		"AIR3_NATS_TLS_KEY_FILE":    "/nats-key.pem",
		"AIR3_NATS_TLS_SERVER_NAME": "nats.local",
	}
	files := map[string]bool{"/ca.pem": true, "/cert.pem": true, "/key.pem": true, "/creds": true, "/nats-cert.pem": true, "/nats-key.pem": true}
	cfg, err := LoadEdge(testOptions(env, files))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	if cfg.MTLS.CAFile != "/ca.pem" || cfg.NATS.CredsFile != "/creds" || cfg.NATS.TLS.ServerName != "nats.local" {
		t.Fatalf("TLS paths not parsed: %#v", cfg)
	}

	files["/key.pem"] = false
	if _, err := LoadEdge(testOptions(env, files)); err == nil || !strings.Contains(err.Error(), "KEY_FILE") {
		t.Fatalf("LoadEdge() error = %v, want missing key file error", err)
	}
}

func TestTLSCertKeyMustBeConfiguredTogether(t *testing.T) {
	env := map[string]string{"AIR3_SIGNING_DISABLED": "true", "AIR3_EDGE_MTLS_CERT_FILE": "/cert.pem"}
	if _, err := LoadEdge(testOptions(env, map[string]bool{"/cert.pem": true})); err == nil || !strings.Contains(err.Error(), "together") {
		t.Fatalf("LoadEdge() error = %v, want cert/key pair error", err)
	}
}

func testOptions(values map[string]string, files map[string]bool) Options {
	if values == nil {
		values = map[string]string{}
	}
	return Options{
		Lookup: func(name string) (string, bool) {
			v, ok := values[name]
			return v, ok
		},
		FileExists: func(path string) bool {
			if files == nil {
				return false
			}
			return files[path]
		},
	}
}
