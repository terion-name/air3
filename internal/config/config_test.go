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
	if cfg.StreamCopyBufferBytes != 262144 {
		t.Fatalf("StreamCopyBufferBytes = %d, want 262144", cfg.StreamCopyBufferBytes)
	}
	if cfg.IngestTransport != IngestTransportHTTP || cfg.IngestTCPListenAddr != "" {
		t.Fatalf("ingest transport defaults = %q/%q, want http with no TCP address", cfg.IngestTransport, cfg.IngestTCPListenAddr)
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
		"AIR3_INGEST_DISABLE_HTTP2": "true",
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
	if !cfg.IngestDisableHTTP2 {
		t.Fatal("IngestDisableHTTP2 = false, want true")
	}
	if !reflect.DeepEqual(cfg.S3.AllowedBuckets, []string{"demo", "logs"}) {
		t.Fatalf("S3 buckets = %#v", cfg.S3.AllowedBuckets)
	}
}

func TestLoadConnectorDefaultsIngestDisableHTTP2True(t *testing.T) {
	env := map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":     "access",
		"AIR3_S3_SECRET_ACCESS_KEY": "secret",
	}
	cfg, err := LoadConnector(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadConnector() error = %v", err)
	}
	if !cfg.IngestDisableHTTP2 {
		t.Fatal("IngestDisableHTTP2 = false, want true")
	}
	if cfg.IngestTransport != IngestTransportHTTP || cfg.IngestTCPAddr != "" {
		t.Fatalf("ingest transport defaults = %q/%q, want http with no TCP address", cfg.IngestTransport, cfg.IngestTCPAddr)
	}
}

func TestLoadConnectorCanOptInToIngestHTTP2(t *testing.T) {
	env := map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":     "access",
		"AIR3_S3_SECRET_ACCESS_KEY": "secret",
		"AIR3_INGEST_DISABLE_HTTP2": "false",
	}
	cfg, err := LoadConnector(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadConnector() error = %v", err)
	}
	if cfg.IngestDisableHTTP2 {
		t.Fatal("IngestDisableHTTP2 = true, want false from explicit opt-in")
	}
}

func TestLoadHTTPIngestTransportsDoNotRequireTCPAddress(t *testing.T) {
	for _, transport := range []IngestTransport{IngestTransportHTTP, IngestTransportHTTP1, IngestTransportHTTP2} {
		t.Run(string(transport), func(t *testing.T) {
			edge, err := LoadEdge(testOptions(map[string]string{
				"AIR3_SIGNING_DISABLED": "true",
				"AIR3_INGEST_TRANSPORT": string(transport),
			}, nil))
			if err != nil {
				t.Fatalf("LoadEdge() error = %v", err)
			}
			if edge.IngestTransport != transport || edge.IngestTCPListenAddr != "" {
				t.Fatalf("edge HTTP ingest config = %q/%q, want %s with no TCP address", edge.IngestTransport, edge.IngestTCPListenAddr, transport)
			}
			if !edge.IngestTransport.IsHTTP() || edge.IngestTransport.IsTCP() {
				t.Fatalf("edge transport predicates for %q: IsHTTP=%t IsTCP=%t, want true/false", edge.IngestTransport, edge.IngestTransport.IsHTTP(), edge.IngestTransport.IsTCP())
			}

			connector, err := LoadConnector(testOptions(map[string]string{
				"AIR3_S3_ACCESS_KEY_ID":     "access",
				"AIR3_S3_SECRET_ACCESS_KEY": "secret",
				"AIR3_INGEST_TRANSPORT":     string(transport),
			}, nil))
			if err != nil {
				t.Fatalf("LoadConnector() error = %v", err)
			}
			if connector.IngestTransport != transport || connector.IngestTCPAddr != "" {
				t.Fatalf("connector HTTP ingest config = %q/%q, want %s with no TCP address", connector.IngestTransport, connector.IngestTCPAddr, transport)
			}
			if !connector.IngestTransport.IsHTTP() || connector.IngestTransport.IsTCP() {
				t.Fatalf("connector transport predicates for %q: IsHTTP=%t IsTCP=%t, want true/false", connector.IngestTransport, connector.IngestTransport.IsHTTP(), connector.IngestTransport.IsTCP())
			}
		})
	}
}

func TestLoadEdgeParsesTCPIngestTransport(t *testing.T) {
	env := map[string]string{
		"AIR3_SIGNING_DISABLED":     "true",
		"AIR3_INGEST_TRANSPORT":     "tcp",
		"AIR3_EDGE_INGEST_TCP_ADDR": ":9000",
	}
	cfg, err := LoadEdge(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	if cfg.IngestTransport != IngestTransportTCP || cfg.IngestTCPListenAddr != ":9000" {
		t.Fatalf("edge TCP ingest config = %q/%q, want tcp/:9000", cfg.IngestTransport, cfg.IngestTCPListenAddr)
	}
	if !cfg.IngestTransport.IsTCP() || cfg.IngestTransport.IsHTTP() || !cfg.IngestTransport.UsesTCPIngestAddr() {
		t.Fatalf("edge transport predicates for %q: IsTCP=%t IsHTTP=%t UsesTCPIngestAddr=%t, want true/false/true", cfg.IngestTransport, cfg.IngestTransport.IsTCP(), cfg.IngestTransport.IsHTTP(), cfg.IngestTransport.UsesTCPIngestAddr())
	}
	if cfg.IngestURL != "https://localhost:8443/ingest" {
		t.Fatalf("IngestURL = %q, want default HTTP ingest URL unchanged", cfg.IngestURL)
	}
}

func TestLoadConnectorParsesTCPIngestTransport(t *testing.T) {
	env := map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":     "access",
		"AIR3_S3_SECRET_ACCESS_KEY": "secret",
		"AIR3_INGEST_TRANSPORT":     "tcp",
		"AIR3_INGEST_TCP_ADDR":      "edge.internal:9000",
	}
	cfg, err := LoadConnector(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadConnector() error = %v", err)
	}
	if cfg.IngestTransport != IngestTransportTCP || cfg.IngestTCPAddr != "edge.internal:9000" {
		t.Fatalf("connector TCP ingest config = %q/%q, want tcp/edge.internal:9000", cfg.IngestTransport, cfg.IngestTCPAddr)
	}
	if !cfg.IngestTransport.IsTCP() || cfg.IngestTransport.IsHTTP() || !cfg.IngestTransport.UsesTCPIngestAddr() {
		t.Fatalf("connector transport predicates for %q: IsTCP=%t IsHTTP=%t UsesTCPIngestAddr=%t, want true/false/true", cfg.IngestTransport, cfg.IngestTransport.IsTCP(), cfg.IngestTransport.IsHTTP(), cfg.IngestTransport.UsesTCPIngestAddr())
	}
	if cfg.IngestURL != "https://localhost:8443/ingest" {
		t.Fatalf("IngestURL = %q, want default HTTP ingest URL unchanged", cfg.IngestURL)
	}
}

func TestLoadEdgeParsesSMUXIngestTransport(t *testing.T) {
	env := map[string]string{
		"AIR3_SIGNING_DISABLED":     "true",
		"AIR3_INGEST_TRANSPORT":     "smux",
		"AIR3_EDGE_INGEST_TCP_ADDR": ":9000",
	}
	cfg, err := LoadEdge(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	if cfg.IngestTransport != IngestTransportSMUX || cfg.IngestTCPListenAddr != ":9000" {
		t.Fatalf("edge smux ingest config = %q/%q, want smux/:9000", cfg.IngestTransport, cfg.IngestTCPListenAddr)
	}
	if cfg.IngestTransport.IsTCP() || cfg.IngestTransport.IsHTTP() || !cfg.IngestTransport.UsesTCPIngestAddr() {
		t.Fatalf("edge transport predicates for %q: IsTCP=%t IsHTTP=%t UsesTCPIngestAddr=%t, want false/false/true", cfg.IngestTransport, cfg.IngestTransport.IsTCP(), cfg.IngestTransport.IsHTTP(), cfg.IngestTransport.UsesTCPIngestAddr())
	}
	if cfg.IngestURL != "https://localhost:8443/ingest" {
		t.Fatalf("IngestURL = %q, want default HTTP ingest URL unchanged", cfg.IngestURL)
	}
}

func TestLoadConnectorParsesSMUXIngestTransport(t *testing.T) {
	env := map[string]string{
		"AIR3_S3_ACCESS_KEY_ID":     "access",
		"AIR3_S3_SECRET_ACCESS_KEY": "secret",
		"AIR3_INGEST_TRANSPORT":     "smux",
		"AIR3_INGEST_TCP_ADDR":      "edge.internal:9000",
	}
	cfg, err := LoadConnector(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadConnector() error = %v", err)
	}
	if cfg.IngestTransport != IngestTransportSMUX || cfg.IngestTCPAddr != "edge.internal:9000" {
		t.Fatalf("connector smux ingest config = %q/%q, want smux/edge.internal:9000", cfg.IngestTransport, cfg.IngestTCPAddr)
	}
	if cfg.IngestTransport.IsTCP() || cfg.IngestTransport.IsHTTP() || !cfg.IngestTransport.UsesTCPIngestAddr() {
		t.Fatalf("connector transport predicates for %q: IsTCP=%t IsHTTP=%t UsesTCPIngestAddr=%t, want false/false/true", cfg.IngestTransport, cfg.IngestTransport.IsTCP(), cfg.IngestTransport.IsHTTP(), cfg.IngestTransport.UsesTCPIngestAddr())
	}
	if cfg.IngestURL != "https://localhost:8443/ingest" {
		t.Fatalf("IngestURL = %q, want default HTTP ingest URL unchanged", cfg.IngestURL)
	}
}

func TestLoadIngestTransportRejectsInvalidValue(t *testing.T) {
	tests := []struct {
		name string
		load func(map[string]string) error
	}{
		{
			name: "edge",
			load: func(env map[string]string) error {
				env["AIR3_SIGNING_DISABLED"] = "true"
				_, err := LoadEdge(testOptions(env, nil))
				return err
			},
		},
		{
			name: "connector",
			load: func(env map[string]string) error {
				_, err := LoadConnector(testOptions(env, nil))
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.load(map[string]string{"AIR3_INGEST_TRANSPORT": "grpc"})
			if err == nil || !strings.Contains(err.Error(), "AIR3_INGEST_TRANSPORT") {
				t.Fatalf("load error = %v, want invalid AIR3_INGEST_TRANSPORT allowed-values error", err)
			}
			for _, want := range []string{"http1", "http2", "tcp", "smux", "legacy http"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("load error = %v, want containing %q", err, want)
				}
			}
		})
	}
}

func TestLoadTCPBasedIngestTransportsRequireTCPAddress(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		load func(map[string]string) error
		want string
	}{
		{
			name: "edge tcp",
			env: map[string]string{
				"AIR3_SIGNING_DISABLED": "true",
				"AIR3_INGEST_TRANSPORT": "tcp",
			},
			load: func(env map[string]string) error {
				_, err := LoadEdge(testOptions(env, nil))
				return err
			},
			want: "AIR3_EDGE_INGEST_TCP_ADDR",
		},
		{
			name: "edge smux",
			env: map[string]string{
				"AIR3_SIGNING_DISABLED": "true",
				"AIR3_INGEST_TRANSPORT": "smux",
			},
			load: func(env map[string]string) error {
				_, err := LoadEdge(testOptions(env, nil))
				return err
			},
			want: "AIR3_EDGE_INGEST_TCP_ADDR",
		},
		{
			name: "connector tcp",
			env: map[string]string{
				"AIR3_S3_ACCESS_KEY_ID":     "access",
				"AIR3_S3_SECRET_ACCESS_KEY": "secret",
				"AIR3_INGEST_TRANSPORT":     "tcp",
			},
			load: func(env map[string]string) error {
				_, err := LoadConnector(testOptions(env, nil))
				return err
			},
			want: "AIR3_INGEST_TCP_ADDR",
		},
		{
			name: "connector smux",
			env: map[string]string{
				"AIR3_S3_ACCESS_KEY_ID":     "access",
				"AIR3_S3_SECRET_ACCESS_KEY": "secret",
				"AIR3_INGEST_TRANSPORT":     "smux",
			},
			load: func(env map[string]string) error {
				_, err := LoadConnector(testOptions(env, nil))
				return err
			},
			want: "AIR3_INGEST_TCP_ADDR",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.load(tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("load error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadEdgeParsesStreamCopyBufferBytes(t *testing.T) {
	env := map[string]string{
		"AIR3_SIGNING_DISABLED":         "true",
		"AIR3_STREAM_COPY_BUFFER_BYTES": "524288",
	}
	cfg, err := LoadEdge(testOptions(env, nil))
	if err != nil {
		t.Fatalf("LoadEdge() error = %v", err)
	}
	if cfg.StreamCopyBufferBytes != 524288 {
		t.Fatalf("StreamCopyBufferBytes = %d, want 524288", cfg.StreamCopyBufferBytes)
	}
}

func TestLoadEdgeRejectsInvalidStreamCopyBufferBytes(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"non-number", "many", "AIR3_STREAM_COPY_BUFFER_BYTES must be an integer byte count"},
		{"zero", "0", "AIR3_STREAM_COPY_BUFFER_BYTES must be between 32768 and 1048576 bytes"},
		{"negative", "-1", "AIR3_STREAM_COPY_BUFFER_BYTES must be between 32768 and 1048576 bytes"},
		{"below minimum", "32767", "AIR3_STREAM_COPY_BUFFER_BYTES must be between 32768 and 1048576 bytes"},
		{"above maximum", "1048577", "AIR3_STREAM_COPY_BUFFER_BYTES must be between 32768 and 1048576 bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{
				"AIR3_SIGNING_DISABLED":         "true",
				"AIR3_STREAM_COPY_BUFFER_BYTES": tc.value,
			}
			_, err := LoadEdge(testOptions(env, nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadEdge() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadConnectorRejectsInvalidIngestDisableHTTP2(t *testing.T) {
	env := map[string]string{"AIR3_INGEST_DISABLE_HTTP2": "sometimes"}
	_, err := LoadConnector(testOptions(env, nil))
	if err == nil || !strings.Contains(err.Error(), "AIR3_INGEST_DISABLE_HTTP2 must be a boolean") {
		t.Fatalf("LoadConnector() error = %v, want boolean error", err)
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
