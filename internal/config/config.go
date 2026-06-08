package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type LookupFunc func(string) (string, bool)
type FileExistsFunc func(string) bool

type Options struct {
	Lookup     LookupFunc
	FileExists FileExistsFunc
}

type EdgeConfig struct {
	PublicListenAddr string
	IngestListenAddr string
	IngestURL        string
	AllowedBuckets   []string
	NATS             NATSConfig
	Signing          SigningConfig
	MTLS             MTLSPaths
	Timeouts         TimeoutConfig
}

type ConnectorConfig struct {
	IngestURL      string
	AllowedBuckets []string
	NATS           NATSConfig
	S3             S3Config
	MTLS           MTLSPaths
	Timeouts       TimeoutConfig
}

type NATSConfig struct {
	URL        string
	Subject    string
	QueueGroup string
	TLS        MTLSPaths
	CredsFile  string
	NKeyFile   string
	User       string
	Password   string
}

type S3Config struct {
	Endpoint           string
	Region             string
	AllowedBuckets     []string
	AccessKeyID        string
	SecretAccessKey    string
	UsePathStyle       bool
	InsecureSkipVerify bool
}

type SigningConfig struct {
	Secret   string
	TTL      time.Duration
	Disabled bool
}

type MTLSPaths struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

type TimeoutConfig struct {
	PendingRequestTTL time.Duration
	StreamTimeout     time.Duration
}

func LoadEdgeFromEnv() (EdgeConfig, error) {
	return LoadEdge(Options{})
}

func LoadConnectorFromEnv() (ConnectorConfig, error) {
	return LoadConnector(Options{})
}

func LoadEdge(opts Options) (EdgeConfig, error) {
	env := normalizedOptions(opts)
	cfg := EdgeConfig{
		PublicListenAddr: env.get("AIR3_EDGE_PUBLIC_ADDR", ":8080"),
		IngestListenAddr: env.get("AIR3_EDGE_INGEST_ADDR", ":8443"),
		IngestURL:        env.get("AIR3_INGEST_URL", "https://localhost:8443/ingest"),
	}
	var err error
	cfg.AllowedBuckets, err = env.list("AIR3_ALLOWED_BUCKETS", "demo")
	if err != nil {
		return EdgeConfig{}, err
	}
	cfg.NATS, err = loadNATS(env, true)
	if err != nil {
		return EdgeConfig{}, err
	}
	cfg.Signing, err = loadSigning(env)
	if err != nil {
		return EdgeConfig{}, err
	}
	cfg.MTLS, err = loadMTLS(env, "AIR3_EDGE_MTLS")
	if err != nil {
		return EdgeConfig{}, err
	}
	cfg.Timeouts, err = loadTimeouts(env)
	if err != nil {
		return EdgeConfig{}, err
	}
	if cfg.PublicListenAddr == "" || cfg.IngestListenAddr == "" || cfg.IngestURL == "" {
		return EdgeConfig{}, errors.New("edge listener addresses and ingest url are required")
	}
	return cfg, nil
}

func LoadConnector(opts Options) (ConnectorConfig, error) {
	env := normalizedOptions(opts)
	cfg := ConnectorConfig{
		IngestURL: env.get("AIR3_INGEST_URL", "https://localhost:8443/ingest"),
	}
	var err error
	cfg.AllowedBuckets, err = env.list("AIR3_ALLOWED_BUCKETS", "demo")
	if err != nil {
		return ConnectorConfig{}, err
	}
	cfg.NATS, err = loadNATS(env, false)
	if err != nil {
		return ConnectorConfig{}, err
	}
	cfg.S3, err = loadS3(env)
	if err != nil {
		return ConnectorConfig{}, err
	}
	cfg.MTLS, err = loadMTLS(env, "AIR3_CONNECTOR_MTLS")
	if err != nil {
		return ConnectorConfig{}, err
	}
	cfg.Timeouts, err = loadTimeouts(env)
	if err != nil {
		return ConnectorConfig{}, err
	}
	if cfg.IngestURL == "" {
		return ConnectorConfig{}, errors.New("ingest url is required")
	}
	return cfg, nil
}

func loadNATS(env envReader, edge bool) (NATSConfig, error) {
	queueDefault := "air3-connectors"
	if edge {
		queueDefault = ""
	}
	cfg := NATSConfig{
		URL:        env.get("AIR3_NATS_URL", "nats://localhost:4222"),
		Subject:    env.get("AIR3_NATS_SUBJECT", "air3.tickets"),
		QueueGroup: env.get("AIR3_NATS_QUEUE", queueDefault),
		CredsFile:  env.get("AIR3_NATS_CREDS_FILE", ""),
		NKeyFile:   env.get("AIR3_NATS_NKEY_FILE", ""),
		User:       env.get("AIR3_NATS_USER", ""),
		Password:   env.get("AIR3_NATS_PASSWORD", ""),
	}
	var err error
	cfg.TLS, err = loadMTLS(env, "AIR3_NATS_TLS")
	if err != nil {
		return NATSConfig{}, err
	}
	if cfg.URL == "" || cfg.Subject == "" {
		return NATSConfig{}, errors.New("nats url and subject are required")
	}
	if (cfg.User == "") != (cfg.Password == "") {
		return NATSConfig{}, errors.New("nats user and password must be configured together")
	}
	if err := env.requireFile("AIR3_NATS_CREDS_FILE", cfg.CredsFile); err != nil {
		return NATSConfig{}, err
	}
	if err := env.requireFile("AIR3_NATS_NKEY_FILE", cfg.NKeyFile); err != nil {
		return NATSConfig{}, err
	}
	return cfg, nil
}

func loadS3(env envReader) (S3Config, error) {
	buckets, err := env.list("AIR3_S3_ALLOWED_BUCKETS", strings.Join(mustList(env, "AIR3_ALLOWED_BUCKETS", "demo"), ","))
	if err != nil {
		return S3Config{}, err
	}
	usePathStyle, err := env.bool("AIR3_S3_USE_PATH_STYLE", true)
	if err != nil {
		return S3Config{}, err
	}
	insecure, err := env.bool("AIR3_S3_INSECURE_SKIP_VERIFY", false)
	if err != nil {
		return S3Config{}, err
	}
	cfg := S3Config{
		Endpoint:           env.get("AIR3_S3_ENDPOINT", "http://localhost:7070"),
		Region:             env.get("AIR3_S3_REGION", "us-east-1"),
		AllowedBuckets:     buckets,
		AccessKeyID:        env.get("AIR3_S3_ACCESS_KEY_ID", ""),
		SecretAccessKey:    env.get("AIR3_S3_SECRET_ACCESS_KEY", ""),
		UsePathStyle:       usePathStyle,
		InsecureSkipVerify: insecure,
	}
	if cfg.Endpoint == "" || cfg.Region == "" {
		return S3Config{}, errors.New("s3 endpoint and region are required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return S3Config{}, errors.New("connector s3 access key id and secret access key are required")
	}
	return cfg, nil
}

func loadSigning(env envReader) (SigningConfig, error) {
	disabled, err := env.bool("AIR3_SIGNING_DISABLED", false)
	if err != nil {
		return SigningConfig{}, err
	}
	ttl, err := env.duration("AIR3_SIGNING_TTL", 15*time.Minute)
	if err != nil {
		return SigningConfig{}, err
	}
	cfg := SigningConfig{
		Secret:   env.get("AIR3_SIGNING_SECRET", ""),
		TTL:      ttl,
		Disabled: disabled,
	}
	if !cfg.Disabled && cfg.Secret == "" {
		return SigningConfig{}, errors.New("signing secret is required unless signing is disabled")
	}
	return cfg, nil
}

func loadTimeouts(env envReader) (TimeoutConfig, error) {
	pending, err := env.duration("AIR3_PENDING_TTL", 30*time.Second)
	if err != nil {
		return TimeoutConfig{}, err
	}
	stream, err := env.duration("AIR3_STREAM_TIMEOUT", 5*time.Minute)
	if err != nil {
		return TimeoutConfig{}, err
	}
	return TimeoutConfig{PendingRequestTTL: pending, StreamTimeout: stream}, nil
}

func loadMTLS(env envReader, prefix string) (MTLSPaths, error) {
	cfg := MTLSPaths{
		CAFile:     env.get(prefix+"_CA_FILE", ""),
		CertFile:   env.get(prefix+"_CERT_FILE", ""),
		KeyFile:    env.get(prefix+"_KEY_FILE", ""),
		ServerName: env.get(prefix+"_SERVER_NAME", ""),
	}
	var err error
	cfg.InsecureSkipVerify, err = env.bool(prefix+"_INSECURE_SKIP_VERIFY", false)
	if err != nil {
		return MTLSPaths{}, err
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return MTLSPaths{}, fmt.Errorf("%s cert file and key file must be configured together", prefix)
	}
	for name, path := range map[string]string{
		prefix + "_CA_FILE":   cfg.CAFile,
		prefix + "_CERT_FILE": cfg.CertFile,
		prefix + "_KEY_FILE":  cfg.KeyFile,
	} {
		if err := env.requireFile(name, path); err != nil {
			return MTLSPaths{}, err
		}
	}
	return cfg, nil
}

type envReader struct {
	lookup     LookupFunc
	fileExists FileExistsFunc
}

func normalizedOptions(opts Options) envReader {
	lookup := opts.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	fileExists := opts.FileExists
	if fileExists == nil {
		fileExists = func(path string) bool {
			info, err := os.Stat(path)
			return err == nil && !info.IsDir()
		}
	}
	return envReader{lookup: lookup, fileExists: fileExists}
}

func (e envReader) get(name, fallback string) string {
	if value, ok := e.lookup(name); ok {
		return strings.TrimSpace(value)
	}
	return fallback
}

func (e envReader) duration(name string, fallback time.Duration) (time.Duration, error) {
	text := e.get(name, "")
	if text == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(text)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", name, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return d, nil
}

func (e envReader) bool(name string, fallback bool) (bool, error) {
	text := e.get(name, "")
	if text == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(text)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return v, nil
}

func (e envReader) list(name, fallback string) ([]string, error) {
	text := e.get(name, fallback)
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("%s must contain at least one bucket", name)
	}
	parts := strings.Split(text, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, part := range parts {
		bucket := strings.TrimSpace(part)
		if bucket == "" {
			return nil, fmt.Errorf("%s contains an empty entry", name)
		}
		if err := validateBucket(bucket); err != nil {
			return nil, fmt.Errorf("%s contains invalid bucket %q: %w", name, bucket, err)
		}
		if !seen[bucket] {
			out = append(out, bucket)
			seen[bucket] = true
		}
	}
	return out, nil
}

func (e envReader) requireFile(name, path string) error {
	if path == "" {
		return nil
	}
	if !e.fileExists(path) {
		return fmt.Errorf("%s file does not exist: %s", name, path)
	}
	return nil
}

func mustList(env envReader, name, fallback string) []string {
	values, err := env.list(name, fallback)
	if err != nil {
		return []string{fallback}
	}
	return values
}

func validateBucket(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 63 {
		return errors.New("bucket length must be 3-63")
	}
	for _, r := range bucket {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			continue
		}
		return errors.New("bucket must contain lowercase letters, numbers, dots, or hyphens")
	}
	if strings.HasPrefix(bucket, ".") || strings.HasPrefix(bucket, "-") || strings.HasSuffix(bucket, ".") || strings.HasSuffix(bucket, "-") || strings.Contains(bucket, "..") {
		return errors.New("bucket has invalid DNS label placement")
	}
	return nil
}
