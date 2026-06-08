// Package config loads environment configuration for the edge gateway and
// private connector while preserving the trust boundary: S3 credentials appear
// only in connector configuration, never edge configuration.
package config
