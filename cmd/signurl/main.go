package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/terion-name/air3/internal/signing"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, time.Now))
}

func run(args []string, stdout, stderr io.Writer, now func() time.Time) int {
	fs := flag.NewFlagSet("signurl", flag.ContinueOnError)
	fs.SetOutput(stderr)

	method := fs.String("method", "GET", "HTTP method to sign: GET or HEAD")
	baseURL := fs.String("base-url", "http://localhost:8080", "public base URL")
	bucket := fs.String("bucket", "", "S3 bucket name")
	key := fs.String("key", "", "S3 object key")
	secret := fs.String("secret", "", "HMAC signing secret")
	expiresIn := fs.Duration("expiration", 15*time.Minute, "duration until the signed URL expires")
	rangeHeader := fs.String("range", "", "optional byte range claim, for example bytes=0-99")
	responseContentType := fs.String("response-content-type", "", "optional response content type claim")
	responseContentDisposition := fs.String("response-content-disposition", "", "optional response content disposition claim")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *bucket == "" || *key == "" || *secret == "" {
		fmt.Fprintln(stderr, "bucket, key, and secret are required")
		fs.Usage()
		return 2
	}
	if *expiresIn <= 0 {
		fmt.Fprintln(stderr, "expiration must be positive")
		return 2
	}

	raw, err := signing.SignURL(signing.SignInput{
		Method:                     *method,
		BaseURL:                    *baseURL,
		Bucket:                     *bucket,
		Key:                        *key,
		Range:                      *rangeHeader,
		ResponseContentType:        *responseContentType,
		ResponseContentDisposition: *responseContentDisposition,
		Expires:                    now().Add(*expiresIn),
		Secret:                     *secret,
	})
	if err != nil {
		fmt.Fprintf(stderr, "signurl: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, raw)
	return 0
}
