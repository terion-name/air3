package signing

import (
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"
)

type edgeSigningVector struct {
	Name  string `json:"name"`
	Input struct {
		Method                     string `json:"method"`
		BaseURL                    string `json:"baseUrl"`
		Bucket                     string `json:"bucket"`
		Key                        string `json:"key"`
		Secret                     string `json:"secret"`
		Expires                    int64  `json:"expires"`
		Range                      string `json:"range"`
		ResponseContentType        string `json:"responseContentType"`
		ResponseContentDisposition string `json:"responseContentDisposition"`
	} `json:"input"`
	Now         int64  `json:"now"`
	Canonical   string `json:"canonical"`
	Signature   string `json:"signature"`
	URL         string `json:"url"`
	VerifyRange string `json:"verifyRange"`
}

func TestEdgeSigningVectors(t *testing.T) {
	vectors := loadEdgeSigningVectors(t)
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			expires := time.Unix(vector.Input.Expires, 0).UTC()
			claims := Claims{
				Method:                     vector.Input.Method,
				Bucket:                     vector.Input.Bucket,
				Key:                        vector.Input.Key,
				Expires:                    expires,
				Range:                      vector.Input.Range,
				ResponseContentType:        vector.Input.ResponseContentType,
				ResponseContentDisposition: vector.Input.ResponseContentDisposition,
			}
			if got := canonicalString(claims); got != vector.Canonical {
				t.Fatalf("canonicalString() = %q, want %q", got, vector.Canonical)
			}

			raw, err := SignURL(SignInput{
				Method:                     vector.Input.Method,
				BaseURL:                    vector.Input.BaseURL,
				Bucket:                     vector.Input.Bucket,
				Key:                        vector.Input.Key,
				Secret:                     vector.Input.Secret,
				Expires:                    expires,
				Range:                      vector.Input.Range,
				ResponseContentType:        vector.Input.ResponseContentType,
				ResponseContentDisposition: vector.Input.ResponseContentDisposition,
			})
			if err != nil {
				t.Fatalf("SignURL() error = %v", err)
			}
			if raw != vector.URL {
				t.Fatalf("SignURL() = %q, want %q", raw, vector.URL)
			}
			u, err := url.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := u.Query().Get(ParamSig); got != vector.Signature {
				t.Fatalf("signature = %q, want %q", got, vector.Signature)
			}

			gotClaims, err := ValidateURL(vector.Input.Method, raw, ValidationConfig{Secret: vector.Input.Secret}, time.Unix(vector.Now, 0).UTC())
			if err != nil {
				t.Fatalf("ValidateURL() error = %v", err)
			}
			if gotClaims.Bucket != vector.Input.Bucket || gotClaims.Key != vector.Input.Key || gotClaims.Range != vector.Input.Range {
				t.Fatalf("claims = %#v", gotClaims)
			}
		})
	}
}

func loadEdgeSigningVectors(t *testing.T) []edgeSigningVector {
	t.Helper()
	data, err := os.ReadFile("../../packages/testdata/edge-signing-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []edgeSigningVector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}
