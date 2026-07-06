package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClassifyMultipartOperations(t *testing.T) {
	singleServer := ClassifyOptions{Mode: RoutingSingleServer}
	multiServer := ClassifyOptions{Mode: RoutingMultiServer}

	tests := []struct {
		name    string
		method  string
		target  string
		opts    ClassifyOptions
		want    RequestMapping
		wantErr string
	}{
		{
			name:   "create",
			method: http.MethodPost,
			target: "/demo-bucket/objects/file.bin?uploads",
			opts:   singleServer,
			want: RequestMapping{
				Operation:     OperationCreateMultipartUpload,
				S3Bucket:      "demo-bucket",
				S3Key:         "objects/file.bin",
				BackendBucket: "demo-bucket",
				BackendKey:    "objects/file.bin",
			},
		},
		{
			name:   "upload part",
			method: http.MethodPut,
			target: "/demo-bucket/objects/file.bin?partNumber=7&uploadId=upload-1",
			opts:   singleServer,
			want: RequestMapping{
				Operation:     OperationUploadPart,
				S3Bucket:      "demo-bucket",
				S3Key:         "objects/file.bin",
				BackendBucket: "demo-bucket",
				BackendKey:    "objects/file.bin",
				Multipart:     MultipartMapping{UploadID: "upload-1", PartNumber: 7},
			},
		},
		{
			name:   "complete",
			method: http.MethodPost,
			target: "/demo-bucket/objects/file.bin?uploadId=upload-1",
			opts:   singleServer,
			want: RequestMapping{
				Operation:     OperationCompleteMultipartUpload,
				S3Bucket:      "demo-bucket",
				S3Key:         "objects/file.bin",
				BackendBucket: "demo-bucket",
				BackendKey:    "objects/file.bin",
				Multipart:     MultipartMapping{UploadID: "upload-1"},
			},
		},
		{
			name:   "abort",
			method: http.MethodDelete,
			target: "/demo-bucket/objects/file.bin?uploadId=upload-1",
			opts:   singleServer,
			want: RequestMapping{
				Operation:     OperationAbortMultipartUpload,
				S3Bucket:      "demo-bucket",
				S3Key:         "objects/file.bin",
				BackendBucket: "demo-bucket",
				BackendKey:    "objects/file.bin",
				Multipart:     MultipartMapping{UploadID: "upload-1"},
			},
		},
		{
			name:   "multiserver standard topology upload part",
			method: http.MethodPut,
			target: "/blue/demo-bucket/objects/file.bin?partNumber=2&uploadId=upload-1",
			opts:   multiServer,
			want: RequestMapping{
				Operation:     OperationUploadPart,
				Server:        "blue",
				S3Bucket:      "blue",
				S3Key:         "demo-bucket/objects/file.bin",
				BackendBucket: "demo-bucket",
				BackendKey:    "objects/file.bin",
				Multipart:     MultipartMapping{UploadID: "upload-1", PartNumber: 2},
			},
		},
		{name: "plain post is unsupported", method: http.MethodPost, target: "/demo-bucket/objects/file.bin", opts: singleServer, wantErr: "unsupported method"},
		{name: "uploads with uploadId conflicts", method: http.MethodPost, target: "/demo-bucket/k?uploads&uploadId=x", opts: singleServer, wantErr: "conflicts"},
		{name: "part without uploadId", method: http.MethodPut, target: "/demo-bucket/k?partNumber=1", opts: singleServer, wantErr: "uploadId"},
		{name: "part number zero", method: http.MethodPut, target: "/demo-bucket/k?partNumber=0&uploadId=x", opts: singleServer, wantErr: "partNumber"},
		{name: "part number too large", method: http.MethodPut, target: "/demo-bucket/k?partNumber=10001&uploadId=x", opts: singleServer, wantErr: "partNumber"},
		{name: "part number not an integer", method: http.MethodPut, target: "/demo-bucket/k?partNumber=abc&uploadId=x", opts: singleServer, wantErr: "partNumber"},
		{name: "complete with empty uploadId", method: http.MethodPost, target: "/demo-bucket/k?uploadId=", opts: singleServer, wantErr: "uploadId"},
		{name: "create on bucket only", method: http.MethodPost, target: "/demo-bucket?uploads", opts: singleServer, wantErr: "bucket-level"},
		{name: "create with acl query", method: http.MethodPost, target: "/demo-bucket/k?uploads&acl", opts: singleServer, wantErr: "acl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.target, nil)
			got, err := Classify(r, tc.opts)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Classify() error = %v, want error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("Classify() = %#v, want %#v", got, tc.want)
			}
		})
	}
}
