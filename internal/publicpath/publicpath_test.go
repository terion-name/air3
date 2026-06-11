package publicpath

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseEscapedPathSingle(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    Object
		wantErr string
	}{
		{
			name: "bucket and key",
			path: "/photos/cat.jpg",
			want: Object{Bucket: "photos", Key: "cat.jpg"},
		},
		{
			name: "escaped bucket and key segments",
			path: "/my%20bucket/dir%201/file%2Fname.txt",
			want: Object{Bucket: "my bucket", Key: "dir 1/file/name.txt"},
		},
		{
			name:    "missing leading slash",
			path:    "bucket/key",
			wantErr: "leading slash",
		},
		{
			name:    "missing bucket empty path",
			path:    "/",
			wantErr: "missing bucket",
		},
		{
			name:    "empty bucket",
			path:    "//key",
			wantErr: "empty bucket",
		},
		{
			name:    "missing key",
			path:    "/bucket",
			wantErr: "missing key",
		},
		{
			name:    "empty key",
			path:    "/bucket/",
			wantErr: "empty key",
		},
		{
			name:    "bad bucket escape",
			path:    "/bad%ZZ/key",
			wantErr: "bad escape in bucket",
		},
		{
			name:    "bad key escape",
			path:    "/bucket/bad%ZZ",
			wantErr: "bad escape in key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseEscapedPath(tc.path, ModeSingle)
			assertParseResult(t, got, err, tc.want, tc.wantErr)
		})
	}
}

func TestParseEscapedPathMulti(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    Object
		wantErr string
	}{
		{
			name: "server bucket and key",
			path: "/edge-1/photos/cat.jpg",
			want: Object{Server: "edge-1", Bucket: "photos", Key: "cat.jpg"},
		},
		{
			name: "escaped bucket and key segments",
			path: "/server_1/my%20bucket/dir%201/file%2Fname.txt",
			want: Object{Server: "server_1", Bucket: "my bucket", Key: "dir 1/file/name.txt"},
		},
		{
			name:    "missing leading slash",
			path:    "server/bucket/key",
			wantErr: "leading slash",
		},
		{
			name:    "missing server empty path",
			path:    "/",
			wantErr: "missing server",
		},
		{
			name:    "missing server empty segment",
			path:    "//bucket/key",
			wantErr: "missing server",
		},
		{
			name:    "missing bucket",
			path:    "/server",
			wantErr: "missing bucket",
		},
		{
			name:    "empty bucket",
			path:    "/server//key",
			wantErr: "empty bucket",
		},
		{
			name:    "missing key",
			path:    "/server/bucket",
			wantErr: "missing key",
		},
		{
			name:    "empty key",
			path:    "/server/bucket/",
			wantErr: "empty key",
		},
		{
			name:    "bad server escape",
			path:    "/bad%ZZ/bucket/key",
			wantErr: "bad escape in server",
		},
		{
			name:    "invalid server alias",
			path:    "/bad.alias/bucket/key",
			wantErr: "invalid server alias",
		},
		{
			name:    "bad bucket escape",
			path:    "/server/bad%ZZ/key",
			wantErr: "bad escape in bucket",
		},
		{
			name:    "bad key escape",
			path:    "/server/bucket/bad%ZZ",
			wantErr: "bad escape in key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseEscapedPath(tc.path, ModeMulti)
			assertParseResult(t, got, err, tc.want, tc.wantErr)
		})
	}
}

func TestValidateAlias(t *testing.T) {
	valid63 := "a" + strings.Repeat("-", 62)
	tooLong := "a" + strings.Repeat("b", 63)

	tests := []struct {
		name    string
		alias   string
		wantErr bool
	}{
		{name: "single letter", alias: "a"},
		{name: "single digit", alias: "1"},
		{name: "mixed allowed chars", alias: "Edge_1-prod"},
		{name: "max length", alias: valid63},
		{name: "empty", alias: "", wantErr: true},
		{name: "too long", alias: tooLong, wantErr: true},
		{name: "starts with hyphen", alias: "-edge", wantErr: true},
		{name: "starts with underscore", alias: "_edge", wantErr: true},
		{name: "slash separator", alias: "edge/one", wantErr: true},
		{name: "backslash separator", alias: `edge\one`, wantErr: true},
		{name: "dot", alias: "edge.one", wantErr: true},
		{name: "single dot", alias: ".", wantErr: true},
		{name: "dot dot", alias: "..", wantErr: true},
		{name: "space", alias: "edge one", wantErr: true},
		{name: "tab control", alias: "edge\tone", wantErr: true},
		{name: "star wildcard", alias: "edge*", wantErr: true},
		{name: "greater wildcard", alias: "edge>", wantErr: true},
		{name: "non ascii", alias: "é", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAlias(tc.alias)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateAlias(%q) error = %v, wantErr %v", tc.alias, err, tc.wantErr)
			}
		})
	}
}

func TestEnvSuffix(t *testing.T) {
	tests := []struct {
		alias string
		want  string
	}{
		{alias: "foo", want: "FOO"},
		{alias: "Foo", want: "FOO"},
		{alias: "foo-bar", want: "FOO_BAR"},
		{alias: "foo_bar", want: "FOO_BAR"},
		{alias: "edge-1_prod", want: "EDGE_1_PROD"},
	}

	for _, tc := range tests {
		t.Run(tc.alias, func(t *testing.T) {
			if got := EnvSuffix(tc.alias); got != tc.want {
				t.Fatalf("EnvSuffix(%q) = %q, want %q", tc.alias, got, tc.want)
			}
		})
	}
}

func TestEnvSuffixCollisions(t *testing.T) {
	aliases := []string{"foo-bar", "foo_bar", "Foo", "foo", "unique"}
	want := map[string][]string{
		"FOO_BAR": {"foo-bar", "foo_bar"},
		"FOO":     {"Foo", "foo"},
	}

	got := EnvSuffixCollisions(aliases)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("EnvSuffixCollisions(%v) = %#v, want %#v", aliases, got, want)
	}
}

func assertParseResult(t *testing.T, got Object, err error, want Object, wantErr string) {
	t.Helper()

	if wantErr != "" {
		if err == nil {
			t.Fatalf("ParseEscapedPath() error = nil, want containing %q", wantErr)
		}
		if !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("ParseEscapedPath() error = %q, want containing %q", err.Error(), wantErr)
		}
		return
	}
	if err != nil {
		t.Fatalf("ParseEscapedPath() error = %v", err)
	}
	if got != want {
		t.Fatalf("ParseEscapedPath() = %#v, want %#v", got, want)
	}
}
