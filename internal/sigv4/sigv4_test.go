package sigv4

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testKey    = "AKIAPAILTEST00000000"
	testSecret = "pail-test-secret"
)

var signer = v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// s3Path encodes a key the way the S3 SDKs put it on the wire.
func s3Path(key string) string { return uriEncode("/"+key, false) }

// signed builds a server-side request for target and signs it like an SDK.
func signed(t *testing.T, method, target, body, payloadHash string, at time.Time, mutate func(*http.Request)) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if mutate != nil {
		mutate(r)
	}
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := signer.SignHTTP(r.Context(), creds, r, payloadHash, "s3", "eu-west-1", at); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestVerifyAcceptsSDKSignaturesOnTheWire(t *testing.T) {
	v := New(testKey, testSecret)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := v.Verify(r); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)

	tests := []struct {
		key, query string
	}{
		{"a//b", ""},
		{"../x", ""},
		{"a+b", ""},
		{"a b", ""},
		{"✓", ""},
		{"dir/", ""},
		{"100%", ""},
		{"a%2Fb", ""},
		{"k", "list-type=2&prefix=a%2Fb&delimiter=%2F"},
		{"k", "uploads"},
		{"k", "response-content-type=text%2Fplain&x-id=GetObject"},
		{"k", "a=2&a=1&b=%20+"},
		{"", "location"},
	}
	for _, tt := range tests {
		const body = "payload"
		u := &url.URL{Scheme: "http", Host: strings.TrimPrefix(srv.URL, "http://"), Path: "/bkt/" + tt.key, RawPath: s3Path("bkt/" + tt.key), RawQuery: tt.query}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, u.String(), strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Amz-Content-Sha256", sha256Hex(body))
		req.Header.Set("X-Amz-Meta-Color", "  blue   and  green ")
		creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
		if err := signer.SignHTTP(req.Context(), creds, req, sha256Hex(body), "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("PUT key %q query %q = %d %s, want 200", tt.key, tt.query, resp.StatusCode, msg)
		}
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	v := New(testKey, testSecret)
	now := time.Now()
	const target = "http://bkt.localhost:9000/k?list-type=2"
	tests := []struct {
		name   string
		tamper func(*http.Request)
		want   error
	}{
		{"method", func(r *http.Request) { r.Method = http.MethodDelete }, ErrSignatureMismatch},
		{"path", func(r *http.Request) { r.URL.Path = "/other" }, ErrSignatureMismatch},
		{"query", func(r *http.Request) { r.URL.RawQuery = "list-type=1" }, ErrSignatureMismatch},
		{"host", func(r *http.Request) { r.Host = "evil.localhost:9000" }, ErrSignatureMismatch},
		{"signed header", func(r *http.Request) { r.Header.Set("X-Amz-Meta-Color", "red") }, ErrSignatureMismatch},
		{"payload hash header", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", sha256Hex("other")) }, ErrSignatureMismatch},
		{"signature", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "Signature=", "Signature=0", 1))
		}, ErrSignatureMismatch},
		{"unknown key", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), testKey, "AKIAUNKNOWN000000000", 1))
		}, ErrInvalidAccessKeyID},
		{"service", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "/s3/", "/ec2/", 1))
		}, ErrMalformedAuth},
		{"date does not match scope", func(r *http.Request) {
			r.Header.Set("X-Amz-Date", now.Add(48*time.Hour).UTC().Format(timeFormat))
		}, ErrMalformedAuth},
		{"no date", func(r *http.Request) { r.Header.Del("X-Amz-Date") }, ErrMalformedAuth},
		{"no payload hash", func(r *http.Request) { r.Header.Del("X-Amz-Content-Sha256") }, ErrMissingContentSHA256},
		{"bad payload hash", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", "nope") }, ErrMalformedAuth},
		{"streaming", func(r *http.Request) {
			r.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD")
		}, ErrNotImplemented},
		{"no authorization", func(r *http.Request) { r.Header.Del("Authorization") }, ErrMissingAuth},
		{"presigned without its parameters", func(r *http.Request) {
			r.Header.Del("Authorization")
			r.URL.RawQuery = "X-Amz-Signature=abc"
		}, ErrMalformedPresign},
		{"sigv2", func(r *http.Request) { r.Header.Set("Authorization", "AWS "+testKey+":c2ln") }, ErrUnsupportedAuth},
		{"unsigned x-amz header added", func(r *http.Request) { r.Header.Set("X-Amz-Copy-Source", "/secret/object") }, ErrUnsignedHeader},
		{"no host signed", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "SignedHeaders=host;", "SignedHeaders=", 1))
		}, ErrMalformedAuth},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := signed(t, http.MethodGet, target, "", sha256Hex(""), now, func(r *http.Request) {
				r.Header.Set("X-Amz-Meta-Color", "blue")
			})
			if err := v.Verify(r); err != nil {
				t.Fatalf("untampered request: Verify() error = %v", err)
			}
			r = signed(t, http.MethodGet, target, "", sha256Hex(""), now, func(r *http.Request) {
				r.Header.Set("X-Amz-Meta-Color", "blue")
			})
			tt.tamper(r)
			if err := v.Verify(r); !errors.Is(err, tt.want) {
				t.Errorf("Verify() after changing the %s = %v, want %v", tt.name, err, tt.want)
			}
		})
	}
}

func TestVerifyPayloadHash(t *testing.T) {
	v := New(testKey, testSecret)
	tests := []struct {
		name, body, hash string
		wantReadErr      error
	}{
		{"matching body", "hello", sha256Hex("hello"), nil},
		{"changed body", "hellO", sha256Hex("hello"), ErrContentSHA256Mismatch},
		{"unsigned payload", "anything", "UNSIGNED-PAYLOAD", nil},
		{"empty body", "", sha256Hex(""), nil},
	}
	for _, tt := range tests {
		r := signed(t, http.MethodPut, "http://bkt.localhost/k", tt.body, tt.hash, time.Now(), nil)
		if err := v.Verify(r); err != nil {
			t.Fatalf("%s: Verify() error = %v", tt.name, err)
		}
		got, err := io.ReadAll(r.Body)
		if !errors.Is(err, tt.wantReadErr) || (err == nil && string(got) != tt.body) {
			t.Errorf("%s: read body = %q, %v, want %q, %v", tt.name, got, err, tt.body, tt.wantReadErr)
		}
	}
}

func TestVerifyClockSkew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := New(testKey, testSecret)
		tests := []struct {
			offset time.Duration
			want   error
		}{
			{0, nil},
			{-14 * time.Minute, nil},
			{14 * time.Minute, nil},
			{-16 * time.Minute, ErrRequestTimeTooSkewed},
			{16 * time.Minute, ErrRequestTimeTooSkewed},
		}
		for _, tt := range tests {
			r := signed(t, http.MethodGet, "http://bkt.localhost/k", "", sha256Hex(""), time.Now().Add(tt.offset), nil)
			if err := v.Verify(r); !errors.Is(err, tt.want) {
				t.Errorf("signed %v from now: Verify() error = %v, want %v", tt.offset, err, tt.want)
			}
		}
	})
}

func TestCanonicalQuery(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"uploads", "uploads="},
		{"b=2&a=1", "a=1&b=2"},
		{"a=2&a=1", "a=1&a=2"},
		{"prefix=a%2Fb", "prefix=a%2Fb"},
		{"k=a+b", "k=a%20b"},
		{"k=a%2Bb", "k=a%2Bb"},
		{"k=a%20b", "k=a%20b"},
		{"k=%E2%9C%93", "k=%E2%9C%93"},
	}
	for _, tt := range tests {
		if got := canonicalQuery(tt.in); got != tt.want {
			t.Errorf("canonicalQuery(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestQueryPlusIsNotPercent2B(t *testing.T) {
	v := New(testKey, testSecret)
	r := signed(t, http.MethodGet, "http://bkt.localhost/?prefix=a%2Bb", "", sha256Hex(""), time.Now(), nil)
	r.URL.RawQuery = "prefix=a+b" // reads as "a b" to r.URL.Query()
	if err := v.Verify(r); !errors.Is(err, ErrSignatureMismatch) {
		t.Errorf("Verify() with %q signed and %q sent = %v, want ErrSignatureMismatch", "prefix=a%2Bb", "prefix=a+b", err)
	}
}

func TestCollapseSpaces(t *testing.T) {
	tests := []struct{ in, want string }{
		{"a", "a"},
		{"  a   b  ", "a b"},
		{"a\tb", "a\tb"},
		{"\ta b", "\ta b"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := collapseSpaces(tt.in); got != tt.want {
			t.Errorf("collapseSpaces(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// presign builds a server-side request for a presigned URL for target, valid
// for expires seconds from at.
func presign(t *testing.T, method, target string, expires int, at time.Time) *http.Request {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("X-Amz-Expires", strconv.Itoa(expires))
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(context.Background(), method, u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	signedURL, _, err := signer.PresignHTTP(req.Context(), creds, req, unsignedHash, "s3", "eu-west-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequestWithContext(context.Background(), method, signedURL, nil)
}

// setQuery rewrites the query of r through f.
func setQuery(r *http.Request, f func(url.Values)) {
	q := r.URL.Query()
	f(q)
	r.URL.RawQuery = q.Encode()
}

func TestVerifyPresignedMethods(t *testing.T) {
	v := New(testKey, testSecret)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodHead, http.MethodDelete} {
		r := presign(t, method, "http://bkt.localhost:9000/k", 900, time.Now())
		if err := v.Verify(r); err != nil {
			t.Errorf("presigned %s: Verify() error = %v, want nil", method, err)
		}
	}
}

func TestVerifyPresignedOnTheWire(t *testing.T) {
	v := New(testKey, testSecret)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := v.Verify(r); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)

	tests := []struct {
		method, key, query string
	}{
		{http.MethodGet, "a//b", ""},
		{http.MethodGet, "../x", ""},
		{http.MethodGet, "a+b", ""},
		{http.MethodGet, "a b", ""},
		{http.MethodGet, "✓", ""},
		{http.MethodGet, "dir/", ""},
		{http.MethodGet, "100%", ""},
		{http.MethodGet, "k", "response-content-disposition=attachment%3B%20filename%3D%22a%20b.txt%22"},
		{http.MethodGet, "k", "response-content-type=text%2Fplain&x-id=GetObject"},
		{http.MethodGet, "k", "a=2&a=1&b=%20+"},
		{http.MethodPut, "k", ""},
		{http.MethodPut, "a b", ""},
		{http.MethodDelete, "k", ""},
	}
	for _, tt := range tests {
		u := &url.URL{Scheme: "http", Host: strings.TrimPrefix(srv.URL, "http://"), Path: "/bkt/" + tt.key, RawPath: s3Path("bkt/" + tt.key), RawQuery: tt.query}
		signedReq := presign(t, tt.method, u.String(), 900, time.Now())
		var body io.Reader
		if tt.method == http.MethodPut {
			body = strings.NewReader("payload")
		}
		req, err := http.NewRequestWithContext(context.Background(), tt.method, signedReq.URL.String(), body)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("presigned %s key %q query %q = %d %s, want 200", tt.method, tt.key, tt.query, resp.StatusCode, msg)
		}
	}
}

func TestVerifyPresignedRejectsTampering(t *testing.T) {
	v := New(testKey, testSecret)
	const target = "http://bkt.localhost:9000/k?response-content-disposition=attachment"
	tests := []struct {
		name   string
		tamper func(*http.Request)
		want   error
	}{
		{"method", func(r *http.Request) { r.Method = http.MethodDelete }, ErrSignatureMismatch},
		{"path", func(r *http.Request) { r.URL.Path = "/other" }, ErrSignatureMismatch},
		{"query param added", func(r *http.Request) { setQuery(r, func(q url.Values) { q.Set("x-id", "Tampered") }) }, ErrSignatureMismatch},
		{"query value", func(r *http.Request) {
			setQuery(r, func(q url.Values) { q.Set("response-content-disposition", "inline") })
		}, ErrSignatureMismatch},
		{"query param removed", func(r *http.Request) {
			setQuery(r, func(q url.Values) { q.Del("response-content-disposition") })
		}, ErrSignatureMismatch},
		{"host", func(r *http.Request) { r.Host = "evil.localhost:9000" }, ErrSignatureMismatch},
		{"signature", func(r *http.Request) {
			setQuery(r, func(q url.Values) { q.Set("X-Amz-Signature", "0"+q.Get("X-Amz-Signature")) })
		}, ErrSignatureMismatch},
		{"longer expiry", func(r *http.Request) { setQuery(r, func(q url.Values) { q.Set("X-Amz-Expires", "901") }) }, ErrSignatureMismatch},
		{"unknown key", func(r *http.Request) {
			setQuery(r, func(q url.Values) {
				q.Set("X-Amz-Credential", strings.Replace(q.Get("X-Amz-Credential"), testKey, "AKIAUNKNOWN000000000", 1))
			})
		}, ErrInvalidAccessKeyID},
		{"service", func(r *http.Request) {
			setQuery(r, func(q url.Values) {
				q.Set("X-Amz-Credential", strings.Replace(q.Get("X-Amz-Credential"), "/s3/", "/ec2/", 1))
			})
		}, ErrMalformedAuth},
		{"date does not match scope", func(r *http.Request) {
			setQuery(r, func(q url.Values) { q.Set("X-Amz-Date", time.Now().Add(48*time.Hour).UTC().Format(timeFormat)) })
		}, ErrMalformedAuth},
		{"bad date", func(r *http.Request) { setQuery(r, func(q url.Values) { q.Set("X-Amz-Date", "yesterday") }) }, ErrMalformedAuth},
		{"no host signed", func(r *http.Request) {
			setQuery(r, func(q url.Values) { q.Set("X-Amz-SignedHeaders", "x-amz-meta-a") })
		}, ErrMalformedAuth},
		{"algorithm", func(r *http.Request) {
			setQuery(r, func(q url.Values) { q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA512") })
		}, ErrMalformedPresign},
		{"duplicate parameter", func(r *http.Request) { setQuery(r, func(q url.Values) { q.Add("X-Amz-Expires", "900") }) }, ErrMalformedPresign},
		{"unsigned x-amz header", func(r *http.Request) { r.Header.Set("X-Amz-Copy-Source", "/secret/object") }, ErrUnsignedHeader},
		{"authorization header with a bad scheme", func(r *http.Request) { r.Header.Set("Authorization", "Basic abc") }, ErrUnsupportedAuth},
	}
	for _, name := range presignParams {
		tests = append(tests, struct {
			name   string
			tamper func(*http.Request)
			want   error
		}{"no " + name, func(r *http.Request) { setQuery(r, func(q url.Values) { q.Del(name) }) }, ErrMalformedPresign})
	}
	for _, expires := range []string{"0", "604801", "-5", "+5", "1e3", "9999999999999999999999", " 5", ""} {
		tests = append(tests, struct {
			name   string
			tamper func(*http.Request)
			want   error
		}{"expires " + strconv.Quote(expires), func(r *http.Request) { setQuery(r, func(q url.Values) { q.Set("X-Amz-Expires", expires) }) }, ErrMalformedPresign})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := v.Verify(presign(t, http.MethodGet, target, 900, time.Now())); err != nil {
				t.Fatalf("untampered request: Verify() error = %v", err)
			}
			r := presign(t, http.MethodGet, target, 900, time.Now())
			tt.tamper(r)
			if err := v.Verify(r); !errors.Is(err, tt.want) {
				t.Errorf("Verify() after changing the %s = %v, want %v", tt.name, err, tt.want)
			}
		})
	}
}

func TestVerifyPresignedExpiresRange(t *testing.T) {
	v := New(testKey, testSecret)
	for _, expires := range []int{1, 900, maxExpires} {
		if err := v.Verify(presign(t, http.MethodGet, "http://bkt.localhost/k", expires, time.Now())); err != nil {
			t.Errorf("X-Amz-Expires=%d: Verify() error = %v, want nil", expires, err)
		}
	}
}

func TestVerifyPresignedExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := New(testKey, testSecret)
		r := presign(t, http.MethodGet, "http://bkt.localhost/k", 60, time.Now())
		// Each wait adds to the time since signing. A URL is valid until
		// signing time plus X-Amz-Expires, inclusive.
		steps := []struct {
			wait time.Duration
			want error
		}{
			{59 * time.Second, nil},
			{time.Second, nil},
			{time.Nanosecond, ErrRequestExpired},
			{time.Hour, ErrRequestExpired},
		}
		var total time.Duration
		for _, st := range steps {
			time.Sleep(st.wait)
			total += st.wait
			if err := v.Verify(r); !errors.Is(err, st.want) {
				t.Errorf("X-Amz-Expires=60, verified %v after signing: Verify() error = %v, want %v", total, err, st.want)
			}
		}
	})
}

func TestVerifyPresignedClockSkew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := New(testKey, testSecret)
		tests := []struct {
			offset time.Duration
			want   error
		}{
			{14 * time.Minute, nil},
			{16 * time.Minute, ErrRequestTimeTooSkewed},
		}
		for _, tt := range tests {
			r := presign(t, http.MethodGet, "http://bkt.localhost/k", 900, time.Now().Add(tt.offset))
			if err := v.Verify(r); !errors.Is(err, tt.want) {
				t.Errorf("signed %v from now: Verify() error = %v, want %v", tt.offset, err, tt.want)
			}
		}
	})
}
