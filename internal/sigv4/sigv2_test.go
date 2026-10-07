package sigv4

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestSigV2CanonicalResource(t *testing.T) {
	for _, tc := range []struct {
		target, bucket, want string
	}{
		{"http://localhost/bucket/a//b", "", "/bucket/a//b"},
		{"http://localhost/bucket/fran%c3%a7ais/a%2fb", "", "/bucket/fran%c3%a7ais/a%2fb"},
		{"http://bucket.localhost/a%20b%2Bc", "bucket", "/bucket/a%20b%2Bc"},
		{"http://bucket.localhost", "bucket", "/bucket/"},
		{"http://localhost/", "", "/"},
		{"http://localhost/bucket/?prefix=photos&max-keys=50", "", "/bucket/"},
		{"http://localhost/bucket/k?versionId=v%2F1&acl&response-content-type=text%2Fplain", "", "/bucket/k?acl&response-content-type=text/plain&versionId=v/1"},
		{"http://localhost/bucket/k?uploadId=id%2B1&partNumber=2", "", "/bucket/k?partNumber=2&uploadId=id+1"},
		{"http://localhost/bucket/?delete=", "", "/bucket/?delete"},
	} {
		u, err := url.Parse(tc.target)
		if err != nil {
			t.Fatal(err)
		}
		if got := canonicalSigV2Resource(u, u.Query(), tc.bucket); got != tc.want {
			t.Errorf("resource for %q, bucket %q = %q, want %q", tc.target, tc.bucket, got, tc.want)
		}
	}
}

func TestSigV2CanonicalHeaders(t *testing.T) {
	headers := http.Header{
		"X-Amz-Meta-Reviewedby": {" joe@example.com ", "\tjane@example.com\t"},
		"X-Amz-Acl":             {"private"},
		"X-Amz-Meta-Color":      {" blue  green "},
		"Content-Type":          {"text/plain"},
		"Date":                  {"ignored"},
	}
	want := "x-amz-acl:private\nx-amz-meta-color:blue  green\nx-amz-meta-reviewedby:joe@example.com,jane@example.com\n"
	if got := canonicalSigV2Headers(headers); got != want {
		t.Errorf("canonical headers = %q, want %q", got, want)
	}
}

func sigV2Request(t *testing.T, expires time.Time) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "http://localhost/bucket/a%20b%2Bc?uploadId=upload&partNumber=1", nil)
	r.Header.Set("Content-MD5", "1B2M2Y8AsgTpgAmY7PhCfg==")
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("X-Amz-Acl", "private")
	r.Header.Add("X-Amz-Meta-Color", " blue ")
	r.Header.Add("X-Amz-Meta-Color", "green")
	expiration := strconv.FormatInt(expires.Unix(), 10)
	stringToSign := "PUT\n1B2M2Y8AsgTpgAmY7PhCfg==\ntext/plain\n" + expiration +
		"\nx-amz-acl:private\nx-amz-meta-color:blue,green\n/bucket/a%20b%2Bc?partNumber=1&uploadId=upload"
	mac := hmac.New(sha1.New, []byte(testSecret))
	_, _ = mac.Write([]byte(stringToSign))
	q := r.URL.Query()
	q.Set("AWSAccessKeyId", testKey)
	q.Set("Expires", expiration)
	q.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	r.URL.RawQuery = q.Encode()
	return r
}

func TestVerifyPresignedV2(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
		want   error
	}{
		{"valid", func(*http.Request) {}, nil},
		{"date ignored", func(r *http.Request) { r.Header.Set("Date", "ignored") }, nil},
		{"unsigned listing parameter", func(r *http.Request) { r.URL.RawQuery += "&prefix=anything" }, nil},
		{"method", func(r *http.Request) { r.Method = http.MethodDelete }, ErrSignatureMismatch},
		{"escaped path", func(r *http.Request) { r.URL.RawPath = "/bucket/a%20b%2bc" }, ErrSignatureMismatch},
		{"MD5", func(r *http.Request) { r.Header.Set("Content-MD5", "changed") }, ErrSignatureMismatch},
		{"content type", func(r *http.Request) { r.Header.Set("Content-Type", "image/png") }, ErrSignatureMismatch},
		{"metadata", func(r *http.Request) { r.Header.Set("X-Amz-Meta-Color", "red") }, ErrSignatureMismatch},
		{"new amz header", func(r *http.Request) { r.Header.Set("X-Amz-Copy-Source", "/other/key") }, ErrSignatureMismatch},
		{"subresource", func(r *http.Request) { r.URL.RawQuery += "&acl" }, ErrSignatureMismatch},
		{"duplicate resource", func(r *http.Request) { r.URL.RawQuery += "&partNumber=2" }, ErrMalformedPresignV2},
		{"duplicate expiration", func(r *http.Request) { r.URL.RawQuery += "&Expires=100" }, ErrMalformedPresignV2},
		{"missing expiration", func(r *http.Request) {
			q := r.URL.Query()
			q.Del("Expires")
			r.URL.RawQuery = q.Encode()
		}, ErrMissingAuth},
		{"bad encoding", func(r *http.Request) { r.URL.RawQuery += "&bad=%zz" }, ErrMalformedPresignV2},
		{"mixed signature versions", func(r *http.Request) { r.URL.RawQuery += "&X-Amz-Signature=wrong" }, ErrUnsupportedAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := sigV2Request(t, time.Now().Add(time.Hour))
			tc.mutate(r)
			if err := New(testKey, testSecret).VerifyPresignedV2(r, ""); !errors.Is(err, tc.want) {
				t.Errorf("verify %s = %v, want %v", tc.name, err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name, field, value string
		want               error
	}{
		{"missing key", "AWSAccessKeyId", "", ErrMalformedPresignV2},
		{"unknown key", "AWSAccessKeyId", "unknown", ErrInvalidAccessKeyID},
		{"missing signature", "Signature", "", ErrMalformedPresignV2},
		{"malformed signature", "Signature", "broken", ErrSignatureMismatch},
		{"wrong signature", "Signature", base64.StdEncoding.EncodeToString(make([]byte, 20)), ErrSignatureMismatch},
		{"empty expiry", "Expires", "", ErrMalformedPresignV2},
		{"invalid expiry", "Expires", "never", ErrMalformedPresignV2},
		{"negative expiry", "Expires", "-1", ErrMalformedPresignV2},
		{"signed expiry", "Expires", "+123", ErrMalformedPresignV2},
		{"overflow expiry", "Expires", strings.Repeat("9", 30), ErrMalformedPresignV2},
		{"expired", "Expires", "1", ErrRequestExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := sigV2Request(t, time.Now().Add(time.Hour))
			q := r.URL.Query()
			q.Set(tc.field, tc.value)
			r.URL.RawQuery = q.Encode()
			if err := New(testKey, testSecret).VerifyPresignedV2(r, ""); !errors.Is(err, tc.want) {
				t.Errorf("verify %s = %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

func TestPresignedV2Expiration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := New(testKey, testSecret)
		r := sigV2Request(t, time.Now().Add(time.Second))
		if err := v.VerifyPresignedV2(r, ""); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if err := v.VerifyPresignedV2(r, ""); !errors.Is(err, ErrRequestExpired) {
			t.Fatalf("expired request = %v, want ErrRequestExpired", err)
		}
		r = sigV2Request(t, time.Now().Add(30*24*time.Hour))
		if err := v.VerifyPresignedV2(r, ""); err != nil {
			t.Fatalf("30-day URL = %v, want nil", err)
		}
	})
}
