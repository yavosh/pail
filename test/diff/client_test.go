package diff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// authMode picks how a step signs its request.
type authMode int

const (
	authSigned authMode = iota
	authNone
	authUnknownKey
	authBadSignature
	authSkewed
)

// target is one S3 implementation the suite talks to over HTTP.
type target struct {
	name   string
	region string
	creds  aws.Credentials
	client *http.Client
	// host returns the virtual-hosted-style host for a bucket.
	host   func(bucket string) string
	scheme string
}

// response is one raw HTTP answer.
type response struct {
	status int
	header http.Header
	body   []byte
}

// signer signs like the S3 SDKs: S3 paths are encoded once, never twice.
var signer = v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })

// do sends one step to the target, addressed virtual-hosted-style at bucket.
func (tg *target) do(ctx context.Context, st step, bucket string) (response, error) {
	expand := func(s string) string { return strings.ReplaceAll(s, "{bucket}", bucket) }
	u := &url.URL{
		Scheme:   tg.scheme,
		Host:     tg.host(bucket),
		Path:     "/" + st.key,
		RawPath:  "/" + s3Escape(st.key),
		RawQuery: expand(st.query),
	}
	req, err := http.NewRequestWithContext(ctx, st.method, u.String(), bytes.NewReader([]byte(st.body)))
	if err != nil {
		return response{}, err
	}
	for k, v := range st.header {
		req.Header.Set(k, expand(v))
	}
	sum := sha256.Sum256([]byte(st.body))
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	creds, signTime := tg.creds, time.Now()
	switch st.auth {
	case authNone:
	case authUnknownKey:
		creds = aws.Credentials{AccessKeyID: "AKIAPAILDIFFUNKNOWN0", SecretAccessKey: "unknown"}
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime)
	case authBadSignature:
		creds.SecretAccessKey += "-wrong"
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime)
	case authSkewed:
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime.Add(-20*time.Minute))
	default:
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime)
	}
	if err != nil {
		return response{}, fmt.Errorf("sign: %w", err)
	}

	resp, err := tg.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

// s3Escape percent-encodes a key the way S3 clients do: every byte except
// unreserved characters and "/".
func s3Escape(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || strings.IndexByte("-._~/", c) >= 0 {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// cleanupBucket deletes every object, upload, and finally the bucket. It runs
// only against buckets this run created, and logs instead of failing.
func cleanupBucket(t *testing.T, tg *target, bucket string) {
	t.Helper()
	ctx := context.Background()
	for range 100 {
		resp, err := tg.do(ctx, step{method: http.MethodGet, query: "list-type=2"}, bucket)
		if err != nil || resp.status != http.StatusOK {
			break
		}
		var list struct {
			Contents []struct{ Key string }
		}
		if err := xml.Unmarshal(resp.body, &list); err != nil || len(list.Contents) == 0 {
			break
		}
		for _, c := range list.Contents {
			if _, err := tg.do(ctx, step{method: http.MethodDelete, key: c.Key}, bucket); err != nil {
				t.Logf("cleanup: delete %q: %v", c.Key, err)
			}
		}
	}
	if resp, err := tg.do(ctx, step{method: http.MethodGet, query: "uploads"}, bucket); err == nil && resp.status == http.StatusOK {
		var uploads struct {
			Upload []struct {
				Key      string
				UploadID string `xml:"UploadId"`
			}
		}
		if xml.Unmarshal(resp.body, &uploads) == nil {
			for _, u := range uploads.Upload {
				_, _ = tg.do(ctx, step{method: http.MethodDelete, key: u.Key, query: "uploadId=" + url.QueryEscape(u.UploadID)}, bucket)
			}
		}
	}
	resp, err := tg.do(ctx, step{method: http.MethodDelete}, bucket)
	if err != nil {
		t.Logf("cleanup: delete bucket %s: %v", bucket, err)
		return
	}
	if resp.status != http.StatusNoContent && resp.status != http.StatusNotFound {
		t.Logf("cleanup: delete bucket %s: status %d: %s", bucket, resp.status, resp.body)
	}
}
