package diff

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
	authPresigned              // a query-string signature, valid for 15 minutes unless the step sets X-Amz-Expires
	authPresignedExpired       // signed two hours ago, valid for one hour
	authPresignedTampered      // a valid signature, then a query parameter it does not cover
	authPresignedFuture        // signed one hour from now
	authPresignedBadCredential // credential scope names the ec2 service
	authPresignedMissingParam  // X-Amz-Date removed after signing
)

// target is one S3 implementation the suite talks to over HTTP.
type target struct {
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
var signer = newSigner()

func newSigner() *v4.Signer {
	return v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
}

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
	if st.stream != "" {
		payloadHash = st.stream
		setStreamHeaders(req, st)
	}
	// A presigned request has no x-amz-content-sha256: it would be an unsigned x-amz header.
	presigned := st.auth >= authPresigned
	if !presigned {
		req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}

	creds, signTime := tg.creds, time.Now()
	switch st.auth {
	case authNone:
	case authUnknownKey:
		creds = aws.Credentials{AccessKeyID: "AKIAPAILDIFFUNKNOWN0", SecretAccessKey: "unknown"}
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime)
	case authBadSignature:
		// The signer caches keys by access key, not secret, so a wrong secret
		// through the shared signer would break every later request.
		creds.SecretAccessKey += "-wrong"
		err = newSigner().SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime)
	case authSkewed:
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime.Add(-20*time.Minute))
	case authPresigned, authPresignedExpired, authPresignedTampered, authPresignedFuture, authPresignedBadCredential, authPresignedMissingParam:
		err = tg.presign(ctx, req, st.auth, signTime)
	default:
		err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", tg.region, signTime)
	}
	if err != nil {
		return response{}, fmt.Errorf("sign: %w", err)
	}
	if st.stream != "" {
		amzDate := req.Header.Get("X-Amz-Date")
		scope := amzDate[:8] + "/" + tg.region + "/s3/aws4_request"
		_, seed, _ := strings.Cut(req.Header.Get("Authorization"), "Signature=")
		enc := encodeChunked(st, signingKey(creds.SecretAccessKey, amzDate[:8], tg.region), amzDate, scope, seed)
		req.Body, req.GetBody = io.NopCloser(bytes.NewReader(enc)), nil
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

// presign replaces req.URL with a presigned URL. The payload is unsigned.
func (tg *target) presign(ctx context.Context, req *http.Request, auth authMode, signTime time.Time) error {
	expires := "900"
	switch auth {
	case authPresignedExpired:
		signTime, expires = signTime.Add(-2*time.Hour), "3600"
	case authPresignedFuture:
		signTime = signTime.Add(time.Hour)
	}
	q := req.URL.Query()
	if !q.Has("X-Amz-Expires") {
		q.Set("X-Amz-Expires", expires)
	}
	req.URL.RawQuery = q.Encode()
	signedURL, _, err := signer.PresignHTTP(ctx, tg.creds, req, "UNSIGNED-PAYLOAD", "s3", tg.region, signTime)
	if err != nil {
		return err
	}
	req.URL, err = url.Parse(signedURL)
	if err != nil {
		return err
	}
	switch auth {
	case authPresignedTampered:
		req.URL.RawQuery += "&x-id=Tampered"
	case authPresignedBadCredential:
		q := req.URL.Query()
		q.Set("X-Amz-Credential", strings.Replace(q.Get("X-Amz-Credential"), "/s3/aws4_request", "/ec2/aws4_request", 1))
		req.URL.RawQuery = q.Encode()
	case authPresignedMissingParam:
		q := req.URL.Query()
		q.Del("X-Amz-Date")
		req.URL.RawQuery = q.Encode()
	}
	return nil
}

const (
	streamSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	streamSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// setStreamHeaders adds the aws-chunked headers a step does not set itself;
// an empty value in the step removes one. Signatures have a fixed length, so
// a dummy encoding gives the Content-Length to sign.
func setStreamHeaders(req *http.Request, st step) {
	defaults := map[string]string{
		"Content-Encoding":             "aws-chunked",
		"X-Amz-Decoded-Content-Length": strconv.Itoa(len(st.body)),
	}
	if name, _, ok := strings.Cut(st.trailer, ":"); ok {
		defaults["X-Amz-Trailer"] = name
	}
	for k, v := range defaults {
		if _, set := st.header[k]; !set {
			req.Header.Set(k, v)
		}
		if req.Header.Get(k) == "" {
			req.Header.Del(k)
		}
	}
	req.ContentLength = int64(len(encodeChunked(st, nil, "", "", "")))
}

// encodeChunked aws-chunk encodes st.body, chaining chunk signatures from
// seed. The suite signs chunks itself, so recording checks this code against
// AWS, independently of pail's verifier.
func encodeChunked(st step, key []byte, amzDate, scope, seed string) []byte {
	prev := seed
	sign := func(stringToSign string) string {
		prev = hex.EncodeToString(hmacSHA256(key, stringToSign))
		return prev
	}
	size := cmp.Or(st.chunk, 8<<10)
	var b bytes.Buffer
	for rest := st.body; ; {
		data := rest[:min(size, len(rest))]
		rest = rest[len(data):]
		if st.stream == streamUnsignedTrailer {
			fmt.Fprintf(&b, "%x\r\n", len(data))
		} else {
			sig := sign("AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" + prev + "\n" + sha256Hex("") + "\n" + sha256Hex(data))
			if st.badChunkSig && b.Len() == 0 {
				sig = strings.Repeat("0", len(sig))
			}
			fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(data), sig)
		}
		if data == "" {
			break
		}
		b.WriteString(data + "\r\n")
	}
	if st.trailer != "" {
		b.WriteString(st.trailer + "\r\n")
		if st.stream == streamSignedTrailer {
			sig := sign("AWS4-HMAC-SHA256-TRAILER\n" + amzDate + "\n" + scope + "\n" + prev + "\n" + sha256Hex(st.trailer+"\n"))
			b.WriteString("x-amz-trailer-signature:" + sig + "\r\n")
		}
	}
	b.WriteString("\r\n")
	return b.Bytes()
}

func signingKey(secret, date, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	for _, s := range []string{region, "s3", "aws4_request"} {
		k = hmacSHA256(k, s)
	}
	return k
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
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

// cleanupBucket deletes every object, upload, and finally the bucket. It logs
// instead of failing, and never logs a raw error body: AWS can echo the token.
func cleanupBucket(t *testing.T, tg *target, bucket string) {
	t.Helper()
	if !strings.HasPrefix(bucket, "pail-diff-") {
		t.Fatalf("cleanup refuses bucket %q: not created by this suite", bucket)
	}
	ctx := context.Background()
list:
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
			resp, err := tg.do(ctx, step{method: http.MethodDelete, key: c.Key}, bucket)
			if err != nil || resp.status != http.StatusNoContent {
				t.Logf("cleanup: delete %q: %s", c.Key, failure(resp, err))
				break list
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
	if err != nil || resp.status != http.StatusNoContent && resp.status != http.StatusNotFound {
		t.Logf("cleanup: delete bucket %s: %s", bucket, failure(resp, err))
	}
}

// failure describes a failed cleanup call by status and S3 error code only.
func failure(resp response, err error) string {
	if err != nil {
		return err.Error()
	}
	s := fmt.Sprintf("status %d", resp.status)
	if body, isErr, _ := canonicalXML(string(resp.body)); isErr {
		s += " " + strings.Join(strings.Fields(body), " ")
	}
	return s
}
