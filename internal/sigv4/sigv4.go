// Package sigv4 verifies AWS Signature Version 4 on S3 requests.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Errors callers map to S3 error codes with errors.Is.
var (
	ErrMissingAuth           = errors.New("request is not signed")
	ErrUnsupportedAuth       = errors.New("unsupported authorization mechanism")
	ErrMalformedAuth         = errors.New("malformed authorization header")
	ErrInvalidAccessKeyID    = errors.New("unknown access key")
	ErrSignatureMismatch     = errors.New("signature does not match")
	ErrRequestTimeTooSkewed  = errors.New("request time too skewed")
	ErrMissingContentSHA256  = errors.New("missing x-amz-content-sha256")
	ErrContentSHA256Mismatch = errors.New("x-amz-content-sha256 does not match the body")
	// ErrNotImplemented marks valid requests pail cannot verify yet.
	ErrNotImplemented = errors.New("signing mode not implemented")
)

const (
	algorithm     = "AWS4-HMAC-SHA256"
	timeFormat    = "20060102T150405Z"
	maxSkew       = 15 * time.Minute
	unsignedHash  = "UNSIGNED-PAYLOAD"
	streamingMode = "STREAMING-"
)

// Verifier checks signatures against one set of access keys.
type Verifier struct {
	secrets map[string]string
}

// New returns a Verifier that accepts the given access key ID and secret.
func New(accessKeyID, secretAccessKey string) *Verifier {
	return &Verifier{secrets: map[string]string{accessKeyID: secretAccessKey}}
}

// Verify checks the Authorization header of r. When the request carries a
// SHA-256 payload hash, Verify wraps r.Body so a mismatch fails the last read
// with ErrContentSHA256Mismatch; the caller must not commit before EOF.
func (v *Verifier) Verify(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		if r.URL.Query().Has("X-Amz-Signature") {
			return fmt.Errorf("presigned URLs: %w", ErrNotImplemented)
		}
		return ErrMissingAuth
	}
	rest, ok := strings.CutPrefix(auth, algorithm+" ")
	if !ok {
		return ErrUnsupportedAuth
	}
	a, err := parseAuthorization(rest)
	if err != nil {
		return err
	}
	secret, ok := v.secrets[a.accessKeyID]
	if !ok {
		return ErrInvalidAccessKeyID
	}

	amzDate, signedAt, err := requestTime(r)
	if err != nil {
		return err
	}
	if amzDate[:8] != a.date {
		return fmt.Errorf("credential date %s does not match request date %s: %w", a.date, amzDate[:8], ErrMalformedAuth)
	}
	if skew := time.Since(signedAt); skew > maxSkew || skew < -maxSkew {
		return ErrRequestTimeTooSkewed
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	switch {
	case payloadHash == "":
		return ErrMissingContentSHA256
	case strings.HasPrefix(payloadHash, streamingMode):
		return fmt.Errorf("%s: %w", payloadHash, ErrNotImplemented)
	case payloadHash != unsignedHash && !isSHA256Hex(payloadHash):
		return fmt.Errorf("x-amz-content-sha256 %q: %w", payloadHash, ErrMalformedAuth)
	}

	scope := strings.Join([]string{a.date, a.region, "s3", "aws4_request"}, "/")
	creq := canonicalRequest(r, a.signedHeaders, payloadHash)
	sum := sha256.Sum256([]byte(creq))
	stringToSign := algorithm + "\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	key := signingKey(secret, a.date, a.region)
	want := hex.EncodeToString(hmacSHA256(key, stringToSign))
	if !hmac.Equal([]byte(want), []byte(a.signature)) {
		return ErrSignatureMismatch
	}

	if payloadHash != unsignedHash && r.Body != nil {
		want, _ := hex.DecodeString(payloadHash)
		r.Body = &hashingBody{body: r.Body, h: sha256.New(), want: want}
	}
	return nil
}

type authorization struct {
	accessKeyID, date, region string
	signedHeaders             []string
	signature                 string
}

// parseAuthorization reads "Credential=..., SignedHeaders=..., Signature=...".
func parseAuthorization(s string) (authorization, error) {
	fields := map[string]string{}
	for part := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return authorization{}, ErrMalformedAuth
		}
		fields[k] = v
	}
	cred := strings.Split(fields["Credential"], "/")
	if len(cred) != 5 || cred[0] == "" || cred[3] != "s3" || cred[4] != "aws4_request" {
		return authorization{}, fmt.Errorf("credential %q: %w", fields["Credential"], ErrMalformedAuth)
	}
	if fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return authorization{}, ErrMalformedAuth
	}
	headers := strings.Split(fields["SignedHeaders"], ";")
	if !slices.Contains(headers, "host") {
		return authorization{}, fmt.Errorf("host is not signed: %w", ErrMalformedAuth)
	}
	return authorization{
		accessKeyID:   cred[0],
		date:          cred[1],
		region:        cred[2],
		signedHeaders: headers,
		signature:     fields["Signature"],
	}, nil
}

// requestTime returns the signing time from X-Amz-Date, else from Date.
func requestTime(r *http.Request) (string, time.Time, error) {
	if s := r.Header.Get("X-Amz-Date"); s != "" {
		t, err := time.Parse(timeFormat, s)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("x-amz-date %q: %w", s, ErrMalformedAuth)
		}
		return s, t, nil
	}
	if s := r.Header.Get("Date"); s != "" {
		t, err := http.ParseTime(s)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("date %q: %w", s, ErrMalformedAuth)
		}
		return t.UTC().Format(timeFormat), t, nil
	}
	return "", time.Time{}, fmt.Errorf("no request date: %w", ErrMalformedAuth)
}

// canonicalRequest builds the SigV4 canonical request with the S3 rules: the
// path is encoded once and never normalized.
func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) string {
	var b strings.Builder
	b.WriteString(r.Method + "\n")
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	b.WriteString(uriEncode(path, false) + "\n")
	b.WriteString(canonicalQuery(r.URL.RawQuery) + "\n")
	for _, name := range signedHeaders {
		b.WriteString(name + ":" + headerValue(r, name) + "\n")
	}
	b.WriteString("\n" + strings.Join(signedHeaders, ";") + "\n" + payloadHash)
	return b.String()
}

func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var pairs []pair
	for part := range strings.SplitSeq(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		// PathUnescape keeps "+" literal, as SigV4 clients encode it.
		if dk, err := url.PathUnescape(k); err == nil {
			k = dk
		}
		if dv, err := url.PathUnescape(v); err == nil {
			v = dv
		}
		pairs = append(pairs, pair{uriEncode(k, true), uriEncode(v, true)})
	}
	slices.SortFunc(pairs, func(a, b pair) int {
		if c := strings.Compare(a.k, b.k); c != 0 {
			return c
		}
		return strings.Compare(a.v, b.v)
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.k + "=" + p.v
	}
	return strings.Join(out, "&")
}

// headerValue joins a header's values and collapses runs of spaces. net/http
// may keep Host, Transfer-Encoding, and Content-Length only in fields.
func headerValue(r *http.Request, name string) string {
	var values []string
	switch name {
	case "host":
		values = []string{r.Host}
	case "transfer-encoding":
		values = r.TransferEncoding
	case "content-length":
		values = slices.Clone(r.Header.Values(name))
		if len(values) == 0 && r.ContentLength > 0 {
			values = []string{strconv.FormatInt(r.ContentLength, 10)}
		}
	default:
		values = slices.Clone(r.Header.Values(name)) // Values shares the header's slice
	}
	for i, v := range values {
		values[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(values, ",")
}

// uriEncode percent-encodes every byte except unreserved characters, and
// "/" unless encodeSlash is set.
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func signingKey(secret, date, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, "s3")
	return hmacSHA256(k, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// hashingBody hashes the body as it is read and fails the read that reaches
// EOF when the hash does not match.
type hashingBody struct {
	body io.ReadCloser
	h    hash.Hash
	want []byte
}

func (b *hashingBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	b.h.Write(p[:n])
	if errors.Is(err, io.EOF) && !hmac.Equal(b.h.Sum(nil), b.want) {
		return n, ErrContentSHA256Mismatch
	}
	return n, err
}

func (b *hashingBody) Close() error { return b.body.Close() }
