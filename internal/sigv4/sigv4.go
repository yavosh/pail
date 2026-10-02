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
	ErrMalformedPresign      = errors.New("malformed presigned URL")
	ErrRequestExpired        = errors.New("presigned URL has expired")
	ErrUnsupportedAuth       = errors.New("unsupported authorization mechanism")
	ErrMalformedAuth         = errors.New("malformed authorization header")
	ErrUnsignedHeader        = errors.New("x-amz header present but not signed")
	ErrInvalidAccessKeyID    = errors.New("unknown access key")
	ErrSignatureMismatch     = errors.New("signature does not match")
	ErrRequestTimeTooSkewed  = errors.New("request time too skewed")
	ErrMissingContentSHA256  = errors.New("missing x-amz-content-sha256")
	ErrContentSHA256Mismatch = errors.New("x-amz-content-sha256 does not match the body")
	ErrMissingDecodedLength  = errors.New("missing or invalid x-amz-decoded-content-length")
	ErrMalformedChunk        = errors.New("malformed aws-chunked body")
	ErrChunkTooSmall         = errors.New("aws-chunked chunk too small")
	// ErrNotImplemented marks valid requests pail cannot verify yet.
	ErrNotImplemented = errors.New("signing mode not implemented")
)

const (
	algorithm     = "AWS4-HMAC-SHA256"
	timeFormat    = "20060102T150405Z"
	maxSkew       = 15 * time.Minute
	unsignedHash  = "UNSIGNED-PAYLOAD"
	streamingMode = "STREAMING-"
	maxExpires    = 7 * 24 * 60 * 60 // seconds, the longest a presigned URL may live

	streamingSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	streamingSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// Verifier checks signatures against one set of access keys.
type Verifier struct {
	secrets map[string]string
}

// New returns a Verifier that accepts the given access key ID and secret.
func New(accessKeyID, secretAccessKey string) *Verifier {
	return &Verifier{secrets: map[string]string{accessKeyID: secretAccessKey}}
}

// Verify checks the Authorization header of r, or a presigned query. For a
// SHA-256 payload hash or an aws-chunked body it wraps r.Body, so a bad body
// fails a read by EOF; do not commit before then. aws-chunked fills r.Trailer.
func (v *Verifier) Verify(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		if q := r.URL.Query(); q.Has("X-Amz-Algorithm") || q.Has("X-Amz-Signature") {
			return v.verifyPresigned(r, q)
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

	if err := checkSignedHeaders(r, a.signedHeaders); err != nil {
		return err
	}

	amzDate := r.Header.Get("X-Amz-Date")
	signedAt, err := parseSigningTime(amzDate, a.date)
	if err != nil {
		return err
	}
	if skew := time.Since(signedAt); skew > maxSkew || skew < -maxSkew {
		return ErrRequestTimeTooSkewed
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	streaming := payloadHash == streamingSigned || payloadHash == streamingUnsignedTrailer || payloadHash == streamingSignedTrailer
	switch {
	case payloadHash == "":
		return ErrMissingContentSHA256
	case streaming:
	case strings.HasPrefix(payloadHash, streamingMode):
		return fmt.Errorf("%s: %w", payloadHash, ErrNotImplemented)
	case payloadHash != unsignedHash && !isSHA256Hex(payloadHash):
		return fmt.Errorf("x-amz-content-sha256 %q: %w", payloadHash, ErrMalformedAuth)
	}

	scope := strings.Join([]string{a.date, a.region, "s3", "aws4_request"}, "/")
	creq := canonicalRequest(r, r.URL.RawQuery, a.signedHeaders, payloadHash)
	key := signingKey(secret, a.date, a.region)
	if want := sign(key, amzDate, scope, creq); !hmac.Equal([]byte(want), []byte(a.signature)) {
		return ErrSignatureMismatch
	}

	if streaming {
		decodedLen, err := DecodedLength(r.Header)
		if err != nil {
			return err
		}
		r.Trailer = http.Header{}
		body := r.Body
		if body == nil {
			body = http.NoBody
		}
		r.Body = newChunkedReader(body, chunkParams{
			signed:     payloadHash != streamingUnsignedTrailer,
			trailers:   payloadHash != streamingSigned,
			key:        key,
			amzDate:    amzDate,
			scope:      scope,
			seed:       a.signature,
			decodedLen: decodedLen,
			declared:   declaredTrailers(r.Header),
			trailer:    r.Trailer,
		})
		return nil
	}
	if payloadHash != unsignedHash && r.Body != nil {
		want, _ := hex.DecodeString(payloadHash)
		r.Body = &hashingBody{body: r.Body, h: sha256.New(), want: want}
	}
	return nil
}

// IsStreaming reports whether h names an aws-chunked payload.
func IsStreaming(h http.Header) bool {
	return strings.HasPrefix(h.Get("X-Amz-Content-Sha256"), streamingMode)
}

// DecodedLength parses x-amz-decoded-content-length, the size of an
// aws-chunked payload once decoded.
func DecodedLength(h http.Header) (int64, error) {
	values := h.Values("X-Amz-Decoded-Content-Length")
	if len(values) != 1 {
		return 0, ErrMissingDecodedLength
	}
	v := values[0]
	if v == "" || strings.Trim(v, "0123456789") != "" {
		return 0, ErrMissingDecodedLength
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, ErrMissingDecodedLength
	}
	return n, nil
}

// declaredTrailers lists the lowercase names in x-amz-trailer.
func declaredTrailers(h http.Header) []string {
	var names []string
	for _, v := range h.Values("X-Amz-Trailer") {
		for name := range strings.SplitSeq(v, ",") {
			if name = strings.ToLower(strings.TrimSpace(name)); name != "" {
				names = append(names, name)
			}
		}
	}
	return names
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
	return newAuthorization(fields["Credential"], fields["SignedHeaders"], fields["Signature"])
}

// newAuthorization validates the three fields that the header and the query
// string of a presigned URL both carry.
func newAuthorization(credential, signedHeaders, signature string) (authorization, error) {
	cred := strings.Split(credential, "/")
	if len(cred) != 5 || cred[0] == "" || cred[3] != "s3" || cred[4] != "aws4_request" {
		return authorization{}, fmt.Errorf("credential %q: %w", credential, ErrMalformedAuth)
	}
	if signedHeaders == "" || signature == "" {
		return authorization{}, ErrMalformedAuth
	}
	headers := strings.Split(signedHeaders, ";")
	if !slices.Contains(headers, "host") {
		return authorization{}, fmt.Errorf("host is not signed: %w", ErrMalformedAuth)
	}
	return authorization{
		accessKeyID:   cred[0],
		date:          cred[1],
		region:        cred[2],
		signedHeaders: headers,
		signature:     signature,
	}, nil
}

// checkSignedHeaders fails on an x-amz header that the signature does not
// cover. A replayed request must not gain headers, such as x-amz-copy-source,
// that change what it does. The SDKs sign every x-amz header they send.
func checkSignedHeaders(r *http.Request, signedHeaders []string) error {
	for name := range r.Header {
		if lower := strings.ToLower(name); strings.HasPrefix(lower, "x-amz-") && !slices.Contains(signedHeaders, lower) {
			return fmt.Errorf("%s: %w", lower, ErrUnsignedHeader)
		}
	}
	return nil
}

// parseSigningTime parses an X-Amz-Date value and checks it against the
// credential scope date.
func parseSigningTime(amzDate, scopeDate string) (time.Time, error) {
	signedAt, err := time.Parse(timeFormat, amzDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("x-amz-date %q: %w", amzDate, ErrMalformedAuth)
	}
	if day := signedAt.Format("20060102"); day != scopeDate {
		return time.Time{}, fmt.Errorf("credential date %s does not match request date %s: %w", scopeDate, day, ErrMalformedAuth)
	}
	return signedAt, nil
}

// presignParams are the query parameters every presigned URL carries.
var presignParams = []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature"}

// verifyPresigned checks the query-string signature of r. The payload is
// never signed, so r.Body stays as it is.
func (v *Verifier) verifyPresigned(r *http.Request, q url.Values) error {
	p := map[string]string{}
	for _, name := range presignParams {
		vals := q[name]
		if len(vals) != 1 || vals[0] == "" {
			return fmt.Errorf("%s: %w", name, ErrMalformedPresign)
		}
		p[name] = vals[0]
	}
	if p["X-Amz-Algorithm"] != algorithm {
		return fmt.Errorf("x-amz-algorithm %q: %w", p["X-Amz-Algorithm"], ErrMalformedPresign)
	}
	// Digits only: ParseInt would also take a sign.
	expires, err := strconv.Atoi(p["X-Amz-Expires"])
	if err != nil || strings.Trim(p["X-Amz-Expires"], "0123456789") != "" || expires < 1 || expires > maxExpires {
		return fmt.Errorf("x-amz-expires %q: %w", p["X-Amz-Expires"], ErrMalformedPresign)
	}
	// The shared checks fail with ErrMalformedAuth, which names a header
	// that a presigned URL does not have.
	a, err := newAuthorization(p["X-Amz-Credential"], p["X-Amz-SignedHeaders"], p["X-Amz-Signature"])
	if err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedPresign)
	}
	secret, ok := v.secrets[a.accessKeyID]
	if !ok {
		return ErrInvalidAccessKeyID
	}
	if err := checkSignedHeaders(r, a.signedHeaders); err != nil {
		return err
	}
	amzDate := p["X-Amz-Date"]
	signedAt, err := parseSigningTime(amzDate, a.date)
	if err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrMalformedPresign)
	}
	now := time.Now()
	if signedAt.Sub(now) > maxSkew {
		return ErrRequestTimeTooSkewed
	}
	if now.After(signedAt.Add(time.Duration(expires) * time.Second)) {
		return ErrRequestExpired
	}

	scope := strings.Join([]string{a.date, a.region, "s3", "aws4_request"}, "/")
	creq := canonicalRequest(r, dropQueryParam(r.URL.RawQuery, "X-Amz-Signature"), a.signedHeaders, unsignedHash)
	if want := sign(signingKey(secret, a.date, a.region), amzDate, scope, creq); !hmac.Equal([]byte(want), []byte(a.signature)) {
		return ErrSignatureMismatch
	}
	return nil
}

// sign returns the hex signature of a canonical request.
func sign(key []byte, amzDate, scope, creq string) string {
	sum := sha256.Sum256([]byte(creq))
	stringToSign := algorithm + "\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	return hex.EncodeToString(hmacSHA256(key, stringToSign))
}

// dropQueryParam removes the pairs named name from a raw query string.
func dropQueryParam(raw, name string) string {
	var kept []string
	for part := range strings.SplitSeq(raw, "&") {
		k, _, _ := strings.Cut(part, "=")
		if dk, err := url.QueryUnescape(k); err == nil {
			k = dk
		}
		if k != name {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, "&")
}

// canonicalRequest builds the SigV4 canonical request with the S3 rules: the
// path is encoded once and never normalized. rawQuery is the query to sign.
func canonicalRequest(r *http.Request, rawQuery string, signedHeaders []string, payloadHash string) string {
	var b strings.Builder
	b.WriteString(r.Method + "\n")
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	b.WriteString(uriEncode(path, false) + "\n")
	b.WriteString(canonicalQuery(rawQuery) + "\n")
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
		// QueryUnescape reads "+" as a space, as the SDKs and r.URL.Query() do.
		if dk, err := url.QueryUnescape(k); err == nil {
			k = dk
		}
		if dv, err := url.QueryUnescape(v); err == nil {
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
		values[i] = collapseSpaces(v)
	}
	return strings.Join(values, ",")
}

// collapseSpaces trims and collapses ASCII spaces only, as the SDK signers do.
// Other whitespace is part of the value.
func collapseSpaces(v string) string {
	var b strings.Builder
	for f := range strings.SplitSeq(strings.Trim(v, " "), " ") {
		if f == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(f)
	}
	return b.String()
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
