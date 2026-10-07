package sigv4

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrMalformedPresignV2 means the SigV2 query parameters are invalid.
var ErrMalformedPresignV2 = errors.New("malformed sigv2 presigned URL")

var sigV2Subresources = []string{
	"accelerate", "acl", "analytics", "attributes", "cors", "defaultObjectAcl", "delete",
	"encryption", "intelligent-tiering", "inventory", "legal-hold", "lifecycle", "location",
	"logging", "metrics", "notification", "object-lock", "ownershipControls", "partNumber",
	"policy", "policyStatus", "publicAccessBlock", "replication", "requestPayment", "restore",
	"retention", "select", "select-type", "session", "storageClass", "tagging", "torrent",
	"uploadId", "uploads", "versionId", "versioning", "versions", "website",
	"response-cache-control", "response-content-disposition", "response-content-encoding",
	"response-content-language", "response-content-type", "response-expires",
}

// VerifyPresignedV2 checks a SigV2 URL without wrapping the unsigned body.
// virtualBucket is empty for path-style requests; routing supplies it otherwise.
func (v *Verifier) VerifyPresignedV2(r *http.Request, virtualBucket string) error {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return ErrMalformedPresignV2
	}
	// AWS treats a SigV2 URL without Expires as unauthenticated.
	if !q.Has("Expires") {
		return ErrMissingAuth
	}
	for _, name := range []string{"AWSAccessKeyId", "Signature", "Expires"} {
		if values := q[name]; len(values) != 1 || values[0] == "" {
			return ErrMalformedPresignV2
		}
	}
	if slices.ContainsFunc(presignParams, q.Has) {
		return ErrUnsupportedAuth
	}
	for name, values := range q {
		if slices.Contains(sigV2Subresources, name) && len(values) != 1 {
			return ErrMalformedPresignV2
		}
	}
	for _, name := range []string{"Content-MD5", "Content-Type"} {
		if len(r.Header.Values(name)) > 1 {
			return ErrMalformedPresignV2
		}
	}
	expires, err := strconv.ParseInt(q.Get("Expires"), 10, 64)
	if err != nil || strings.Trim(q.Get("Expires"), "0123456789") != "" {
		return ErrMalformedPresignV2
	}
	secret, ok := v.secrets[q.Get("AWSAccessKeyId")]
	if !ok {
		return ErrInvalidAccessKeyID
	}
	if time.Now().Unix() > expires {
		return ErrRequestExpired
	}
	signature, err := base64.StdEncoding.DecodeString(q.Get("Signature"))
	if err != nil {
		return ErrSignatureMismatch
	}
	stringToSign := r.Method + "\n" + strings.TrimSpace(r.Header.Get("Content-MD5")) + "\n" +
		strings.TrimSpace(r.Header.Get("Content-Type")) + "\n" + q.Get("Expires") + "\n" +
		canonicalSigV2Headers(r.Header) + canonicalSigV2Resource(r.URL, q, virtualBucket)
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(stringToSign))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return ErrSignatureMismatch
	}
	if IsStreaming(r.Header) {
		return ErrNotImplemented
	}
	return nil
}

func canonicalSigV2Headers(header http.Header) string {
	headers := map[string][]string{}
	for name, values := range header {
		name = strings.ToLower(name)
		if !strings.HasPrefix(name, "x-amz-") {
			continue
		}
		for _, value := range values {
			headers[name] = append(headers[name], strings.TrimSpace(value))
		}
	}
	var b strings.Builder
	for _, name := range slices.Sorted(maps.Keys(headers)) {
		b.WriteString(name + ":" + strings.Join(headers[name], ",") + "\n")
	}
	return b.String()
}

func canonicalSigV2Resource(u *url.URL, q url.Values, virtualBucket string) string {
	resource := u.EscapedPath()
	if resource == "" {
		resource = "/"
	}
	if virtualBucket != "" {
		resource = "/" + virtualBucket + resource
	}
	var parameters []string
	for _, name := range slices.Sorted(maps.Keys(q)) {
		if !slices.Contains(sigV2Subresources, name) {
			continue
		}
		parameter := name
		if value := q.Get(name); value != "" {
			parameter += "=" + value
		}
		parameters = append(parameters, parameter)
	}
	if len(parameters) != 0 {
		resource += "?" + strings.Join(parameters, "&")
	}
	return resource
}
