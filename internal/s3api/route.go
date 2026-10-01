package s3api

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

// operation names an S3 API operation. The values match the AWS names.
type operation string

const (
	opListBuckets             operation = "ListBuckets"
	opCreateBucket            operation = "CreateBucket"
	opHeadBucket              operation = "HeadBucket"
	opDeleteBucket            operation = "DeleteBucket"
	opGetBucketLocation       operation = "GetBucketLocation"
	opListObjects             operation = "ListObjects"
	opListObjectsV2           operation = "ListObjectsV2"
	opListMultipartUploads    operation = "ListMultipartUploads"
	opDeleteObjects           operation = "DeleteObjects"
	opPutObject               operation = "PutObject"
	opCopyObject              operation = "CopyObject"
	opGetObject               operation = "GetObject"
	opHeadObject              operation = "HeadObject"
	opDeleteObject            operation = "DeleteObject"
	opCreateMultipartUpload   operation = "CreateMultipartUpload"
	opUploadPart              operation = "UploadPart"
	opCompleteMultipartUpload operation = "CompleteMultipartUpload"
	opAbortMultipartUpload    operation = "AbortMultipartUpload"
	opListParts               operation = "ListParts"
)

// subresources are the query keys that select an S3 operation rather than
// parameterize one. A request carrying one that no table row uses is unsupported.
var subresources = map[string]bool{
	"acl": true, "accelerate": true, "analytics": true, "attributes": true,
	"cors": true, "delete": true, "encryption": true, "intelligent-tiering": true,
	"inventory": true, "legal-hold": true, "lifecycle": true, "location": true,
	"logging": true, "metrics": true, "notification": true, "object-lock": true,
	"ownershipControls": true, "policy": true, "policyStatus": true,
	"publicAccessBlock": true, "replication": true, "requestPayment": true,
	"restore": true, "retention": true, "select": true, "session": true,
	"tagging": true, "torrent": true, "uploadId": true, "uploads": true,
	"versionId": true, "versioning": true, "versions": true, "website": true,
}

// target is the bucket and key a request addresses.
type target struct {
	bucket      string
	key         string
	virtualHost bool
}

// parseTarget reads the bucket from the host when it is a subdomain of domain,
// else from the first path segment. r.URL.Path is decoded but never cleaned.
func parseTarget(r *http.Request, domain string) target {
	host := strings.TrimSuffix(hostOnly(strings.ToLower(r.Host)), ".")
	if domain != "" {
		if bucket, ok := strings.CutSuffix(host, "."+domain); ok && bucket != "" {
			return target{bucket: bucket, key: strings.TrimPrefix(r.URL.Path, "/"), virtualHost: true}
		}
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return target{bucket: bucket, key: key}
}

// hostOnly drops the port from host, if there is one.
func hostOnly(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

// normalizeDomain lets --domain be given as "localhost", ".localhost", or "localhost:9000".
func normalizeDomain(domain string) string {
	return strings.Trim(hostOnly(strings.ToLower(domain)), ".")
}

// resolve is pail's whole S3 operation table. It returns "" for anything
// unsupported, so the caller answers NotImplemented.
func resolve(method string, t target, q url.Values, h http.Header) operation {
	var sub []string
	for k := range q {
		if subresources[k] {
			sub = append(sub, k)
		}
	}
	has := func(k string) bool { _, ok := q[k]; return ok }
	// only reports whether the subresources present are exactly want.
	only := func(want ...string) bool {
		if len(sub) != len(want) {
			return false
		}
		for _, w := range want {
			if !has(w) {
				return false
			}
		}
		return true
	}
	copySource := h.Get("x-amz-copy-source") != ""

	switch {
	case t.bucket == "":
		if method == http.MethodGet && only() && t.key == "" {
			return opListBuckets
		}
	case t.key == "":
		switch {
		case method == http.MethodPut && only():
			return opCreateBucket
		case method == http.MethodHead && only():
			return opHeadBucket
		case method == http.MethodDelete && only():
			return opDeleteBucket
		case method == http.MethodGet && only("location"):
			return opGetBucketLocation
		case method == http.MethodGet && only("uploads"):
			return opListMultipartUploads
		case method == http.MethodGet && only() && q.Get("list-type") == "2":
			return opListObjectsV2
		case method == http.MethodGet && only():
			return opListObjects
		case method == http.MethodPost && only("delete"):
			return opDeleteObjects
		}
	default:
		part := has("partNumber")
		switch {
		case method == http.MethodPut && only("uploadId") && part && !copySource:
			return opUploadPart
		case method == http.MethodPut && only() && !part && copySource:
			return opCopyObject
		case method == http.MethodPut && only() && !part:
			return opPutObject
		case method == http.MethodGet && only("uploadId"):
			return opListParts
		case method == http.MethodGet && only() && !part:
			return opGetObject
		case method == http.MethodHead && only() && !part:
			return opHeadObject
		case method == http.MethodDelete && only("uploadId"):
			return opAbortMultipartUpload
		case method == http.MethodDelete && only():
			return opDeleteObject
		case method == http.MethodPost && only("uploads"):
			return opCreateMultipartUpload
		case method == http.MethodPost && only("uploadId"):
			return opCompleteMultipartUpload
		}
	}
	return ""
}
