package s3api

import (
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// operation names an S3 API operation. The values match the AWS names.
type operation string

const (
	opGetBucketACL            operation = "GetBucketAcl"
	opPutBucketACL            operation = "PutBucketAcl"
	opGetObjectACL            operation = "GetObjectAcl"
	opPutObjectACL            operation = "PutObjectAcl"
	opGetBucketCors           operation = "GetBucketCors"
	opPutBucketCors           operation = "PutBucketCors"
	opDeleteBucketCors        operation = "DeleteBucketCors"
	opGetBucketLifecycle      operation = "GetBucketLifecycleConfiguration"
	opPutBucketLifecycle      operation = "PutBucketLifecycleConfiguration"
	opDeleteBucketLifecycle   operation = "DeleteBucketLifecycle"
	opGetBucketTagging        operation = "GetBucketTagging"
	opPutBucketTagging        operation = "PutBucketTagging"
	opDeleteBucketTagging     operation = "DeleteBucketTagging"
	opGetObjectTagging        operation = "GetObjectTagging"
	opPutObjectTagging        operation = "PutObjectTagging"
	opDeleteObjectTagging     operation = "DeleteObjectTagging"
	opGetBucketNotification   operation = "GetBucketNotificationConfiguration"
	opPutBucketNotification   operation = "PutBucketNotificationConfiguration"
	opGetBucketOwnership      operation = "GetBucketOwnershipControls"
	opPutBucketOwnership      operation = "PutBucketOwnershipControls"
	opDeleteBucketOwnership   operation = "DeleteBucketOwnershipControls"
	opPostObject              operation = "PostObject"
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
	opGetObjectAttributes     operation = "GetObjectAttributes"
	opHeadObject              operation = "HeadObject"
	opDeleteObject            operation = "DeleteObject"
	opCreateMultipartUpload   operation = "CreateMultipartUpload"
	opUploadPart              operation = "UploadPart"
	opUploadPartCopy          operation = "UploadPartCopy"
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

// versioned lists the operations that accept a versionId. pail has no
// versioning, so only "null" is valid and it names the current object.
var versioned = map[operation]bool{
	opGetObject: true, opHeadObject: true, opDeleteObject: true, opGetObjectAttributes: true,
	opGetObjectACL: true, opPutObjectACL: true,
	opGetObjectTagging: true, opPutObjectTagging: true, opDeleteObjectTagging: true,
}

// checkVersionID rejects a versionId other than "null" on a versioned
// operation. A versioning PR changes only this function and resolve.
func checkVersionID(op operation, q url.Values) (apiError, bool) {
	if !versioned[op] || !slices.ContainsFunc(q["versionId"], func(v string) bool { return v != "null" }) {
		return apiError{}, true
	}
	return errInvalidArgument, false
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

// resolve returns the operation for a request, or "" when it is unsupported.
// A versionId is valid only on the operations in versioned.
func resolve(method string, t target, q url.Values, h http.Header) operation {
	op := resolveOperation(method, t, q, h)
	if q.Has("versionId") && !versioned[op] {
		return ""
	}
	return op
}

// resolveOperation is pail's whole S3 operation table. It returns "" for
// anything unsupported, so the caller answers NotImplemented.
func resolveOperation(method string, t target, q url.Values, h http.Header) operation {
	var sub []string
	for k := range q {
		// versionId selects no operation, it narrows one. Operations that ignore it are filtered in resolve.
		if subresources[k] && k != "versionId" {
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
		case only("acl"):
			switch method {
			case http.MethodGet:
				return opGetBucketACL
			case http.MethodPut:
				return opPutBucketACL
			}
		case only("cors"):
			switch method {
			case http.MethodGet:
				return opGetBucketCors
			case http.MethodPut:
				return opPutBucketCors
			case http.MethodDelete:
				return opDeleteBucketCors
			}
		case only("lifecycle"):
			switch method {
			case http.MethodGet:
				return opGetBucketLifecycle
			case http.MethodPut:
				return opPutBucketLifecycle
			case http.MethodDelete:
				return opDeleteBucketLifecycle
			}
		case only("tagging"):
			switch method {
			case http.MethodGet:
				return opGetBucketTagging
			case http.MethodPut:
				return opPutBucketTagging
			case http.MethodDelete:
				return opDeleteBucketTagging
			}
		case only("notification"):
			switch method {
			case http.MethodGet:
				return opGetBucketNotification
			case http.MethodPut:
				return opPutBucketNotification
			}
		case only("ownershipControls"):
			switch method {
			case http.MethodGet:
				return opGetBucketOwnership
			case http.MethodPut:
				return opPutBucketOwnership
			case http.MethodDelete:
				return opDeleteBucketOwnership
			}
		case method == http.MethodPost && only():
			return opPostObject
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
		case method == http.MethodGet && only("acl"):
			return opGetObjectACL
		case method == http.MethodPut && only("acl"):
			return opPutObjectACL
		case only("tagging"):
			switch method {
			case http.MethodGet:
				return opGetObjectTagging
			case http.MethodPut:
				return opPutObjectTagging
			case http.MethodDelete:
				return opDeleteObjectTagging
			}
		case method == http.MethodPut && only("uploadId") && part && !copySource:
			return opUploadPart
		case method == http.MethodPut && only("uploadId") && part && copySource:
			return opUploadPartCopy
		case method == http.MethodPut && only() && !part && copySource:
			return opCopyObject
		case method == http.MethodPut && only() && !part:
			return opPutObject
		case method == http.MethodGet && only("uploadId"):
			return opListParts
		case method == http.MethodGet && only("attributes"):
			return opGetObjectAttributes
		case method == http.MethodGet && only():
			return opGetObject
		case method == http.MethodHead && only():
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
