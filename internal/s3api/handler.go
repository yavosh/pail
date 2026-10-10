// Package s3api serves the S3 HTTP API.
package s3api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/sigv4"
)

// Options configures the S3 handler.
type Options struct {
	// Domain turns on virtual-hosted-style requests for <bucket>.<Domain>.
	Domain string
	// AccessKeyID and SecretAccessKey are the only credentials pail accepts.
	AccessKeyID     string
	SecretAccessKey string
	// Region is reported by GetBucketLocation and x-amz-bucket-region.
	Region string
	Store  Store
	// Internal adds routes to the /_pail/ mux, keyed by a ServeMux pattern. The
	// server uses it for endpoints that other services own, such as the SNS certificate.
	Internal map[string]http.Handler
}

// opHandler serves one S3 operation for the bucket and key in t.
type opHandler func(w http.ResponseWriter, r *http.Request, t target)

type handler struct {
	opts     Options
	verifier *sigv4.Verifier
	ops      map[operation]opHandler
	internal *http.ServeMux
}

// New returns the HTTP handler for the S3 API and pail's /_pail/ endpoints.
func New(opts Options) http.Handler {
	opts.Domain = normalizeDomain(opts.Domain)
	h := &handler{opts: opts, verifier: sigv4.New(opts.AccessKeyID, opts.SecretAccessKey), internal: http.NewServeMux()}
	h.routes()
	return h
}

// routes registers every handler in one place.
func (h *handler) routes() {
	// An operation without a handler here answers NotImplemented.
	h.ops = map[operation]opHandler{
		opGetBucketACL:          h.handleACL,
		opPutBucketACL:          h.handleACL,
		opGetObjectACL:          h.handleACL,
		opPutObjectACL:          h.handleACL,
		opGetBucketCors:         h.handleBucketConfiguration,
		opPutBucketCors:         h.handleBucketConfiguration,
		opDeleteBucketCors:      h.handleBucketConfiguration,
		opGetBucketLifecycle:    h.handleBucketConfiguration,
		opPutBucketLifecycle:    h.handleBucketConfiguration,
		opDeleteBucketLifecycle: h.handleBucketConfiguration,
		opPostObject:            h.handlePostObject,
		opListBuckets:           h.handleListBuckets,
		opCreateBucket:          h.handleCreateBucket,
		opHeadBucket:            h.handleHeadBucket,
		opDeleteBucket:          h.handleDeleteBucket,
		opGetBucketLocation:     h.handleGetBucketLocation,
		opPutObject:             h.handlePutObject,
		opGetObject:             h.handleGetObject,
		opHeadObject:            h.handleHeadObject,
		opDeleteObject:          h.handleDeleteObject,
		opDeleteObjects:         h.handleDeleteObjects,
		opCopyObject:            h.handleCopyObject,
		opListObjects:           h.handleListObjects,
		opListObjectsV2:         h.handleListObjectsV2,

		opCreateMultipartUpload:   h.handleCreateMultipartUpload,
		opUploadPart:              h.handleUploadPart,
		opCompleteMultipartUpload: h.handleCompleteMultipartUpload,
		opAbortMultipartUpload:    h.handleAbortMultipartUpload,
		opListParts:               h.handleListParts,
		opListMultipartUploads:    h.handleListMultipartUploads,
	}
	h.internal.HandleFunc("GET /_pail/health", handleHealth)
	for pattern, fn := range h.opts.Internal {
		h.internal.Handle(pattern, fn)
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &recorder{ResponseWriter: w, status: http.StatusOK}
	t := parseTarget(r, h.opts.Domain)
	var op operation
	if slices.Contains(strings.Split(r.URL.EscapedPath(), "/"), "..") {
		// AWS rejects literal ".." segments with an empty 400 response.
		// Return IDs on GET and DELETE; AWS front ends vary in sending them.
		op, t = "", target{}
		if r.Method == http.MethodGet || r.Method == http.MethodDelete {
			setRequestIDs(rec)
		}
		rec.WriteHeader(http.StatusBadRequest)
	} else if !t.virtualHost && strings.HasPrefix(r.URL.Path, "/_pail/") {
		// "_" is not allowed in bucket names, so path-style /_pail/ is never a bucket.
		op, t = "_pail", target{}
		setRequestIDs(rec)
		h.internal.ServeHTTP(rec, r)
	} else {
		setRequestIDs(rec)
		op = resolve(r.Method, t, r.URL.Query(), r.Header)
		if (op == opListObjects || op == opListObjectsV2) && h.opts.Store != nil {
			// AWS names the region on listings of a bucket that exists, even on an auth error.
			if _, err := h.opts.Store.HeadBucket(r.Context(), t.bucket); err == nil {
				rec.Header().Set("x-amz-bucket-region", h.opts.Region)
			}
		}
		if r.Method != http.MethodOptions && h.opts.Store != nil && t.bucket != "" {
			h.applyCORS(rec, r, t, false)
		}
		if r.Method == http.MethodOptions && t.bucket != "" {
			h.applyCORS(rec, r, t, true)
		} else if op == opPostObject {
			h.handlePostObject(rec, r, t)
		} else if err := h.verifySignature(r, t); err != nil && (!errors.Is(err, sigv4.ErrMissingAuth) || h.opts.Store == nil || !h.anonymousAllowed(r, t, op)) {
			writeError(rec, r, toAPIError(err))
		} else if fn, ok := h.ops[op]; ok {
			fn(rec, r, t)
		} else {
			writeError(rec, r, errNotImplemented)
		}
	}

	clogS3api().Info("request", "method", r.Method, "op", op, "bucket", t.bucket,
		"status", rec.status, "bytes", rec.bytes, "duration", time.Since(start))
}

func (h *handler) verifySignature(r *http.Request, t target) error {
	q := r.URL.Query()
	if r.Header.Get("Authorization") == "" && (q.Has("AWSAccessKeyId") || q.Has("Signature") || q.Has("Expires")) {
		bucket := ""
		if t.virtualHost {
			bucket = t.bucket
		}
		return h.verifier.VerifyPresignedV2(r, bucket)
	}
	return h.verifier.Verify(r)
}

func setRequestIDs(w http.ResponseWriter) {
	w.Header().Set("x-amz-request-id", strings.ToUpper(hex.EncodeToString(randomBytes(8))))
	w.Header().Set("x-amz-id-2", base64.StdEncoding.EncodeToString(randomBytes(32)))
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

// randomBytes returns n bytes from crypto/rand, which never fails on supported platforms.
func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// recorder captures the status and body size for the access log.
type recorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

// WriteHeader records only the first status, the one net/http sends.
func (rec *recorder) WriteHeader(status int) {
	if !rec.wroteHeader {
		rec.status, rec.wroteHeader = status, true
	}
	rec.ResponseWriter.WriteHeader(status)
}

func (rec *recorder) Write(b []byte) (int, error) {
	rec.wroteHeader = true
	n, err := rec.ResponseWriter.Write(b)
	rec.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rec *recorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }
