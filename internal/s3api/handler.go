// Package s3api serves the S3 HTTP API.
package s3api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net/http"
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
		opListBuckets:       h.handleListBuckets,
		opCreateBucket:      h.handleCreateBucket,
		opHeadBucket:        h.handleHeadBucket,
		opDeleteBucket:      h.handleDeleteBucket,
		opGetBucketLocation: h.handleGetBucketLocation,
		opPutObject:         h.handlePutObject,
		opGetObject:         h.handleGetObject,
		opHeadObject:        h.handleHeadObject,
		opDeleteObject:      h.handleDeleteObject,
		opListObjects:       h.handleListObjects,
		opListObjectsV2:     h.handleListObjectsV2,
	}
	h.internal.HandleFunc("GET /_pail/health", handleHealth)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("x-amz-request-id", strings.ToUpper(hex.EncodeToString(randomBytes(8))))
	w.Header().Set("x-amz-id-2", base64.StdEncoding.EncodeToString(randomBytes(32)))
	rec := &recorder{ResponseWriter: w, status: http.StatusOK}

	t := parseTarget(r, h.opts.Domain)
	var op operation
	if !t.virtualHost && strings.HasPrefix(r.URL.Path, "/_pail/") {
		// "_" is not allowed in bucket names, so path-style /_pail/ is never a bucket.
		op, t = "_pail", target{}
		h.internal.ServeHTTP(rec, r)
	} else {
		op = resolve(r.Method, t, r.URL.Query(), r.Header)
		if err := h.verifier.Verify(r); err != nil {
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
