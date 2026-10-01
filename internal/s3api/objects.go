package s3api

import (
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yavosh/pail/internal/store"
)

const (
	maxObjectSize   = 5 << 30 // a single PutObject, as on AWS
	maxUserMetadata = 2 << 10 // bytes of x-amz-meta-* names and values
	maxKeyLen       = 1024
	defaultType     = "binary/octet-stream"
	userMetaPrefix  = "X-Amz-Meta-"
)

// storedHeaders are the system headers kept with an object and returned on reads.
var storedHeaders = []string{"Content-Type", "Content-Encoding", "Content-Disposition", "Content-Language", "Cache-Control", "Expires"}

// responseOverrides map response-* query parameters to the headers they set.
var responseOverrides = map[string]string{
	"response-content-type":        "Content-Type",
	"response-content-language":    "Content-Language",
	"response-expires":             "Expires",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
}

func (h *handler) handlePutObject(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	if r.ContentLength < 0 {
		writeError(w, r, errMissingContentLength)
		return
	}
	if r.ContentLength > maxObjectSize {
		writeError(w, r, errEntityTooLarge)
		return
	}
	opts := store.PutOptions{Metadata: map[string]string{}}
	for _, name := range storedHeaders {
		if v := r.Header.Get(name); v != "" {
			opts.Metadata[name] = v
		}
	}
	if opts.Metadata["Content-Type"] == "" {
		opts.Metadata["Content-Type"] = defaultType
	}
	userSize := 0
	for name, values := range r.Header {
		if suffix, ok := strings.CutPrefix(name, userMetaPrefix); ok {
			v := strings.Join(values, ",")
			opts.Metadata[name] = v
			userSize += len(suffix) + len(v)
		}
	}
	if userSize > maxUserMetadata {
		writeError(w, r, errMetadataTooLarge)
		return
	}
	if s := r.Header.Get("Content-MD5"); s != "" {
		sum, err := base64.StdEncoding.DecodeString(s)
		if err != nil || len(sum) != 16 {
			writeError(w, r, errInvalidDigest)
			return
		}
		opts.ContentMD5 = sum
	}
	switch inm := r.Header.Get("If-None-Match"); inm {
	case "":
	case "*":
		opts.IfNoneMatch = true
	default:
		writeError(w, r, errNotImplemented) // S3 supports only If-None-Match: * on writes
		return
	}
	opts.IfMatch = r.Header.Get("If-Match")

	// The store reads the body to EOF before it commits, which completes the
	// SigV4 payload check; nothing is written to w until it returns.
	info, err := h.opts.Store.PutObject(r.Context(), t.bucket, t.key, r.Body, opts)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *handler) handleGetObject(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	f, info, err := h.opts.Store.GetObject(r.Context(), t.bucket, t.key)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	defer func() { _ = f.Close() }()

	if !h.writeObjectHeaders(w, r, info) {
		return
	}
	start, length, status := int64(0), info.Size, http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		first, last, ok, satisfiable := parseRange(spec, info.Size)
		switch {
		case ok && !satisfiable:
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(info.Size, 10))
			writeError(w, r, errInvalidRange)
			return
		case ok:
			start, length, status = first, last-first+1, http.StatusPartialContent
			w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(first, 10)+"-"+strconv.FormatInt(last, 10)+"/"+strconv.FormatInt(info.Size, 10))
		}
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(status)
	if _, err := io.CopyN(w, f, length); err != nil {
		// Headers are sent, so the client sees a short body; log the cause.
		clogS3api().Warn("object body copy failed", "bucket", t.bucket, "error", err)
	}
}

func (h *handler) handleHeadObject(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	info, err := h.opts.Store.HeadObject(r.Context(), t.bucket, t.key)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if !h.writeObjectHeaders(w, r, info) {
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (h *handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	if err := h.opts.Store.DeleteObject(r.Context(), t.bucket, t.key); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeObjectHeaders evaluates the read conditions and sets the object's
// headers. It returns false when it already answered 304 or 412.
func (h *handler) writeObjectHeaders(w http.ResponseWriter, r *http.Request, info store.ObjectInfo) bool {
	etag := quoteETag(info.ETag)
	lastModified := info.LastModified.UTC().Truncate(time.Second)
	hdr := w.Header()
	hdr.Set("ETag", etag)
	hdr.Set("Last-Modified", lastModified.Format(http.TimeFormat))

	switch checkConditions(r.Header, etag, lastModified) {
	case http.StatusPreconditionFailed:
		writeError(w, r, errPreconditionFailed)
		return false
	case http.StatusNotModified:
		w.WriteHeader(http.StatusNotModified)
		return false
	}

	hdr.Set("Accept-Ranges", "bytes")
	for name, v := range info.Metadata {
		hdr.Set(name, v)
	}
	if hdr.Get("Content-Type") == "" {
		hdr.Set("Content-Type", defaultType)
	}
	q := r.URL.Query()
	for param, name := range responseOverrides {
		if v := q.Get(param); v != "" {
			hdr.Set(name, v)
		}
	}
	return true
}

// checkConditions applies RFC 9110 precedence: If-Match decides over
// If-Unmodified-Since, and If-None-Match over If-Modified-Since. It returns
// 0 to serve the object, 304, or 412.
func checkConditions(h http.Header, etag string, lastModified time.Time) int {
	if im := h.Get("If-Match"); im != "" {
		if !etagListMatches(im, etag) {
			return http.StatusPreconditionFailed
		}
	} else if t, err := http.ParseTime(h.Get("If-Unmodified-Since")); err == nil && lastModified.After(t) {
		return http.StatusPreconditionFailed
	}
	if inm := h.Get("If-None-Match"); inm != "" {
		if etagListMatches(inm, etag) {
			return http.StatusNotModified
		}
	} else if t, err := http.ParseTime(h.Get("If-Modified-Since")); err == nil && !lastModified.After(t) {
		return http.StatusNotModified
	}
	return 0
}

// etagListMatches reports whether a comma-separated If-Match or If-None-Match
// value names etag. S3 has no weak ETags, so W/ is ignored.
func etagListMatches(list, etag string) bool {
	for v := range strings.SplitSeq(list, ",") {
		v = strings.TrimPrefix(strings.TrimSpace(v), "W/")
		if v == "*" || v == etag || quoteETag(v) == etag {
			return true
		}
	}
	return false
}

// parseRange reads a single-range "bytes=" header for an object of size.
// ok is false for anything S3 ignores, such as a malformed or multi-range
// header; satisfiable is false when the range starts past the end.
func parseRange(spec string, size int64) (first, last int64, ok, satisfiable bool) {
	r, found := strings.CutPrefix(spec, "bytes=")
	if !found || strings.Contains(r, ",") {
		return 0, 0, false, false
	}
	a, b, found := strings.Cut(strings.TrimSpace(r), "-")
	if !found {
		return 0, 0, false, false
	}
	if a == "" { // suffix: the last b bytes
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, false, false
		}
		if n == 0 || size == 0 {
			return 0, 0, true, false
		}
		return max(size-n, 0), size - 1, true, true
	}
	first, err := strconv.ParseInt(a, 10, 64)
	if err != nil || first < 0 {
		return 0, 0, false, false
	}
	last = size - 1
	if b != "" {
		if last, err = strconv.ParseInt(b, 10, 64); err != nil || last < first {
			return 0, 0, false, false
		}
		last = min(last, size-1)
	}
	if first >= size {
		return 0, 0, true, false
	}
	return first, last, true, true
}

// checkObjectTarget validates the bucket name and key for an object request.
func checkObjectTarget(t target) (apiError, bool) {
	switch {
	case !validBucketName(t.bucket):
		return errInvalidBucketName, false
	case len(t.key) > maxKeyLen:
		return errKeyTooLong, false
	case !utf8.ValidString(t.key):
		return errInvalidArgument, false
	}
	return apiError{}, true
}

func quoteETag(etag string) string {
	if strings.HasPrefix(etag, `"`) {
		return etag
	}
	return `"` + etag + `"`
}
