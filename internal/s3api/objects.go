package s3api

import (
	"bytes"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs"
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
	size := r.ContentLength
	if sigv4.IsStreaming(r.Header) {
		size, _ = sigv4.DecodedLength(r.Header) // Verify checked it
	}
	if r.ContentLength < 0 {
		writeError(w, r, errMissingContentLength)
		return
	}
	if size > maxObjectSize {
		writeError(w, r, errEntityTooLarge)
		return
	}
	opts := store.PutOptions{Metadata: map[string]string{}}
	for _, name := range storedHeaders {
		if v := r.Header.Get(name); v != "" {
			opts.Metadata[name] = v
		}
	}
	// The SDKs append aws-chunked to any encoding the caller set.
	if v, found := withoutAWSChunked(r.Header); found {
		delete(opts.Metadata, "Content-Encoding")
		if v != "" {
			opts.Metadata["Content-Encoding"] = v
		}
	}
	if opts.Metadata["Content-Type"] == "" {
		opts.Metadata["Content-Type"] = defaultType
	}
	userSize := 0
	for name, values := range r.Header {
		if suffix, ok := strings.CutPrefix(name, userMetaPrefix); ok {
			if suffix == "" {
				writeError(w, r, errInvalidArgument)
				return
			}
			v := strings.Join(values, ",")
			opts.Metadata[name] = v
			userSize += len(suffix) + len(v)
		}
	}
	if userSize > maxUserMetadata {
		writeError(w, r, errMetadataTooLarge)
		return
	}
	// A present but empty Content-MD5 is invalid, not absent.
	if values, ok := r.Header["Content-Md5"]; ok {
		sum, err := base64.StdEncoding.DecodeString(values[0])
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
	var (
		inTrailer bool
		ok        bool
	)
	if opts.ChecksumAlgorithm, opts.Checksum, inTrailer, ok = parseChecksum(r.Header); !ok {
		writeError(w, r, errInvalidChecksum)
		return
	}
	var body io.Reader = r.Body
	if inTrailer {
		sum, _ := checksum.New(opts.ChecksumAlgorithm)
		body = &trailerChecksum{r: r, h: sum, algorithm: opts.ChecksumAlgorithm}
	}

	// The store reads the body to EOF before it commits, which completes the
	// SigV4 payload check; nothing is written to w until it returns.
	info, err := h.opts.Store.PutObject(r.Context(), t.bucket, t.key, body, opts)
	if errors.Is(err, store.ErrChecksumMismatch) {
		e := errChecksumMismatch
		e.Message = "The " + opts.ChecksumAlgorithm + " you specified did not match the calculated checksum."
		writeError(w, r, e)
		return
	}
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("ETag", quoteETag(info.ETag))
	setChecksumHeaders(w.Header(), info)
	w.WriteHeader(http.StatusOK)
}

// parseChecksum reads the flexible checksum of a write: at most one
// x-amz-checksum-* header with one value, or an x-amz-trailer naming one,
// optionally named by x-amz-sdk-checksum-algorithm. No checksum means the
// default algorithm. inTrailer means the value arrives after the body.
func parseChecksum(h http.Header) (algorithm string, value []byte, inTrailer, ok bool) {
	named := ""
	if v := h.Values("x-amz-sdk-checksum-algorithm"); len(v) > 0 {
		if len(v) > 1 {
			return "", nil, false, false
		}
		if named = checksum.Canonical(v[0]); named == "" {
			return "", nil, false, false
		}
	}
	found, raw := "", ""
	for _, a := range checksum.Algorithms {
		// Presence, not value: an empty checksum is invalid, not absent.
		values, present := h[http.CanonicalHeaderKey(checksum.Header(a))]
		if !present {
			continue
		}
		if found != "" || len(values) != 1 {
			return "", nil, false, false // AWS takes a single checksum per request
		}
		found, raw = a, values[0]
	}
	if values, present := h["X-Amz-Trailer"]; present {
		if found != "" || len(values) != 1 {
			return "", nil, false, false
		}
		name, isChecksum := strings.CutPrefix(strings.ToLower(strings.TrimSpace(values[0])), "x-amz-checksum-")
		found = checksum.Canonical(name)
		if !isChecksum || found == "" || named != "" && named != found {
			return "", nil, false, false
		}
		return found, nil, true, true
	}
	if found == "" {
		// AWS needs the value with a named algorithm, in a header or a trailer.
		return "", nil, false, named == ""
	}
	if named != "" && named != found {
		return "", nil, false, false
	}
	value, ok = checksum.Decode(found, raw)
	return found, value, false, ok
}

// errBadTrailerChecksum is a checksum trailer that is missing or not valid base64.
var errBadTrailerChecksum = errors.New("invalid checksum trailer")

// trailerChecksum hashes the body and, at EOF, checks the hash against the
// checksum trailer that sigv4 put in r.Trailer.
type trailerChecksum struct {
	r         *http.Request
	h         hash.Hash
	algorithm string
}

func (t *trailerChecksum) Read(p []byte) (int, error) {
	n, err := t.r.Body.Read(p)
	t.h.Write(p[:n])
	if !errors.Is(err, io.EOF) {
		return n, err
	}
	want, ok := checksum.Decode(t.algorithm, t.r.Trailer.Get(checksum.Header(t.algorithm)))
	if !ok {
		return n, errBadTrailerChecksum
	}
	if !bytes.Equal(t.h.Sum(nil), want) {
		return n, store.ErrChecksumMismatch
	}
	return n, err
}

// withoutAWSChunked returns Content-Encoding without the aws-chunked coding,
// and whether it had one.
func withoutAWSChunked(h http.Header) (string, bool) {
	var codings []string
	found := false
	for _, v := range h.Values("Content-Encoding") {
		for c := range strings.SplitSeq(v, ",") {
			switch c = strings.TrimSpace(c); {
			case strings.EqualFold(c, "aws-chunked"):
				found = true
			case c != "":
				codings = append(codings, c)
			}
		}
	}
	return strings.Join(codings, ","), found
}

// setChecksumHeaders names the object's checksum, when it has one.
func setChecksumHeaders(h http.Header, info store.ObjectInfo) {
	if info.Checksum == "" {
		return
	}
	h.Set(checksum.Header(info.ChecksumAlgorithm), info.Checksum)
	h.Set("x-amz-checksum-type", info.ChecksumType)
}

func (h *handler) handleGetObject(w http.ResponseWriter, r *http.Request, t target) {
	h.serveObject(w, r, t, true)
}

func (h *handler) handleHeadObject(w http.ResponseWriter, r *http.Request, t target) {
	h.serveObject(w, r, t, false)
}

// serveObject answers GET and HEAD alike: conditions first, then the range,
// and the object's stored headers only once the answer is a success.
func (h *handler) serveObject(w http.ResponseWriter, r *http.Request, t target, withBody bool) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	var (
		f    vfs.File
		info store.ObjectInfo
		err  error
	)
	if withBody {
		f, info, err = h.opts.Store.GetObject(r.Context(), t.bucket, t.key)
	} else {
		info, err = h.opts.Store.HeadObject(r.Context(), t.bucket, t.key)
	}
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if f != nil {
		defer func() { _ = f.Close() }()
	}

	etag := quoteETag(info.ETag)
	lastModified := info.LastModified.UTC().Truncate(time.Second)
	hdr := w.Header()
	switch checkConditions(r.Header, etag, lastModified) {
	case http.StatusPreconditionFailed:
		writeError(w, r, errPreconditionFailed) // AWS sends no object headers with it
		return
	case http.StatusNotModified:
		// AWS sends the validators, Cache-Control, and user metadata; RFC 9110
		// adds Expires.
		hdr.Set("ETag", etag)
		hdr.Set("Last-Modified", lastModified.Format(http.TimeFormat))
		for name, v := range info.Metadata {
			if name == "Cache-Control" || name == "Expires" || strings.HasPrefix(name, userMetaPrefix) {
				hdr.Set(name, v)
			}
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}

	start, length, status := int64(0), info.Size, http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		first, last, ok, satisfiable := parseRange(spec, info.Size)
		switch {
		case ok && !satisfiable:
			writeError(w, r, errInvalidRange) // AWS sends no Content-Range with it
			return
		case ok:
			start, length, status = first, last-first+1, http.StatusPartialContent
			hdr.Set("Content-Range", "bytes "+strconv.FormatInt(first, 10)+"-"+strconv.FormatInt(last, 10)+"/"+strconv.FormatInt(info.Size, 10))
		}
	}
	if f != nil {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			hdr.Del("Content-Range")
			writeError(w, r, toAPIError(err))
			return
		}
	}

	hdr.Set("ETag", etag)
	hdr.Set("Last-Modified", lastModified.Format(http.TimeFormat))
	setObjectHeaders(hdr, r, info)
	// The stored checksum covers the whole object, so a range gets none.
	if status == http.StatusOK && strings.EqualFold(r.Header.Get("x-amz-checksum-mode"), "ENABLED") {
		setChecksumHeaders(hdr, info)
	}
	hdr.Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(status)
	if f == nil {
		return
	}
	if _, err := io.CopyN(w, f, length); err != nil {
		// Headers are sent, so the client sees a short body; log the cause.
		clogS3api().Warn("object body copy failed", "bucket", t.bucket, "error", err)
	}
}

// setObjectHeaders sets the stored headers, metadata, and response-* overrides.
func setObjectHeaders(hdr http.Header, r *http.Request, info store.ObjectInfo) {
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

// checkConditions applies RFC 9110 precedence: If-Match decides over
// If-Unmodified-Since, and If-None-Match over If-Modified-Since. It returns
// 0 to serve the object, 304, or 412.
func checkConditions(h http.Header, etag string, lastModified time.Time) int {
	if im := h.Get("If-Match"); im != "" {
		if !etagListMatches(im, etag, false) {
			return http.StatusPreconditionFailed
		}
	} else if t, err := http.ParseTime(h.Get("If-Unmodified-Since")); err == nil && lastModified.After(t) {
		return http.StatusPreconditionFailed
	}
	if inm := h.Get("If-None-Match"); inm != "" {
		if etagListMatches(inm, etag, true) {
			return http.StatusNotModified
		}
	} else if t, err := http.ParseTime(h.Get("If-Modified-Since")); err == nil && !lastModified.After(t) {
		return http.StatusNotModified
	}
	return 0
}

// etagListMatches reports whether a comma-separated ETag list names etag.
// If-Match compares strongly, so a W/ tag never matches; If-None-Match is weak.
func etagListMatches(list, etag string, weak bool) bool {
	for v := range strings.SplitSeq(list, ",") {
		v = strings.TrimSpace(v)
		if after, isWeak := strings.CutPrefix(v, "W/"); isWeak {
			if !weak {
				continue
			}
			v = after
		}
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
	const unit = "bytes="
	if len(spec) < len(unit) || !strings.EqualFold(spec[:len(unit)], unit) {
		return 0, 0, false, false
	}
	r := strings.TrimSpace(spec[len(unit):])
	if strings.Contains(r, ",") {
		return 0, 0, false, false
	}
	a, b, found := strings.Cut(r, "-")
	if !found {
		return 0, 0, false, false
	}
	if a == "" { // suffix: the last b bytes
		n, ok := parseDigits(b)
		if !ok {
			return 0, 0, false, false
		}
		if n == 0 || size == 0 {
			return 0, 0, true, false
		}
		return max(size-n, 0), size - 1, true, true
	}
	first, ok = parseDigits(a)
	if !ok {
		return 0, 0, false, false
	}
	last = size - 1
	if b != "" {
		if last, ok = parseDigits(b); !ok || last < first {
			return 0, 0, false, false
		}
		last = min(last, size-1)
	}
	if first >= size {
		return 0, 0, true, false
	}
	return first, last, true, true
}

// parseDigits parses a non-negative decimal of digits only; RFC 9110 allows
// no sign, which strconv would accept.
func parseDigits(s string) (int64, bool) {
	if s == "" || strings.Trim(s, "0123456789") != "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
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
