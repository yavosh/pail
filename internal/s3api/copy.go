package s3api

import (
	"encoding/xml"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/store"
)

// copyConditions map the copy-source headers to the read conditions they act as.
var copyConditions = map[string]string{
	"x-amz-copy-source-if-match":            "If-Match",
	"x-amz-copy-source-if-none-match":       "If-None-Match",
	"x-amz-copy-source-if-modified-since":   "If-Modified-Since",
	"x-amz-copy-source-if-unmodified-since": "If-Unmodified-Since",
}

func (h *handler) handleCopyObject(w http.ResponseWriter, r *http.Request, t target) {
	// The checksum element is named for its algorithm, such as ChecksumCRC64NVME.
	type checksumElement struct {
		XMLName xml.Name
		Value   string `xml:",chardata"`
	}
	type response struct {
		XMLName      xml.Name         `xml:"CopyObjectResult"`
		Xmlns        string           `xml:"xmlns,attr"`
		LastModified string           `xml:"LastModified"` // AWS's element order
		ETag         string           `xml:"ETag"`
		Checksum     *checksumElement `xml:",omitempty"`
		ChecksumType string           `xml:"ChecksumType,omitempty"`
	}
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	src, apiErr, ok := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if !ok {
		writeError(w, r, apiErr)
		return
	}
	var replace bool
	switch r.Header.Get("x-amz-metadata-directive") {
	case "", "COPY":
	case "REPLACE":
		replace = true
	default:
		writeError(w, r, errUnknownDirective)
		return
	}
	options, apiErr, ok := parseObjectOptions(r.Header)
	if !ok {
		writeError(w, r, apiErr)
		return
	}
	if apiErr, ok := checkExpectedOwner(r.Header, "x-amz-source-expected-bucket-owner"); !ok {
		writeError(w, r, apiErr)
		return
	}
	// A copy onto itself must change the metadata, storage class, website redirect, or encryption.
	changes := replace || r.Header.Get("x-amz-storage-class") != "" || options.ServerSideEncryption != "" || options.WebsiteRedirect != ""
	if src.bucket == t.bucket && src.key == t.key && !changes {
		writeError(w, r, errCopyToSelf)
		return
	}
	policy, apiErr, ok := h.writeACL(r, t, false)
	if !ok {
		writeError(w, r, apiErr)
		return
	}
	opts := store.PutOptions{ACL: &policy, ObjectOptions: options}
	if apiErr, ok := writeConditions(r.Header, &opts); !ok {
		w.Header().Set("Cache-Control", "no-store") // AWS adds it to this NotImplemented answer
		writeError(w, r, apiErr)
		return
	}
	if replace {
		if opts.Metadata, apiErr, ok = requestMetadata(r.Header, false); !ok {
			writeError(w, r, apiErr)
			return
		}
	}
	if values, ok := r.Header["X-Amz-Checksum-Algorithm"]; ok {
		if isUnsupportedChecksum(values[0]) {
			writeError(w, r, errNotImplemented)
			return
		}
		if opts.ChecksumAlgorithm = checksum.Canonical(values[0]); opts.ChecksumAlgorithm == "" || len(values) > 1 {
			writeError(w, r, errInvalidChecksum)
			return
		}
	}

	f, info, err := h.opts.Store.GetObject(r.Context(), src.bucket, src.key)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	defer func() { _ = f.Close() }()
	if isArchived(info.ObjectOptions) {
		writeError(w, r, errInvalidObjectState)
		return
	}
	if !copySourceMatches(r.Header, info) {
		writeError(w, r, errPreconditionFailed)
		return
	}
	if info.Size > maxObjectSize {
		writeError(w, r, errCopySourceTooLarge)
		return
	}
	if !replace {
		opts.Metadata = maps.Clone(info.Metadata)
	}
	if opts.ChecksumAlgorithm == "" {
		opts.ChecksumAlgorithm = info.ChecksumAlgorithm
	}

	dst, err := h.opts.Store.PutObject(r.Context(), t.bucket, t.key, f, opts)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	resp := response{Xmlns: s3Namespace, ETag: quoteETag(dst.ETag), LastModified: dst.LastModified.UTC().Format(timeFormat), ChecksumType: dst.ChecksumType}
	if dst.Checksum != "" {
		resp.Checksum = &checksumElement{XMLName: xml.Name{Local: "Checksum" + dst.ChecksumAlgorithm}, Value: dst.Checksum}
	}
	h.setExpiration(w, r, t.bucket, dst)
	setEncryptionHeader(w.Header(), dst.ServerSideEncryption)
	writeXML(w, r, http.StatusOK, resp)
}

// copySourceMatches reports whether the x-amz-copy-source-if-* headers hold for
// the source object.
func copySourceMatches(h http.Header, info store.ObjectInfo) bool {
	cond := http.Header{}
	for name, as := range copyConditions {
		if v := h.Get(name); v != "" {
			cond.Set(as, v)
		}
	}
	// AWS ignores a future if-modified-since on a copy; RFC 7232 calls it invalid.
	if t, err := http.ParseTime(cond.Get("If-Modified-Since")); err == nil && t.After(time.Now()) {
		cond.Del("If-Modified-Since")
	}
	return checkConditions(cond, quoteETag(info.ETag), info.LastModified.UTC().Truncate(time.Second)) == 0
}

func (h *handler) handleUploadPartCopy(w http.ResponseWriter, r *http.Request, t target) {
	type response struct {
		XMLName      xml.Name `xml:"CopyPartResult"`
		Xmlns        string   `xml:"xmlns,attr"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
		checksumFields
	}
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	if apiErr, ok := rejectSSEC(r.Header); !ok {
		writeError(w, r, apiErr)
		return
	}
	q := r.URL.Query()
	number, ok := parseDigits(q.Get("partNumber"))
	if !ok || number < 1 || number > store.MaxParts {
		writeError(w, r, errInvalidPartNumber)
		return
	}
	src, apiErr, ok := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if !ok {
		writeError(w, r, apiErr)
		return
	}
	if apiErr, ok := checkExpectedOwner(r.Header, "x-amz-source-expected-bucket-owner"); !ok {
		writeError(w, r, apiErr)
		return
	}
	f, info, err := h.opts.Store.GetObject(r.Context(), src.bucket, src.key)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	defer func() { _ = f.Close() }()
	if isArchived(info.ObjectOptions) {
		writeError(w, r, errInvalidObjectState)
		return
	}
	if !copySourceMatches(r.Header, info) {
		writeError(w, r, errPreconditionFailed)
		return
	}
	first, length := int64(0), info.Size
	if spec := r.Header.Get("x-amz-copy-source-range"); spec != "" {
		if first, length, ok = parseCopyRange(spec, info.Size); !ok {
			writeError(w, r, errInvalidArgument)
			return
		}
	}
	if length > maxObjectSize {
		writeError(w, r, errCopySourceTooLarge)
		return
	}
	if _, err := f.Seek(first, io.SeekStart); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	part, err := h.opts.Store.PutPart(r.Context(), t.bucket, t.key, q.Get("uploadId"), int(number), io.LimitReader(f, length), store.PartOptions{})
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	setEncryptionHeader(w.Header(), part.ServerSideEncryption)
	writeXML(w, r, http.StatusOK, response{
		Xmlns: s3Namespace, LastModified: part.LastModified.UTC().Format(timeFormat), ETag: quoteETag(part.ETag),
		checksumFields: newChecksumFields(part.ChecksumAlgorithm, part.Checksum),
	})
}

// parseCopyRange reads "bytes=first-last" for a source of size. Unlike a GET
// range, it must name both ends and may not extend past the last byte.
func parseCopyRange(spec string, size int64) (first, length int64, ok bool) {
	rest, found := strings.CutPrefix(spec, "bytes=")
	a, b, cut := strings.Cut(rest, "-")
	if !found || !cut {
		return 0, 0, false
	}
	first, okA := parseDigits(a)
	last, okB := parseDigits(b)
	if !okA || !okB || last < first || last >= size {
		return 0, 0, false
	}
	return first, last - first + 1, true
}

// parseCopySource reads the URL-encoded bucket/key of x-amz-copy-source. A
// literal "?" starts a query; an encoded %3F belongs to the key.
func parseCopySource(raw string) (target, apiError, bool) {
	rest, query, hasQuery := strings.Cut(strings.TrimPrefix(raw, "/"), "?")
	rest, err := url.PathUnescape(rest)
	bucket, key, _ := strings.Cut(rest, "/")
	if err != nil || bucket == "" || key == "" {
		return target{}, errInvalidCopySource, false
	}
	if hasQuery {
		q, err := url.ParseQuery(query)
		if err != nil {
			return target{}, errInvalidArgument, false
		}
		for name, values := range q {
			switch {
			case name != "versionId":
				return target{}, errInvalidArgument, false
			case slices.ContainsFunc(values, func(v string) bool { return v != "null" }):
				return target{}, errNotImplemented, false // pail has no versioning
			}
		}
	}
	src := target{bucket: bucket, key: key}
	if apiErr, ok := checkObjectTarget(src); !ok {
		return target{}, apiErr, false
	}
	return src, apiError{}, true
}
