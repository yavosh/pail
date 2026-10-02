package s3api

import (
	"encoding/xml"
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
	if src.bucket == t.bucket && src.key == t.key && !replace {
		writeError(w, r, errCopyToSelf)
		return
	}
	opts := store.PutOptions{}
	if replace {
		if opts.Metadata, apiErr, ok = requestMetadata(r.Header, false); !ok {
			writeError(w, r, apiErr)
			return
		}
	}
	if values, ok := r.Header["X-Amz-Checksum-Algorithm"]; ok {
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
	cond := http.Header{}
	for name, as := range copyConditions {
		if v := r.Header.Get(name); v != "" {
			cond.Set(as, v)
		}
	}
	// AWS ignores a future if-modified-since on a copy; RFC 7232 calls it invalid.
	if t, err := http.ParseTime(cond.Get("If-Modified-Since")); err == nil && t.After(time.Now()) {
		cond.Del("If-Modified-Since")
	}
	if checkConditions(cond, quoteETag(info.ETag), info.LastModified.UTC().Truncate(time.Second)) != 0 {
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
	writeXML(w, r, http.StatusOK, resp)
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
