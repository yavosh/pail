package s3api

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/yavosh/pail/internal/store"
)

const maxKeys = 1000 // the default and largest page, as on AWS

// listing is one page of a bucket listing.
type listing struct {
	contents  []store.ObjectInfo
	prefixes  []string
	truncated bool
	// last is the last key or common prefix on the page; the next page
	// resumes after it.
	last string
}

// listEntries pages objects, which are sorted by key and filtered to prefix.
// With a delimiter, keys that share the part up to it collapse into one
// common prefix, and both count toward limit.
func listEntries(objects []store.ObjectInfo, prefix, delimiter, after string, limit int) listing {
	var l listing
	// An empty page must not claim truncation: there is nothing to resume from.
	if limit == 0 {
		return l
	}
	count := 0
	for _, o := range objects {
		entry, isPrefix := o.Key, false
		if delimiter != "" {
			if i := strings.Index(o.Key[len(prefix):], delimiter); i >= 0 {
				entry, isPrefix = o.Key[:len(prefix)+i+len(delimiter)], true
			}
		}
		// Every entry, key or common prefix, must sort after the marker. As on
		// AWS, a marker inside a common prefix therefore skips the whole prefix.
		if entry <= after || (isPrefix && len(l.prefixes) > 0 && l.prefixes[len(l.prefixes)-1] == entry) {
			continue
		}
		if count == limit {
			l.truncated = true
			break
		}
		count++
		if isPrefix {
			l.prefixes = append(l.prefixes, entry)
		} else {
			l.contents = append(l.contents, o)
		}
		l.last = entry
	}
	return l
}

// listContent follows the AWS element order.
type listContent struct {
	Key               string `xml:"Key"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
	Size              int64  `xml:"Size"`
	Owner             *owner `xml:"Owner,omitempty"`
	StorageClass      string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// listParams are the query parameters both listing versions share.
type listParams struct {
	prefix, delimiter string
	requested         int // max-keys as sent; AWS echoes it even above the page limit
	limit             int
	encode            func(string) string
}

func parseListParams(q url.Values) (listParams, bool) {
	p := listParams{prefix: q.Get("prefix"), delimiter: q.Get("delimiter"), requested: maxKeys, limit: maxKeys, encode: func(s string) string { return s }}
	if s := q.Get("max-keys"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return p, false
		}
		p.requested, p.limit = n, min(n, maxKeys)
	}
	switch q.Get("encoding-type") {
	case "":
	case "url":
		p.encode = urlEncode
	default:
		return p, false
	}
	return p, true
}

func (h *handler) listPage(w http.ResponseWriter, r *http.Request, t target, p listParams, after string) (listing, bool) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return listing{}, false
	}
	objects, err := h.opts.Store.ListObjects(r.Context(), t.bucket, p.prefix, after)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return listing{}, false
	}
	return listEntries(objects, p.prefix, p.delimiter, after, p.limit), true
}

func (h *handler) contentsXML(l listing, p listParams, withOwner bool) []listContent {
	var own *owner
	if withOwner {
		o := h.bucketOwner()
		own = &o
	}
	out := make([]listContent, 0, len(l.contents))
	for _, o := range l.contents {
		c := listContent{Key: p.encode(o.Key), LastModified: o.LastModified.UTC().Format(timeFormat), ETag: quoteETag(o.ETag), Size: o.Size, StorageClass: storageClassName(o.ObjectOptions), Owner: own,
			ChecksumAlgorithm: o.ChecksumAlgorithm, ChecksumType: o.ChecksumType}
		if withOwner && o.ACL != nil {
			c.Owner = &owner{ID: o.ACL.Owner.ID}
		}
		out = append(out, c)
	}
	return out
}

func prefixesXML(l listing, p listParams) []commonPrefix {
	out := make([]commonPrefix, 0, len(l.prefixes))
	for _, cp := range l.prefixes {
		out = append(out, commonPrefix{Prefix: p.encode(cp)})
	}
	return out
}

func (h *handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, t target) {
	// Fields follow the element order AWS returns.
	type response struct {
		XMLName               xml.Name       `xml:"ListBucketResult"`
		Xmlns                 string         `xml:"xmlns,attr"`
		Name                  string         `xml:"Name"`
		Prefix                string         `xml:"Prefix"`
		StartAfter            string         `xml:"StartAfter,omitempty"`
		ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
		KeyCount              int            `xml:"KeyCount"`
		MaxKeys               int            `xml:"MaxKeys"`
		Delimiter             string         `xml:"Delimiter,omitempty"`
		EncodingType          string         `xml:"EncodingType,omitempty"`
		IsTruncated           bool           `xml:"IsTruncated"`
		Contents              []listContent  `xml:"Contents"`
		CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
	}
	q := r.URL.Query()
	p, ok := parseListParams(q)
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	token := q.Get("continuation-token")
	after := q.Get("start-after")
	if token != "" {
		b, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			writeError(w, r, errInvalidArgument)
			return
		}
		after = string(b) // a token overrides start-after, as on AWS
	}
	l, ok := h.listPage(w, r, t, p, after)
	if !ok {
		return
	}
	resp := response{
		Xmlns: s3Namespace, Name: t.bucket, Prefix: p.encode(p.prefix), Delimiter: p.encode(p.delimiter),
		MaxKeys: p.requested, EncodingType: q.Get("encoding-type"), KeyCount: len(l.contents) + len(l.prefixes),
		IsTruncated: l.truncated, ContinuationToken: token, StartAfter: p.encode(q.Get("start-after")),
		Contents: h.contentsXML(l, p, strings.EqualFold(q.Get("fetch-owner"), "true")), CommonPrefixes: prefixesXML(l, p),
	}
	if l.truncated {
		resp.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(l.last))
	}
	writeXML(w, r, http.StatusOK, resp)
}

func (h *handler) handleListObjects(w http.ResponseWriter, r *http.Request, t target) {
	// Fields follow the element order AWS returns.
	type response struct {
		XMLName        xml.Name       `xml:"ListBucketResult"`
		Xmlns          string         `xml:"xmlns,attr"`
		Name           string         `xml:"Name"`
		Prefix         string         `xml:"Prefix"`
		Marker         string         `xml:"Marker"`
		NextMarker     string         `xml:"NextMarker,omitempty"`
		MaxKeys        int            `xml:"MaxKeys"`
		Delimiter      string         `xml:"Delimiter,omitempty"`
		EncodingType   string         `xml:"EncodingType,omitempty"`
		IsTruncated    bool           `xml:"IsTruncated"`
		Contents       []listContent  `xml:"Contents"`
		CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
	}
	q := r.URL.Query()
	p, ok := parseListParams(q)
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	marker := q.Get("marker")
	l, ok := h.listPage(w, r, t, p, marker)
	if !ok {
		return
	}
	resp := response{
		Xmlns: s3Namespace, Name: t.bucket, Prefix: p.encode(p.prefix), Marker: p.encode(marker),
		Delimiter: p.encode(p.delimiter), MaxKeys: p.requested, EncodingType: q.Get("encoding-type"),
		IsTruncated: l.truncated, Contents: h.contentsXML(l, p, true), CommonPrefixes: prefixesXML(l, p),
	}
	// AWS sends NextMarker only with a delimiter; without one, clients resume
	// from the last key in Contents.
	if l.truncated && p.delimiter != "" {
		resp.NextMarker = p.encode(l.last)
	}
	writeXML(w, r, http.StatusOK, resp)
}

// urlEncode is the encoding-type=url form AWS returns: query escaping, so a
// space becomes "+", but with "/" left as is.
func urlEncode(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "%2F", "/")
}
