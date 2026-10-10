package s3api

import (
	"cmp"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/store"
)

var (
	errMethodNotAllowed = apiError{"MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed against this resource."}
	errMFADelete        = apiError{"AccessDenied", http.StatusForbidden, "MFA Delete is not supported"}
	errCopySourceMarker = apiError{"InvalidRequest", http.StatusBadRequest, "The source of a copy request may not specifically refer to a delete marker by version id."}
)

// versionLabel is how a version ID appears in headers and XML. The null version has an empty ID.
func versionLabel(id string) string { return cmp.Or(id, store.NullVersionID) }

// validVersionID reports whether id is "null" or an ID in the form pail issues: 32 lowercase hex digits.
func validVersionID(id string) bool {
	return id == store.NullVersionID || len(id) == 32 && strings.Trim(id, "0123456789abcdef") == ""
}

// versionHeader returns the version ID that a read of the bucket reports for
// an object: its ID, "null" in a bucket that has versioning, or "" in one that never had it.
func (h *handler) versionHeader(ctx context.Context, bucket, id string) string {
	if id != "" {
		return id
	}
	if status, err := h.opts.Store.BucketVersioning(ctx, bucket); err == nil && status != "" {
		return store.NullVersionID
	}
	return ""
}

// setVersionHeader sets x-amz-version-id on a read of info.
func (h *handler) setVersionHeader(w http.ResponseWriter, r *http.Request, bucket string, info store.ObjectInfo) {
	if v := h.versionHeader(r.Context(), bucket, info.VersionID); v != "" {
		w.Header().Set("x-amz-version-id", v)
	}
}

// setWriteVersion sets x-amz-version-id on a write. A null version gets none.
func setWriteVersion(w http.ResponseWriter, info store.ObjectInfo) {
	if info.VersionID != "" {
		w.Header().Set("x-amz-version-id", info.VersionID)
	}
}

// setRequestedVersion echoes the versionId a request named, as AWS does on writes to a version.
func setRequestedVersion(w http.ResponseWriter, versionID string) {
	if versionID != "" {
		w.Header().Set("x-amz-version-id", versionID)
	}
}

// writeReadError answers a failed read. A delete marker adds its headers, and
// a marker named by its version ID answers 405.
func writeReadError(w http.ResponseWriter, r *http.Request, err error) {
	m, ok := errors.AsType[*store.DeleteMarkerError](err)
	if !ok {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("x-amz-delete-marker", "true")
	w.Header().Set("x-amz-version-id", versionLabel(m.VersionID))
	if m.Specific {
		w.Header().Set("Last-Modified", m.LastModified.UTC().Truncate(time.Second).Format(http.TimeFormat))
		writeError(w, r, errMethodNotAllowed)
		return
	}
	writeError(w, r, errNoSuchKey)
}

// writeCopySourceError answers a failed read of a copy source. A delete marker
// named by its version ID is a bad request, not a missing key.
func writeCopySourceError(w http.ResponseWriter, r *http.Request, err error) {
	if m, ok := errors.AsType[*store.DeleteMarkerError](err); ok && m.Specific {
		writeError(w, r, errCopySourceMarker)
		return
	}
	writeError(w, r, toAPIError(err))
}

// setCopySourceVersion sets x-amz-copy-source-version-id for a source object in a versioned bucket.
func (h *handler) setCopySourceVersion(w http.ResponseWriter, r *http.Request, bucket string, src store.ObjectInfo) {
	if v := h.versionHeader(r.Context(), bucket, src.VersionID); v != "" {
		w.Header().Set("x-amz-copy-source-version-id", v)
	}
}

// versioningConfiguration is the VersioningConfiguration document.
type versioningConfiguration struct {
	XMLName   xml.Name `xml:"VersioningConfiguration"`
	Xmlns     string   `xml:"xmlns,attr,omitempty"`
	Status    string   `xml:"Status,omitempty"`
	MFADelete string   `xml:"MfaDelete,omitempty"`
}

func (h *handler) handleBucketVersioning(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if r.Method == http.MethodGet {
		status, err := h.opts.Store.BucketVersioning(r.Context(), t.bucket)
		if err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		doc, _ := xml.Marshal(versioningConfiguration{Xmlns: s3Namespace, Status: status})
		writeTagging(w, doc)
		return
	}
	body, e, ok := readConfiguration(w, r, false)
	if !ok {
		writeError(w, r, e)
		return
	}
	var c versioningConfiguration
	if err := decodeXMLDocument(body, &c); err != nil || c.Status != store.VersioningEnabled && c.Status != store.VersioningSuspended {
		writeError(w, r, errMalformedXML)
		return
	}
	switch c.MFADelete {
	case "", "Disabled":
	case "Enabled":
		writeError(w, r, errMFADelete)
		return
	default:
		writeError(w, r, errMalformedXML)
		return
	}
	cfg := store.BucketConfiguration{Status: c.Status}
	if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "versioning", &cfg); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// versionPage is one page of a version listing.
type versionPage struct {
	versions  []store.VersionInfo
	prefixes  []string
	truncated bool
	// nextKey and nextVersion are the markers that resume after the page.
	nextKey, nextVersion string
}

// pageVersions pages entries as listEntries does for objects. The page starts
// after the version that keyMarker and versionMarker name; without a
// versionMarker, or if it is not found, the whole key is skipped.
func pageVersions(entries []store.VersionInfo, prefix, delimiter, keyMarker, versionMarker string, limit int) versionPage {
	var p versionPage
	if limit == 0 {
		return p
	}
	skipping := versionMarker != ""
	count := 0
	for _, e := range entries {
		if e.Key == keyMarker && keyMarker != "" {
			if versionMarker == "" {
				continue
			}
			if skipping {
				skipping = versionLabel(e.VersionID) != versionMarker
				continue
			}
		}
		entry, isPrefix := e.Key, false
		if delimiter != "" {
			if i := strings.Index(e.Key[len(prefix):], delimiter); i >= 0 {
				entry, isPrefix = e.Key[:len(prefix)+i+len(delimiter)], true
			}
		}
		if entry < keyMarker || isPrefix && (entry == keyMarker || len(p.prefixes) > 0 && p.prefixes[len(p.prefixes)-1] == entry) {
			continue
		}
		if count == limit {
			p.truncated = true
			break
		}
		count++
		if isPrefix {
			p.prefixes = append(p.prefixes, entry)
			p.nextKey, p.nextVersion = entry, ""
		} else {
			p.versions = append(p.versions, e)
			p.nextKey, p.nextVersion = e.Key, versionLabel(e.VersionID)
		}
	}
	return p
}

// versionEntry and markerEntry are the Version and DeleteMarker elements,
// in the order AWS writes their children.
type versionEntry struct {
	XMLName           xml.Name `xml:"Version"`
	Key               string   `xml:"Key"`
	VersionID         string   `xml:"VersionId"`
	IsLatest          bool     `xml:"IsLatest"`
	LastModified      string   `xml:"LastModified"`
	ETag              string   `xml:"ETag"`
	ChecksumAlgorithm string   `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string   `xml:"ChecksumType,omitempty"`
	Size              int64    `xml:"Size"`
	Owner             owner    `xml:"Owner"`
	StorageClass      string   `xml:"StorageClass"`
}

type markerEntry struct {
	XMLName      xml.Name `xml:"DeleteMarker"`
	Key          string   `xml:"Key"`
	VersionID    string   `xml:"VersionId"`
	IsLatest     bool     `xml:"IsLatest"`
	LastModified string   `xml:"LastModified"`
	Owner        owner    `xml:"Owner"`
}

func (h *handler) handleListObjectVersions(w http.ResponseWriter, r *http.Request, t target) {
	// Fields follow the element order AWS returns.
	type response struct {
		XMLName             xml.Name       `xml:"ListVersionsResult"`
		Xmlns               string         `xml:"xmlns,attr"`
		Name                string         `xml:"Name"`
		Prefix              string         `xml:"Prefix"`
		KeyMarker           string         `xml:"KeyMarker"`
		VersionIDMarker     string         `xml:"VersionIdMarker"`
		NextKeyMarker       string         `xml:"NextKeyMarker,omitempty"`
		NextVersionIDMarker string         `xml:"NextVersionIdMarker,omitempty"`
		MaxKeys             int            `xml:"MaxKeys"`
		Delimiter           string         `xml:"Delimiter,omitempty"`
		EncodingType        string         `xml:"EncodingType,omitempty"`
		IsTruncated         bool           `xml:"IsTruncated"`
		Entries             []any          `xml:",any"`
		CommonPrefixes      []commonPrefix `xml:"CommonPrefixes"`
	}
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	q := r.URL.Query()
	p, ok := parseListParams(q)
	keyMarker, versionMarker := q.Get("key-marker"), q.Get("version-id-marker")
	if !ok || versionMarker != "" && (keyMarker == "" || !validVersionID(versionMarker)) {
		writeError(w, r, errInvalidArgument)
		return
	}
	all, err := h.opts.Store.ListVersions(r.Context(), t.bucket, p.prefix, keyMarker)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	page := pageVersions(all, p.prefix, p.delimiter, keyMarker, versionMarker, p.limit)
	resp := response{
		Xmlns: s3Namespace, Name: t.bucket, Prefix: p.encode(p.prefix), KeyMarker: p.encode(keyMarker), VersionIDMarker: versionMarker,
		Delimiter: p.encode(p.delimiter), MaxKeys: p.requested, EncodingType: q.Get("encoding-type"),
		IsTruncated: page.truncated, CommonPrefixes: prefixesXML(listing{prefixes: page.prefixes}, p),
	}
	if page.truncated {
		resp.NextKeyMarker, resp.NextVersionIDMarker = p.encode(page.nextKey), page.nextVersion
	}
	bucketOwner := h.bucketOwner()
	for _, v := range page.versions {
		id, lastModified := versionLabel(v.VersionID), v.LastModified.UTC().Format(timeFormat)
		if v.DeleteMarker {
			resp.Entries = append(resp.Entries, markerEntry{Key: p.encode(v.Key), VersionID: id, IsLatest: v.IsLatest, LastModified: lastModified, Owner: bucketOwner})
			continue
		}
		own := bucketOwner
		if v.ACL != nil {
			own = owner{ID: v.ACL.Owner.ID}
		}
		resp.Entries = append(resp.Entries, versionEntry{
			Key: p.encode(v.Key), VersionID: id, IsLatest: v.IsLatest, LastModified: lastModified, ETag: quoteETag(v.ETag),
			ChecksumAlgorithm: v.ChecksumAlgorithm, ChecksumType: v.ChecksumType, Size: v.Size, Owner: own, StorageClass: storageClassName(v.ObjectOptions),
		})
	}
	writeXML(w, r, http.StatusOK, resp)
}
