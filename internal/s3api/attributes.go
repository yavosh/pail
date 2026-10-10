package s3api

import (
	"encoding/xml"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/store"
)

// objectAttributes are the names x-amz-object-attributes accepts.
var objectAttributes = map[string]bool{
	"ETag": true, "Checksum": true, "ObjectParts": true, "StorageClass": true, "ObjectSize": true,
}

// attributeSet reads the comma-separated x-amz-object-attributes headers.
func attributeSet(h http.Header) (map[string]bool, apiError, bool) {
	set := map[string]bool{}
	for _, v := range h.Values("x-amz-object-attributes") {
		for name := range strings.SplitSeq(v, ",") {
			if name = strings.TrimSpace(name); name == "" {
				continue
			}
			if !objectAttributes[name] {
				return nil, errInvalidArgument, false
			}
			set[name] = true
		}
	}
	if len(set) == 0 {
		return nil, errMissingAttributes, false
	}
	return set, apiError{}, true
}

type attrChecksum struct {
	checksumFields
	ChecksumType string `xml:"ChecksumType,omitempty"`
}

type attrPart struct {
	PartNumber int   `xml:"PartNumber"`
	Size       int64 `xml:"Size"`
	checksumFields
}

type attrParts struct {
	PartsCount           int        `xml:"PartsCount"`
	PartNumberMarker     int        `xml:"PartNumberMarker"`
	NextPartNumberMarker int        `xml:"NextPartNumberMarker"`
	MaxParts             int        `xml:"MaxParts"`
	IsTruncated          bool       `xml:"IsTruncated"`
	Parts                []attrPart `xml:"Part"`
}

// pageObjectParts returns the parts after marker, at most limit of them. It
// pages as ListParts does: requested is the max-parts value as sent.
func pageObjectParts(parts []store.ObjectPart, marker, requested, limit int) *attrParts {
	page := &attrParts{PartsCount: len(parts), PartNumberMarker: marker, MaxParts: requested, Parts: []attrPart{}}
	for _, p := range parts {
		if p.PartNumber <= marker {
			continue
		}
		if len(page.Parts) == limit {
			page.IsTruncated = limit > 0
			break
		}
		page.Parts = append(page.Parts, attrPart{PartNumber: p.PartNumber, Size: p.Size, checksumFields: newChecksumFields(p.ChecksumAlgorithm, p.Checksum)})
		page.NextPartNumberMarker = p.PartNumber
	}
	return page
}

func (h *handler) handleGetObjectAttributes(w http.ResponseWriter, r *http.Request, t target) {
	// Fields follow the element order AWS returns.
	type response struct {
		XMLName      xml.Name      `xml:"GetObjectAttributesResponse"`
		Xmlns        string        `xml:"xmlns,attr"`
		ETag         string        `xml:"ETag,omitempty"`
		Checksum     *attrChecksum `xml:"Checksum"`
		ObjectParts  *attrParts    `xml:"ObjectParts"`
		StorageClass string        `xml:"StorageClass,omitempty"`
		ObjectSize   *int64        `xml:"ObjectSize"`
	}
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	want, apiErr, ok := attributeSet(r.Header)
	if !ok {
		writeError(w, r, apiErr)
		return
	}
	requested, limit, ok := pageSize(r.Header.Get("x-amz-max-parts"))
	marker := 0
	if s := r.Header.Get("x-amz-part-number-marker"); ok && s != "" {
		var n int64
		n, ok = parseDigits(s)
		marker = int(min(n, math.MaxInt32))
	}
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	info, err := h.opts.Store.HeadObjectVersion(r.Context(), t.bucket, t.key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeReadError(w, r, err)
		return
	}
	h.setVersionHeader(w, r, t.bucket, info)

	resp := response{Xmlns: s3Namespace}
	if want["ETag"] {
		resp.ETag = info.ETag
	}
	if want["Checksum"] && info.Checksum != "" {
		// A composite value ends in -N, the part count; AWS leaves it out here.
		value, _, _ := strings.Cut(info.Checksum, "-")
		resp.Checksum = &attrChecksum{checksumFields: newChecksumFields(info.ChecksumAlgorithm, value), ChecksumType: info.ChecksumType}
	}
	if want["ObjectParts"] && len(info.Parts) > 0 {
		resp.ObjectParts = pageObjectParts(info.Parts, marker, requested, limit)
	}
	if want["StorageClass"] {
		resp.StorageClass = storageClassName(info.ObjectOptions)
	}
	if want["ObjectSize"] {
		resp.ObjectSize = &info.Size
	}
	w.Header().Set("Last-Modified", info.LastModified.UTC().Truncate(time.Second).Format(http.TimeFormat))
	body, err := xml.Marshal(resp)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	// AWS sends no Content-Type here; a nil value stops net/http from sniffing one.
	w.Header()["Content-Type"] = nil
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append([]byte(xml.Header), body...))
}
