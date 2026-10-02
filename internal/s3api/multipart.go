package s3api

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/store"
)

const (
	maxPartsPage = 1000 // the default and largest page of ListParts and ListMultipartUploads
	// maxCompleteBody holds 10,000 parts with their checksums.
	maxCompleteBody = 8 << 20
)

// checksumFields are the per-algorithm checksum elements that parts and
// completed uploads carry, in the order AWS writes them.
type checksumFields struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

// field returns the element for algorithm, or nil for an unknown one.
func (c *checksumFields) field(algorithm string) *string {
	switch algorithm {
	case checksum.CRC32:
		return &c.ChecksumCRC32
	case checksum.CRC32C:
		return &c.ChecksumCRC32C
	case checksum.CRC64NVME:
		return &c.ChecksumCRC64NVME
	case checksum.SHA1:
		return &c.ChecksumSHA1
	case checksum.SHA256:
		return &c.ChecksumSHA256
	}
	return nil
}

func newChecksumFields(algorithm, value string) checksumFields {
	var c checksumFields
	if f := c.field(algorithm); f != nil {
		*f = value
	}
	return c
}

// sent returns the algorithm and value of the element that is set.
func (c checksumFields) sent() (algorithm, value string) {
	for _, a := range checksum.Algorithms {
		if v := *c.field(a); v != "" {
			return a, v
		}
	}
	return "", ""
}

// uploadChecksum reads the checksum algorithm and type of a new upload. Each
// algorithm allows only some types, and the first one allowed is the default.
func uploadChecksum(h http.Header) (algorithm, typ string, ok bool) {
	typ = h.Get("x-amz-checksum-type")
	if typ != "" && typ != checksum.Composite && typ != checksum.FullObject {
		return "", "", false
	}
	raw := h.Get("x-amz-checksum-algorithm")
	if raw == "" {
		return "", "", typ == ""
	}
	if algorithm = checksum.Canonical(raw); algorithm == "" {
		return "", "", false
	}
	switch algorithm {
	case checksum.CRC64NVME:
		return algorithm, checksum.FullObject, typ != checksum.Composite
	case checksum.CRC32, checksum.CRC32C:
		if typ == "" {
			typ = checksum.Composite
		}
		return algorithm, typ, true
	}
	return algorithm, checksum.Composite, typ != checksum.FullObject
}

func (h *handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, t target) {
	type response struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	metadata, apiErr, valid := requestMetadata(r.Header, false)
	if !valid {
		writeError(w, r, apiErr)
		return
	}
	algorithm, typ, ok := uploadChecksum(r.Header)
	if !ok {
		writeError(w, r, errInvalidChecksum)
		return
	}
	up, err := h.opts.Store.CreateUpload(r.Context(), t.bucket, t.key, store.UploadOptions{Metadata: metadata, ChecksumAlgorithm: algorithm, ChecksumType: typ})
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if algorithm != "" {
		w.Header().Set("x-amz-checksum-algorithm", algorithm)
		w.Header().Set("x-amz-checksum-type", typ)
	}
	body, err := xml.Marshal(response{Xmlns: s3Namespace, Bucket: t.bucket, Key: t.key, UploadID: up.ID})
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	// AWS sends no Content-Type here; a nil value stops net/http from sniffing one.
	w.Header()["Content-Type"] = nil
	_, _ = w.Write(append([]byte(xml.Header), body...))
}

func (h *handler) handleUploadPart(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	q := r.URL.Query()
	number, ok := parseDigits(q.Get("partNumber"))
	if !ok || number < 1 || number > store.MaxParts {
		writeError(w, r, errInvalidPartNumber)
		return
	}
	size, streaming := r.ContentLength, sigv4.IsStreaming(r.Header)
	if streaming {
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
	var opts store.PartOptions
	// A present but empty Content-MD5 is invalid, not absent.
	if values, ok := r.Header["Content-Md5"]; ok {
		sum, err := base64.StdEncoding.DecodeString(values[0])
		if err != nil || len(sum) != 16 {
			writeError(w, r, errInvalidDigest)
			return
		}
		opts.ContentMD5 = sum
	}
	var inTrailer bool
	// Only an aws-chunked body has trailers; fail before reading the body.
	if opts.ChecksumAlgorithm, opts.Checksum, inTrailer, ok = parseChecksum(r.Header); !ok || inTrailer && !streaming {
		writeError(w, r, errInvalidChecksum)
		return
	}
	var body io.Reader = r.Body
	if inTrailer {
		sum, _ := checksum.New(opts.ChecksumAlgorithm)
		body = &trailerChecksum{r: r, h: sum, algorithm: opts.ChecksumAlgorithm}
	}

	info, err := h.opts.Store.PutPart(r.Context(), t.bucket, t.key, q.Get("uploadId"), int(number), body, opts)
	if errors.Is(err, store.ErrChecksumMismatch) {
		writeError(w, r, checksumMismatch(opts.ChecksumAlgorithm))
		return
	}
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("ETag", quoteETag(info.ETag))
	if info.Checksum != "" {
		w.Header().Set(checksum.Header(info.ChecksumAlgorithm), info.Checksum)
	}
	w.WriteHeader(http.StatusOK)
}

func checksumMismatch(algorithm string) apiError {
	e := errChecksumMismatch
	e.Message = "The " + algorithm + " you specified did not match the calculated checksum."
	return e
}

func (h *handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, t target) {
	type part struct {
		PartNumber int `xml:"PartNumber"`
		ETag       string
		checksumFields
	}
	type request struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []part   `xml:"Part"`
	}
	type response struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
		checksumFields
		ChecksumType string `xml:"ChecksumType,omitempty"`
	}
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	var opts store.CompleteOptions
	switch inm := r.Header.Get("If-None-Match"); inm {
	case "":
	case "*":
		opts.IfNoneMatch = true
	default:
		writeError(w, r, errNotImplemented) // S3 supports only If-None-Match: * on writes
		return
	}
	opts.IfMatch = r.Header.Get("If-Match")
	// Here the header names the whole object's checksum, which only a FULL_OBJECT upload has.
	algorithm, want, inTrailer, ok := parseChecksum(r.Header)
	if !ok || inTrailer {
		writeError(w, r, errInvalidChecksum)
		return
	}
	opts.ChecksumAlgorithm, opts.FullObjectChecksum = algorithm, want

	// Reading to EOF also completes the SigV4 payload check.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCompleteBody))
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	var req request
	if err := decodeXMLDocument(body, &req); err != nil || len(req.Parts) == 0 {
		writeError(w, r, errMalformedXML)
		return
	}
	parts := make([]store.CompletePart, len(req.Parts))
	for i, p := range req.Parts {
		alg, value := p.sent()
		parts[i] = store.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag, ChecksumAlgorithm: alg, Checksum: value}
	}

	info, err := h.opts.Store.CompleteUpload(r.Context(), t.bucket, t.key, r.URL.Query().Get("uploadId"), parts, opts)
	if errors.Is(err, store.ErrChecksumMismatch) {
		writeError(w, r, checksumMismatch(algorithm))
		return
	}
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	location := url.URL{Scheme: "http", Host: r.Host, Path: "/" + t.bucket + "/" + t.key}
	if t.virtualHost {
		location.Path = "/" + t.key
	}
	if r.TLS != nil {
		location.Scheme = "https"
	}
	writeXML(w, r, http.StatusOK, response{
		Xmlns: s3Namespace, Location: location.String(), Bucket: t.bucket, Key: t.key, ETag: quoteETag(info.ETag),
		checksumFields: newChecksumFields(info.ChecksumAlgorithm, info.Checksum), ChecksumType: info.ChecksumType,
	})
}

func (h *handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	if err := h.opts.Store.AbortUpload(r.Context(), t.bucket, t.key, r.URL.Query().Get("uploadId")); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// initiator is the owner element with the DisplayName that AWS still sends for it.
type initiator struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

func (h *handler) bucketInitiator() initiator {
	id := h.bucketOwner().ID
	return initiator{ID: id, DisplayName: id}
}

// pageSize reads a max-parts or max-uploads value: the count as sent, and the
// page limit it allows.
func pageSize(s string) (requested, limit int, ok bool) {
	if s == "" {
		return maxPartsPage, maxPartsPage, true
	}
	n, ok := parseDigits(s)
	if !ok {
		return 0, 0, false
	}
	n = min(n, math.MaxInt32)
	return int(n), min(int(n), maxPartsPage), true
}

func (h *handler) handleListParts(w http.ResponseWriter, r *http.Request, t target) {
	type part struct {
		PartNumber   int    `xml:"PartNumber"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		checksumFields
	}
	// Fields follow the element order AWS returns.
	type response struct {
		XMLName      xml.Name  `xml:"ListPartsResult"`
		Xmlns        string    `xml:"xmlns,attr"`
		Bucket       string    `xml:"Bucket"`
		Key          string    `xml:"Key"`
		UploadID     string    `xml:"UploadId"`
		Initiator    initiator `xml:"Initiator"`
		Owner        owner     `xml:"Owner"`
		StorageClass string    `xml:"StorageClass"`
		// The recording has no checksum upload, so the place of these two is unverified.
		ChecksumAlgorithm    string `xml:"ChecksumAlgorithm,omitempty"`
		ChecksumType         string `xml:"ChecksumType,omitempty"`
		PartNumberMarker     int    `xml:"PartNumberMarker"`
		NextPartNumberMarker int    `xml:"NextPartNumberMarker"`
		MaxParts             int    `xml:"MaxParts"`
		IsTruncated          bool   `xml:"IsTruncated"`
		Parts                []part `xml:"Part"`
	}
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	q := r.URL.Query()
	requested, limit, ok := pageSize(q.Get("max-parts"))
	marker := 0
	if s := q.Get("part-number-marker"); ok && s != "" {
		var n int64
		n, ok = parseDigits(s)
		marker = int(min(n, math.MaxInt32))
	}
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	uploadID := q.Get("uploadId")
	up, parts, err := h.opts.Store.ListParts(r.Context(), t.bucket, t.key, uploadID)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	resp := response{
		Xmlns: s3Namespace, Bucket: t.bucket, Key: t.key, UploadID: uploadID, PartNumberMarker: marker, MaxParts: requested,
		Parts: []part{}, Initiator: h.bucketInitiator(), Owner: h.bucketOwner(), StorageClass: "STANDARD",
		ChecksumAlgorithm: up.ChecksumAlgorithm, ChecksumType: up.ChecksumType,
	}
	for _, p := range parts {
		if p.PartNumber <= marker {
			continue
		}
		if len(resp.Parts) == limit {
			resp.IsTruncated = limit > 0 // an empty page has nothing to resume from
			break
		}
		resp.Parts = append(resp.Parts, part{
			PartNumber: p.PartNumber, LastModified: p.LastModified.UTC().Format(timeFormat), ETag: quoteETag(p.ETag), Size: p.Size,
			checksumFields: newChecksumFields(p.ChecksumAlgorithm, p.Checksum),
		})
		resp.NextPartNumberMarker = p.PartNumber
	}
	writeXML(w, r, http.StatusOK, resp)
}

// uploadsPage is one page of ListMultipartUploads.
type uploadsPage struct {
	uploads   []store.UploadInfo
	prefixes  []string
	truncated bool
	// lastKey and lastID name the last entry on the page. lastID is empty
	// when it is a common prefix.
	lastKey, lastID string
}

// pageUploads pages uploads, which are sorted by key and filtered to prefix.
// It groups by delimiter as listEntries does. An entry sorts after the marker
// when its key is greater, or equal with an ID after idMarker in the listing.
func pageUploads(uploads []store.UploadInfo, prefix, delimiter, keyMarker, idMarker string, limit int) uploadsPage {
	var p uploadsPage
	if limit == 0 {
		return p
	}
	passedMarker := false
	count := 0
	for _, u := range uploads {
		entry, isPrefix := u.Key, false
		if delimiter != "" {
			if i := strings.Index(u.Key[len(prefix):], delimiter); i >= 0 {
				entry, isPrefix = u.Key[:len(prefix)+i+len(delimiter)], true
			}
		}
		switch {
		case isPrefix && (entry <= keyMarker || len(p.prefixes) > 0 && p.prefixes[len(p.prefixes)-1] == entry):
			continue
		case !isPrefix && u.Key < keyMarker:
			continue
		case !isPrefix && u.Key == keyMarker && !passedMarker:
			passedMarker = idMarker != "" && u.ID == idMarker
			continue
		}
		if count == limit {
			p.truncated = true
			break
		}
		count++
		if isPrefix {
			p.prefixes = append(p.prefixes, entry)
			p.lastKey, p.lastID = entry, ""
		} else {
			p.uploads = append(p.uploads, u)
			p.lastKey, p.lastID = u.Key, u.ID
		}
	}
	return p
}

func (h *handler) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, t target) {
	type upload struct {
		Key               string    `xml:"Key"`
		UploadID          string    `xml:"UploadId"`
		Initiator         initiator `xml:"Initiator"`
		Owner             owner     `xml:"Owner"`
		StorageClass      string    `xml:"StorageClass"`
		Initiated         string    `xml:"Initiated"`
		ChecksumAlgorithm string    `xml:"ChecksumAlgorithm,omitempty"`
		ChecksumType      string    `xml:"ChecksumType,omitempty"`
	}
	// Fields follow the element order AWS returns.
	type response struct {
		XMLName            xml.Name       `xml:"ListMultipartUploadsResult"`
		Xmlns              string         `xml:"xmlns,attr"`
		Bucket             string         `xml:"Bucket"`
		KeyMarker          string         `xml:"KeyMarker"`
		UploadIDMarker     string         `xml:"UploadIdMarker"`
		NextKeyMarker      string         `xml:"NextKeyMarker"`
		NextUploadIDMarker string         `xml:"NextUploadIdMarker"`
		Prefix             string         `xml:"Prefix,omitempty"`
		Delimiter          string         `xml:"Delimiter,omitempty"` // with CommonPrefixes, unverified: the recording has neither
		MaxUploads         int            `xml:"MaxUploads"`
		IsTruncated        bool           `xml:"IsTruncated"`
		Uploads            []upload       `xml:"Upload"`
		CommonPrefixes     []commonPrefix `xml:"CommonPrefixes"`
	}
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	q := r.URL.Query()
	requested, limit, ok := pageSize(q.Get("max-uploads"))
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	prefix, delimiter, keyMarker, idMarker := q.Get("prefix"), q.Get("delimiter"), q.Get("key-marker"), q.Get("upload-id-marker")
	uploads, err := h.opts.Store.ListUploads(r.Context(), t.bucket)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	uploads = slices.DeleteFunc(uploads, func(u store.UploadInfo) bool { return !strings.HasPrefix(u.Key, prefix) })
	page := pageUploads(uploads, prefix, delimiter, keyMarker, idMarker, limit)

	own := h.bucketOwner()
	starter := h.bucketInitiator()
	resp := response{
		Xmlns: s3Namespace, Bucket: t.bucket, KeyMarker: keyMarker, UploadIDMarker: idMarker, Prefix: prefix, Delimiter: delimiter,
		MaxUploads: requested, IsTruncated: page.truncated, Uploads: []upload{}, CommonPrefixes: []commonPrefix{},
	}
	for _, u := range page.uploads {
		resp.Uploads = append(resp.Uploads, upload{
			Key: u.Key, UploadID: u.ID, Initiator: starter, Owner: own, StorageClass: "STANDARD", Initiated: u.Initiated.UTC().Format(timeFormat),
			ChecksumAlgorithm: u.ChecksumAlgorithm, ChecksumType: u.ChecksumType,
		})
	}
	for _, p := range page.prefixes {
		resp.CommonPrefixes = append(resp.CommonPrefixes, commonPrefix{Prefix: p})
	}
	// AWS always sends the last entry's markers, even on a page that is not truncated.
	resp.NextKeyMarker, resp.NextUploadIDMarker = page.lastKey, page.lastID
	writeXML(w, r, http.StatusOK, resp)
}
