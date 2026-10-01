package s3api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs"
)

// Store is the storage the S3 handlers need. *store.Store implements it.
type Store interface {
	CreateBucket(ctx context.Context, name string) error
	HeadBucket(ctx context.Context, name string) (store.BucketInfo, error)
	ListBuckets(ctx context.Context) ([]store.BucketInfo, error)
	DeleteBucket(ctx context.Context, name string) error
	PutObject(ctx context.Context, bucket, key string, body io.Reader, opts store.PutOptions) (store.ObjectInfo, error)
	GetObject(ctx context.Context, bucket, key string) (vfs.File, store.ObjectInfo, error)
	HeadObject(ctx context.Context, bucket, key string) (store.ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix, startAfter string) ([]store.ObjectInfo, error)
}

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// timeFormat is how S3 writes dates in XML bodies.
const timeFormat = "2006-01-02T15:04:05.000Z"

// maxConfigBody bounds request bodies that carry XML configuration.
const maxConfigBody = 1 << 20

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// bucketOwner is the single account that owns every bucket. Its ID is derived
// from the access key, so it is stable across restarts.
func (h *handler) bucketOwner() owner {
	sum := sha256.Sum256([]byte(h.opts.AccessKeyID))
	return owner{ID: hex.EncodeToString(sum[:]), DisplayName: "pail"}
}

func (h *handler) handleListBuckets(w http.ResponseWriter, r *http.Request, _ target) {
	type bucket struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
		BucketRegion string `xml:"BucketRegion"`
	}
	type response struct {
		XMLName           xml.Name `xml:"ListAllMyBucketsResult"`
		Xmlns             string   `xml:"xmlns,attr"`
		Owner             owner    `xml:"Owner"`
		Buckets           []bucket `xml:"Buckets>Bucket"`
		Prefix            string   `xml:"Prefix,omitempty"`
		ContinuationToken string   `xml:"ContinuationToken,omitempty"`
	}
	q := r.URL.Query()
	prefix := q.Get("prefix")
	limit := maxBuckets
	if s := q.Get("max-buckets"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxBuckets {
			writeError(w, r, errInvalidArgument)
			return
		}
		limit = n
	}
	// The token is the last name of the previous page; names sort, so resume after it.
	after := ""
	if s := q.Get("continuation-token"); s != "" {
		b, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			writeError(w, r, errInvalidArgument)
			return
		}
		after = string(b)
	}
	list, err := h.opts.Store.ListBuckets(r.Context())
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	resp := response{Xmlns: s3Namespace, Owner: h.bucketOwner(), Buckets: []bucket{}, Prefix: prefix}
	if region := q.Get("bucket-region"); region != "" && region != h.opts.Region {
		list = nil
	}
	for _, b := range list {
		if b.Name <= after || !strings.HasPrefix(b.Name, prefix) {
			continue
		}
		if len(resp.Buckets) == limit {
			resp.ContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(resp.Buckets[len(resp.Buckets)-1].Name))
			break
		}
		resp.Buckets = append(resp.Buckets, bucket{Name: b.Name, CreationDate: b.Created.UTC().Format(timeFormat), BucketRegion: h.opts.Region})
	}
	writeXML(w, r, http.StatusOK, resp)
}

// maxBuckets is the default and largest page ListBuckets returns, as on AWS.
const maxBuckets = 10000

func (h *handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, t target) {
	type configuration struct {
		XMLName            xml.Name
		LocationConstraint string `xml:"LocationConstraint"`
	}
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	// Reading to EOF also completes the SigV4 payload check.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxConfigBody))
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if len(body) > 0 {
		var c configuration
		if err := decodeXMLDocument(body, &c); err != nil || c.XMLName.Local != "CreateBucketConfiguration" {
			writeError(w, r, errMalformedXML)
			return
		}
		// pail serves one region, so a bucket can be created only there.
		if c.LocationConstraint != "" && c.LocationConstraint != h.opts.Region {
			writeError(w, r, errIllegalLocationConstraint)
			return
		}
	}
	if err := h.opts.Store.CreateBucket(r.Context(), t.bucket); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("Location", "/"+t.bucket)
	w.WriteHeader(http.StatusOK)
}

func (h *handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if _, err := h.opts.Store.HeadBucket(r.Context(), t.bucket); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.Header().Set("x-amz-bucket-region", h.opts.Region)
	w.WriteHeader(http.StatusOK)
}

func (h *handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if err := h.opts.Store.DeleteBucket(r.Context(), t.bucket); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, t target) {
	type response struct {
		XMLName  xml.Name `xml:"LocationConstraint"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:",chardata"`
	}
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if _, err := h.opts.Store.HeadBucket(r.Context(), t.bucket); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	// AWS answers an empty constraint for us-east-1, its original region.
	location := h.opts.Region
	if location == "us-east-1" {
		location = ""
	}
	writeXML(w, r, http.StatusOK, response{Xmlns: s3Namespace, Location: location})
}

// validBucketName applies the AWS general-purpose bucket naming rules.
func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !isAlnum(c) && c != '.' && c != '-' {
			return false
		}
	}
	if !isAlnum(name[0]) || !isAlnum(name[len(name)-1]) || strings.Contains(name, "..") {
		return false
	}
	if net.ParseIP(name) != nil || isDottedQuad(name) {
		return false
	}
	for _, p := range []string{"xn--", "sthree-", "amzn-s3-demo-"} {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	for _, s := range []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"} {
		if strings.HasSuffix(name, s) {
			return false
		}
	}
	return true
}

func isAlnum(c byte) bool { return 'a' <= c && c <= 'z' || '0' <= c && c <= '9' }

// isDottedQuad reports four all-digit labels, such as 999.0.01.1, which AWS
// treats as IP-formatted even when net.ParseIP rejects them.
func isDottedQuad(name string) bool {
	labels := strings.Split(name, ".")
	if len(labels) != 4 {
		return false
	}
	for _, l := range labels {
		if l == "" || strings.Trim(l, "0123456789") != "" {
			return false
		}
	}
	return true
}

// decodeXMLDocument decodes body into v and rejects anything around the root
// element except whitespace, comments, and the XML declaration before it.
func decodeXMLDocument(body []byte, v any) error {
	dec := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if start, ok := tok.(xml.StartElement); ok {
			if err := dec.DecodeElement(v, &start); err != nil {
				return err
			}
			break
		}
		if cd, ok := tok.(xml.CharData); ok && len(bytes.TrimSpace(cd)) > 0 {
			return errors.New("content before the root element")
		}
	}
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if cd, ok := tok.(xml.CharData); !ok || len(bytes.TrimSpace(cd)) > 0 {
			return errors.New("content after the root element")
		}
	}
}

// writeXML sends v as an XML body. A HEAD response gets the headers only.
func writeXML(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	body = append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
