package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"slices"

	"github.com/yavosh/pail/internal/store"
)

const ownerEnforced = "BucketOwnerEnforced"

var (
	errACLNotSupported = apiError{"AccessControlListNotSupported", http.StatusBadRequest, "The bucket does not allow ACLs"}
	errNoOwnership     = apiError{"OwnershipControlsNotFoundError", http.StatusNotFound, "The bucket ownership controls were not found"}
)

// ownershipControls follows the S3 OwnershipControls XML schema.
type ownershipControls struct {
	XMLName xml.Name        `xml:"OwnershipControls"`
	Xmlns   string          `xml:"xmlns,attr,omitempty"`
	Rules   []ownershipRule `xml:"Rule"`
}

type ownershipRule struct {
	ObjectOwnership string     `xml:"ObjectOwnership"`
	Unknown         []xml.Name `xml:",any"`
}

func validOwnership(value string) bool {
	return slices.Contains([]string{ownerEnforced, "BucketOwnerPreferred", "ObjectWriter"}, value)
}

// ownerEnforced reports whether the bucket disables ACLs. A bucket without
// ownership controls keeps ACLs enabled.
func (h *handler) ownerEnforced(ctx context.Context, bucket string) (bool, error) {
	cfg, err := h.opts.Store.GetBucketConfiguration(ctx, bucket, "ownership")
	if errors.Is(err, store.ErrNoSuchConfiguration) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var c ownershipControls
	if err := xml.Unmarshal(cfg.XML, &c); err != nil || len(c.Rules) != 1 {
		return false, errors.New("corrupt ownership controls")
	}
	return c.Rules[0].ObjectOwnership == ownerEnforced, nil
}

func (h *handler) putOwnership(ctx context.Context, bucket, value string) error {
	body, _ := xml.Marshal(ownershipControls{Xmlns: s3Namespace, Rules: []ownershipRule{{ObjectOwnership: value}}})
	return h.opts.Store.PutBucketConfiguration(ctx, bucket, "ownership", &store.BucketConfiguration{XML: body})
}

// checkACLsEnabled rejects a write that sets an ACL on a bucket that disables them.
func (h *handler) checkACLsEnabled(ctx context.Context, bucket string, headers http.Header) (apiError, bool) {
	if _, ok := aclHeadersAllowed(headers); ok {
		return apiError{}, true
	}
	enforced, err := h.ownerEnforced(ctx, bucket)
	if err != nil {
		return toAPIError(err), false
	}
	if enforced {
		return errACLNotSupported, false
	}
	return apiError{}, true
}

// aclHeadersAllowed reports whether headers are valid under BucketOwnerEnforced:
// no grants, and only the canned ACLs that name the owner.
func aclHeadersAllowed(headers http.Header) (apiError, bool) {
	canned := headers.Get("x-amz-acl")
	if (canned == "" || canned == "private" || canned == "bucket-owner-full-control") && !slices.ContainsFunc(grantHeaders, func(g struct{ header, permission string }) bool { return headers.Get(g.header) != "" }) {
		return apiError{}, true
	}
	return errACLNotSupported, false
}

func (h *handler) handleOwnershipControls(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if expected := r.Header.Get("x-amz-expected-bucket-owner"); expected != "" && expected != h.bucketOwner().ID {
		writeError(w, r, errAccessDenied)
		return
	}
	switch r.Method {
	case http.MethodGet:
		cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), t.bucket, "ownership")
		if errors.Is(err, store.ErrNoSuchConfiguration) {
			writeError(w, r, errNoOwnership)
			return
		}
		if err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		// S3 omits Content-Type here; nil also prevents HTTP content sniffing.
		w.Header()["Content-Type"] = nil
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append([]byte(xml.Header), cfg.XML...))
	case http.MethodDelete:
		if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "ownership", nil); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPut:
		body, e, ok := readConfiguration(w, r, false)
		if !ok {
			writeError(w, r, e)
			return
		}
		var c ownershipControls
		if err := decodeXMLDocument(body, &c); err != nil || len(c.Rules) != 1 || len(c.Rules[0].Unknown) != 0 || !validOwnership(c.Rules[0].ObjectOwnership) {
			writeError(w, r, errMalformedXML)
			return
		}
		if err := h.putOwnership(r.Context(), t.bucket, c.Rules[0].ObjectOwnership); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
