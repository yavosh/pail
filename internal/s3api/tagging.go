package s3api

import (
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/tag"
)

var (
	errInvalidTag          = apiError{"InvalidTag", http.StatusBadRequest, "The tag provided was not a valid tag. This error can occur if the tag did not pass input validation."}
	errTooManyTags         = apiError{"BadRequest", http.StatusBadRequest, "The number of tags exceeds the limit."}
	errNoSuchTagSet        = apiError{"NoSuchTagSet", http.StatusNotFound, "The TagSet does not exist"}
	errUnknownTagDirective = apiError{"InvalidArgument", http.StatusBadRequest, "Unknown tagging directive."}
)

// tagging is the Tagging document of the tagging operations and the POST form.
type tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  struct {
		Tags []tag.Tag `xml:"Tag"`
	} `xml:"TagSet"`
}

// parseTagging decodes and validates a Tagging document.
func parseTagging(doc []byte, limit int) ([]tag.Tag, apiError, bool) {
	var d tagging
	if err := decodeXMLDocument(doc, &d); err != nil {
		return nil, errMalformedXML, false
	}
	if err := tag.Validate(d.TagSet.Tags, limit); err != nil {
		return nil, toAPIError(err), false
	}
	return d.TagSet.Tags, apiError{}, true
}

// marshalTagging renders tags as a Tagging document.
func marshalTagging(tags []tag.Tag) []byte {
	d := tagging{Xmlns: s3Namespace}
	d.TagSet.Tags = tags
	out, _ := xml.Marshal(d)
	return out
}

// writeTagging answers a tagging read. Like AWS, it sends no Content-Type.
func writeTagging(w http.ResponseWriter, doc []byte) {
	w.Header()["Content-Type"] = nil
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append([]byte(xml.Header), doc...))
}

// readTagging reads a PutObjectTagging or PutBucketTagging body.
func readTagging(w http.ResponseWriter, r *http.Request, limit int) ([]tag.Tag, apiError, bool) {
	body, e, ok := readConfiguration(w, r, false)
	if !ok {
		return nil, e, false
	}
	return parseTagging(body, limit)
}

func (h *handler) handleObjectTagging(w http.ResponseWriter, r *http.Request, t target) {
	if apiErr, ok := checkObjectTarget(t); !ok {
		writeError(w, r, apiErr)
		return
	}
	versionID := r.URL.Query().Get("versionId")
	switch r.Method {
	case http.MethodGet:
		info, err := h.opts.Store.HeadObjectVersion(r.Context(), t.bucket, t.key, versionID)
		if err != nil {
			writeReadError(w, r, err)
			return
		}
		h.setVersionHeader(w, r, t.bucket, info)
		writeTagging(w, marshalTagging(info.Tags))
	case http.MethodPut:
		tags, e, ok := readTagging(w, r, tag.MaxObject)
		if !ok {
			writeError(w, r, e)
			return
		}
		if err := h.opts.Store.PutObjectTags(r.Context(), t.bucket, t.key, versionID, tags); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		setRequestedVersion(w, versionID)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := h.opts.Store.PutObjectTags(r.Context(), t.bucket, t.key, versionID, nil); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		setRequestedVersion(w, versionID)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *handler) handleBucketTagging(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	switch r.Method {
	case http.MethodGet:
		cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), t.bucket, "tagging")
		if errors.Is(err, store.ErrNoSuchConfiguration) {
			writeError(w, r, errNoSuchTagSet)
			return
		}
		if err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		writeTagging(w, cfg.XML)
	case http.MethodPut:
		tags, e, ok := readTagging(w, r, tag.MaxBucket)
		if !ok {
			writeError(w, r, e)
			return
		}
		// An empty tag set reads back as NoSuchTagSet (recorded in object-tagging).
		var cfg *store.BucketConfiguration
		if len(tags) > 0 {
			cfg = &store.BucketConfiguration{XML: marshalTagging(tags)}
		}
		if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "tagging", cfg); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "tagging", nil); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
