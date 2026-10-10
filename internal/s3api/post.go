package s3api

import (
	"cmp"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/tag"
)

func (h *handler) handlePostObject(w http.ResponseWriter, r *http.Request, t target) {
	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, r, errInvalidArgument)
		return
	}
	fields := map[string]string{}
	fieldBytes := 0
	for {
		part, err := mr.NextPart()
		if err != nil {
			writeError(w, r, errInvalidArgument)
			return
		}
		name := strings.ToLower(part.FormName())
		if name == "file" {
			filename := path.Base(strings.ReplaceAll(part.FileName(), "\\", "/"))
			for key, value := range fields {
				fields[key] = strings.ReplaceAll(value, "${filename}", filename)
			}
			if value, exists := fields["bucket"]; exists && value != t.bucket {
				writeError(w, r, errAccessDenied)
				return
			}
			fields["bucket"] = t.bucket
			t.key = fields["key"]
			if e, ok := checkObjectTarget(t); !ok {
				writeError(w, r, e)
				return
			}
			minimum, maximum := int64(0), int64(maxObjectSize)
			ownerID := h.bucketOwner().ID
			if fields["policy"] == "" && fields["x-amz-signature"] == "" && fields["x-amz-credential"] == "" && fields["x-amz-algorithm"] == "" && fields["x-amz-date"] == "" {
				if !h.anonymousAllowed(r, t, opPutObject) {
					writeError(w, r, errAccessDenied)
					return
				}
				ownerID = acl.AnonymousID
			} else {
				minimum, maximum, err = h.verifier.VerifyPost(fields)
				if err != nil {
					writeError(w, r, toAPIError(err))
					return
				}
			}
			headers := http.Header{}
			for field, value := range fields {
				switch {
				case strings.HasPrefix(field, "x-amz-meta-"):
					headers.Set(field, value)
				case field == "tagging": // an XML document, parsed after the checks above
				case field == "x-amz-storage-class", field == "x-amz-website-redirect-location",
					strings.HasPrefix(field, "x-amz-server-side-encryption"), strings.HasPrefix(field, "x-amz-object-lock-"):
					headers.Set(field, value)
				case field == "x-amz-checksum-algorithm":
					headers.Set("x-amz-sdk-checksum-algorithm", value)
				case strings.HasPrefix(field, "x-amz-checksum-"):
					headers.Set(field, value)
				case field == "acl":
					headers.Set("x-amz-acl", value)
				case strings.HasPrefix(field, "x-amz-") && field != "x-amz-algorithm" && field != "x-amz-credential" && field != "x-amz-date" && field != "x-amz-signature":
					writeError(w, r, errNotImplemented)
					return
				}
			}
			for _, name := range storedHeaders {
				if value := fields[strings.ToLower(name)]; value != "" {
					headers.Set(name, value)
				}
			}
			metadata, e, ok := requestMetadata(headers, false)
			if !ok {
				writeError(w, r, e)
				return
			}
			redirect := fields["success_action_redirect"]
			if redirect == "" {
				redirect = fields["redirect"]
			}
			var destination *url.URL
			if redirect != "" {
				destination, err = url.Parse(redirect)
				if err != nil || destination.Host == "" || destination.Scheme != "http" && destination.Scheme != "https" {
					writeError(w, r, errInvalidArgument)
					return
				}
			}
			if e, ok := h.checkACLsEnabled(r.Context(), t.bucket, headers); !ok {
				writeError(w, r, e)
				return
			}
			policy, e, ok := h.requestACL(headers, false, ownerID)
			if !ok {
				writeError(w, r, e)
				return
			}
			opts := store.PutOptions{Metadata: metadata, ACL: &policy, Anonymous: ownerID == acl.AnonymousID}
			if value, exists := fields["content-md5"]; exists {
				digest, err := base64.StdEncoding.DecodeString(value)
				if err != nil || len(digest) != 16 {
					writeError(w, r, errInvalidDigest)
					return
				}
				opts.ContentMD5 = digest
			}
			options, e, ok := parseObjectOptions(headers)
			if !ok {
				writeError(w, r, e)
				return
			}
			opts.ObjectOptions = options
			if doc := fields["tagging"]; doc != "" {
				if opts.Tags, e, ok = parseTagging([]byte(doc), tag.MaxObject); !ok {
					writeError(w, r, e)
					return
				}
			}
			algorithm, digest, trailer, e, valid := parseChecksum(headers)
			if !valid || trailer || fields["x-amz-checksum-algorithm"] != "" && digest == nil {
				writeError(w, r, cmp.Or(e, errInvalidChecksum))
				return
			}
			opts.ChecksumAlgorithm, opts.Checksum = algorithm, digest
			body := &postBody{part: part, multipart: mr, minimum: minimum, maximum: maximum}
			info, err := h.opts.Store.PutObject(r.Context(), t.bucket, t.key, body, opts)
			if err != nil {
				writeError(w, r, toAPIError(err))
				return
			}
			h.notify(w, r, t, createdEvent("ObjectCreated:Post", t.key, info))
			etag := quoteETag(info.ETag)
			w.Header().Set("ETag", etag)
			setChecksumHeaders(w.Header(), info)
			setOptionHeaders(w.Header(), info.ObjectOptions)
			if destination != nil {
				q := destination.Query()
				q.Set("bucket", t.bucket)
				q.Set("key", t.key)
				q.Set("etag", etag)
				destination.RawQuery = q.Encode()
				w.Header().Set("Location", destination.String())
				w.WriteHeader(http.StatusSeeOther)
				return
			}
			switch fields["success_action_status"] {
			case "200":
				w.WriteHeader(http.StatusOK)
			case "201":
				type response struct {
					XMLName  xml.Name `xml:"PostResponse"`
					Location string   `xml:"Location"`
					Bucket   string   `xml:"Bucket"`
					Key      string   `xml:"Key"`
					ETag     string   `xml:"ETag"`
				}
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				location := &url.URL{Scheme: scheme, Host: r.Host, Path: "/" + t.key}
				if !t.virtualHost {
					location.Path = "/" + t.bucket + "/" + t.key
				}
				w.Header().Set("Location", location.String())
				writeXML(w, r, http.StatusCreated, response{Location: location.String(), Bucket: t.bucket, Key: t.key, ETag: etag})
			default:
				w.WriteHeader(http.StatusNoContent)
			}
			return
		}
		if name == "" {
			writeError(w, r, errInvalidArgument)
			return
		}
		value, err := io.ReadAll(io.LimitReader(part, int64((20<<10)-fieldBytes+1)))
		fieldBytes += len(value) + len(name)
		if err != nil || fieldBytes > 20<<10 {
			writeError(w, r, errMaxMessageLength)
			return
		}
		if _, exists := fields[name]; exists {
			writeError(w, r, errInvalidArgument)
			return
		}
		fields[name] = string(value)
	}
}

// postBody checks size and the final boundary before the store commits.
type postBody struct {
	part                   *multipart.Part
	multipart              *multipart.Reader
	minimum, maximum, size int64
}

func (b *postBody) Read(p []byte) (int, error) {
	p = p[:min(int64(len(p)), b.maximum-b.size+1)]
	n, err := b.part.Read(p)
	b.size += int64(n)
	if b.size > b.maximum {
		return n, store.ErrEntityTooLarge
	}
	if errors.Is(err, io.EOF) {
		if b.size < b.minimum {
			return n, store.ErrEntityTooSmall
		}
		if _, nextErr := b.multipart.NextPart(); !errors.Is(nextErr, io.EOF) {
			return n, sigv4.ErrPolicyCondition
		}
	}
	return n, err
}
