package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"slices"

	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/sigv4"
)

const (
	maxDeleteKeys = 1000
	// maxDeleteBody holds 1000 keys of 1024 bytes, with XML escaping.
	maxDeleteBody = 8 << 20
)

func (h *handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, t target) {
	type object struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	}
	type request struct {
		XMLName xml.Name `xml:"Delete"`
		Quiet   bool     `xml:"Quiet"`
		Objects []object `xml:"Object"`
	}
	type deleted struct {
		Key string `xml:"Key"`
	}
	type failure struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	type response struct {
		XMLName xml.Name  `xml:"DeleteResult"`
		Xmlns   string    `xml:"xmlns,attr"`
		Deleted []deleted `xml:"Deleted"`
		Errors  []failure `xml:"Error"`
	}
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if _, err := h.opts.Store.HeadBucket(r.Context(), t.bucket); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	var md5Sum []byte
	// A present but empty Content-MD5 is invalid, not absent.
	if values, ok := r.Header["Content-Md5"]; ok {
		var err error
		if md5Sum, err = base64.StdEncoding.DecodeString(values[0]); err != nil || len(md5Sum) != 16 {
			writeError(w, r, errInvalidDigest)
			return
		}
	}
	algorithm, want, inTrailer, ok := parseChecksum(r.Header)
	streaming := sigv4.IsStreaming(r.Header)
	if !ok || inTrailer && !streaming {
		writeError(w, r, errInvalidChecksum)
		return
	}
	if md5Sum == nil && algorithm == "" {
		writeError(w, r, errMissingContentMD5)
		return
	}

	var src io.Reader = r.Body
	if inTrailer {
		sum, _ := checksum.New(algorithm)
		src = &trailerChecksum{r: r, h: sum, algorithm: algorithm}
	}
	// Reading to EOF also completes the SigV4 payload check and the trailer check.
	body, err := io.ReadAll(http.MaxBytesReader(w, io.NopCloser(src), maxDeleteBody))
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if md5Sum != nil {
		if sum := md5.Sum(body); !bytes.Equal(sum[:], md5Sum) {
			writeError(w, r, errBadDigest)
			return
		}
	}
	if want != nil {
		sum, _ := checksum.New(algorithm)
		sum.Write(body)
		if !bytes.Equal(sum.Sum(nil), want) {
			writeError(w, r, errChecksumMismatch)
			return
		}
	}

	var req request
	// A key is required; check them all before deleting any.
	if err := decodeXMLDocument(body, &req); err != nil || len(req.Objects) == 0 || len(req.Objects) > maxDeleteKeys ||
		slices.ContainsFunc(req.Objects, func(o object) bool { return o.Key == "" }) {
		writeError(w, r, errMalformedXML)
		return
	}
	resp := response{Xmlns: s3Namespace}
	for _, o := range req.Objects {
		apiErr := apiError{}
		switch {
		case o.VersionID != "" && o.VersionID != "null":
			apiErr = errNoSuchVersion
		default:
			if e, ok := checkObjectTarget(target{bucket: t.bucket, key: o.Key}); !ok {
				apiErr = e
			} else if err := h.opts.Store.DeleteObject(r.Context(), t.bucket, o.Key); err != nil {
				apiErr = toAPIError(err)
			}
		}
		switch {
		case apiErr != (apiError{}):
			resp.Errors = append(resp.Errors, failure{Key: o.Key, Code: apiErr.Code, Message: apiErr.Message})
		case !req.Quiet:
			resp.Deleted = append(resp.Deleted, deleted{Key: o.Key})
		}
	}
	if req.Quiet {
		w.Header()["Content-Type"] = nil // AWS sends a quiet result untyped; nil stops sniffing
		w.WriteHeader(http.StatusOK)
		body, _ := xml.Marshal(resp) // marshals plain strings, so it cannot fail
		_, _ = w.Write(append([]byte(xml.Header), body...))
		return
	}
	writeXML(w, r, http.StatusOK, resp)
}
