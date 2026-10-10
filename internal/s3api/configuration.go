package s3api

import (
	"bytes"
	"cmp"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/lifecycle"
	"github.com/yavosh/pail/internal/store"
)

func (h *handler) handleBucketConfiguration(w http.ResponseWriter, r *http.Request, t target) {
	kind := "cors"
	if r.URL.Query().Has("lifecycle") {
		kind = "lifecycle"
	}
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
		cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), t.bucket, kind)
		if errors.Is(err, store.ErrNoSuchConfiguration) {
			e := apiError{"NoSuchCORSConfiguration", 404, "The CORS configuration does not exist"}
			if kind == "lifecycle" {
				e = apiError{"NoSuchLifecycleConfiguration", 404, "The lifecycle configuration does not exist"}
			}
			writeError(w, r, e)
			return
		}
		if err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		if kind == "lifecycle" {
			w.Header().Set("x-amz-transition-default-minimum-object-size", cfg.TransitionMinimum)
		}
		// S3 omits Content-Type here; nil also prevents HTTP content sniffing.
		w.Header()["Content-Type"] = nil
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append([]byte(xml.Header), cfg.XML...))
	case http.MethodDelete:
		if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, kind, nil); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPut:
		body, e, ok := readConfiguration(w, r, kind == "cors")
		if !ok {
			writeError(w, r, e)
			return
		}
		cfg := store.BucketConfiguration{}
		if kind == "cors" {
			var c corsConfiguration
			if err := decodeXMLDocument(body, &c); err != nil {
				writeError(w, r, errMalformedXML)
				return
			}
			if e := validateCORS(c); e != (apiError{}) {
				writeError(w, r, e)
				return
			}
			c.Xmlns = s3Namespace
			cfg.XML, _ = xml.Marshal(c)
		} else {
			var c lifecycle.Configuration
			if err := decodeXMLDocument(body, &c); err != nil {
				writeError(w, r, errMalformedXML)
				return
			}
			if e := validateLifecycle(c); e != (apiError{}) {
				writeError(w, r, e)
				return
			}
			for i := range c.Rules {
				if c.Rules[i].ID == "" {
					c.Rules[i].ID = base64.RawURLEncoding.EncodeToString(randomBytes(16))
				}
			}
			c.Xmlns = s3Namespace
			cfg.XML, _ = xml.Marshal(c)
			cfg.TransitionMinimum = r.Header.Get("x-amz-transition-default-minimum-object-size")
			if cfg.TransitionMinimum == "" {
				cfg.TransitionMinimum = "all_storage_classes_128K"
			}
			if cfg.TransitionMinimum != "all_storage_classes_128K" && cfg.TransitionMinimum != "varies_by_storage_class" {
				writeError(w, r, errInvalidArgument)
				return
			}
		}
		if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, kind, &cfg); err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		if kind == "lifecycle" {
			w.Header().Set("x-amz-transition-default-minimum-object-size", cfg.TransitionMinimum)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func readConfiguration(w http.ResponseWriter, r *http.Request, requireChecksum bool) ([]byte, apiError, bool) {
	algorithm, want, trailer, apiErr, ok := parseChecksum(r.Header)
	if !ok || trailer {
		return nil, cmp.Or(apiErr, errInvalidChecksum), false
	}
	md5Value, hasMD5 := r.Header["Content-Md5"]
	var digest []byte
	if hasMD5 {
		var err error
		digest, err = base64.StdEncoding.DecodeString(md5Value[0])
		if err != nil || len(digest) != 16 {
			return nil, errInvalidDigest, false
		}
	}
	if requireChecksum && !hasMD5 && want == nil {
		return nil, errMissingContentMD5, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxConfigBody))
	if err != nil {
		return nil, toAPIError(err), false
	}
	if hasMD5 {
		sum := md5.Sum(body)
		if !bytes.Equal(sum[:], digest) {
			return nil, errBadDigest, false
		}
	}
	if want != nil {
		sum, _ := checksum.New(algorithm)
		_, _ = sum.Write(body)
		if !bytes.Equal(sum.Sum(nil), want) {
			return nil, errChecksumMismatch, false
		}
	}
	return body, apiError{}, true
}

func validateLifecycle(c lifecycle.Configuration) apiError {
	if len(c.Unknown) != 0 || len(c.Rules) == 0 || len(c.Rules) > 1000 {
		return errMalformedXML
	}
	ids := map[string]bool{}
	for _, rule := range c.Rules {
		if len(rule.Unknown) != 0 {
			return errNotImplemented
		}
		if len(rule.ID) > 255 || rule.ID != "" && ids[rule.ID] {
			return errInvalidArgument
		}
		ids[rule.ID] = true
		if rule.Status != "Enabled" && rule.Status != "Disabled" || (rule.Prefix == nil) == (rule.Filter == nil) || rule.Expiration == nil && rule.Abort == nil {
			return errMalformedXML
		}
		if rule.Filter != nil {
			f := rule.Filter
			count := filterCount(f)
			if len(f.Unknown) != 0 {
				return errNotImplemented
			}
			if f.And != nil {
				if count != 1 {
					return errMalformedXML
				}
				f = f.And
				if len(f.Unknown) != 0 {
					return errNotImplemented
				}
				if f.And != nil || filterCount(f) < 2 {
					return errMalformedXML
				}
			} else if count > 1 {
				return errMalformedXML
			}
			if f.Greater != nil && *f.Greater < 0 || f.Less != nil && *f.Less < 0 || f.Greater != nil && f.Less != nil && *f.Greater >= *f.Less {
				return errInvalidArgument
			}
			if rule.Abort != nil && (f.Greater != nil || f.Less != nil) {
				return errInvalidArgument
			}
		}
		if exp := rule.Expiration; exp != nil {
			if len(exp.Unknown) != 0 {
				return errNotImplemented
			}
			if (exp.Days == nil) == (exp.Date == "") {
				return errMalformedXML
			}
			if exp.Days != nil && (*exp.Days < 1 || *exp.Days > 2147483647) {
				return errInvalidLifecycleDays
			}
			if exp.Date != "" {
				date, err := time.Parse(time.RFC3339, exp.Date)
				if err != nil || !strings.HasSuffix(exp.Date, "Z") || !date.Equal(date.UTC().Truncate(24*time.Hour)) {
					return errInvalidArgument
				}
			}
		}
		if abort := rule.Abort; abort != nil {
			if len(abort.Unknown) != 0 {
				return errMalformedXML
			}
			if abort.Days < 1 || abort.Days > 2147483647 {
				return errInvalidArgument
			}
		}
	}
	return apiError{}
}

func filterCount(f *lifecycle.Filter) int {
	n := 0
	for _, present := range []bool{f.Prefix != nil, f.Greater != nil, f.Less != nil, f.And != nil} {
		if present {
			n++
		}
	}
	return n
}

// corsConfiguration follows the S3 CORS XML schema.
type corsConfiguration struct {
	XMLName xml.Name   `xml:"CORSConfiguration"`
	Xmlns   string     `xml:"xmlns,attr,omitempty"`
	Rules   []corsRule `xml:"CORSRule"`
	Unknown []xml.Name `xml:",any"`
}

type corsRule struct {
	ID      string     `xml:"ID,omitempty"`
	Origins []string   `xml:"AllowedOrigin"`
	Methods []string   `xml:"AllowedMethod"`
	MaxAge  *int       `xml:"MaxAgeSeconds,omitempty"`
	Expose  []string   `xml:"ExposeHeader"`
	Headers []string   `xml:"AllowedHeader"`
	Unknown []xml.Name `xml:",any"`
}

func validateCORS(c corsConfiguration) apiError {
	if len(c.Unknown) != 0 || len(c.Rules) == 0 || len(c.Rules) > 100 {
		return errMalformedXML
	}
	for _, rule := range c.Rules {
		if len(rule.Unknown) != 0 || len(rule.Origins) == 0 || len(rule.Methods) == 0 || len(rule.ID) > 255 || rule.MaxAge != nil && *rule.MaxAge < 0 {
			return errInvalidArgument
		}
		for _, method := range rule.Methods {
			if !slices.Contains([]string{"GET", "PUT", "HEAD", "POST", "DELETE"}, method) {
				return errInvalidArgument
			}
		}
		for _, pattern := range append(slices.Clone(rule.Origins), rule.Headers...) {
			if pattern == "" || strings.Count(pattern, "*") > 1 {
				return errInvalidArgument
			}
		}
		for _, name := range rule.Expose {
			if name == "" || strings.Contains(name, "*") {
				return errInvalidArgument
			}
		}
	}
	return apiError{}
}
