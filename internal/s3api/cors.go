package s3api

import (
	"encoding/xml"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/yavosh/pail/internal/store"
)

func wildcardMatch(pattern, value string) bool {
	before, after, star := strings.Cut(pattern, "*")
	if !star {
		return pattern == value
	}
	return len(value) >= len(before)+len(after) && strings.HasPrefix(value, before) && strings.HasSuffix(value, after)
}

func (h *handler) applyCORS(w http.ResponseWriter, r *http.Request, t target, preflight bool) {
	if preflight && !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		if preflight {
			writeError(w, r, errInvalidArgument)
		}
		return
	}
	method := r.Method
	var requested []string
	if preflight {
		method = r.Header.Get("Access-Control-Request-Method")
		if method == "" {
			writeError(w, r, errInvalidArgument)
			return
		}
		if raw := r.Header.Get("Access-Control-Request-Headers"); raw != "" {
			for name := range strings.SplitSeq(raw, ",") {
				requested = append(requested, strings.TrimSpace(name))
			}
		}
	}
	cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), t.bucket, "cors")
	if err != nil {
		if preflight {
			if errors.Is(err, store.ErrNoSuchConfiguration) {
				writeError(w, r, errCORSForbidden)
			} else {
				writeError(w, r, toAPIError(err))
			}
		}
		return
	}
	var c corsConfiguration
	if err := xml.Unmarshal(cfg.XML, &c); err != nil {
		if preflight {
			writeError(w, r, errInternal)
		}
		return
	}
	for _, rule := range c.Rules {
		if !slices.Contains(rule.Methods, method) {
			continue
		}
		matched := ""
		for _, pattern := range rule.Origins {
			if wildcardMatch(pattern, origin) {
				matched = pattern
				break
			}
		}
		if matched == "" {
			continue
		}
		if !slices.ContainsFunc(requested, func(name string) bool {
			return !slices.ContainsFunc(rule.Headers, func(pattern string) bool { return wildcardMatch(strings.ToLower(pattern), strings.ToLower(name)) })
		}) {
			allowed := origin
			if matched == "*" {
				allowed = "*"
			} else {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
			w.Header().Set("Access-Control-Allow-Origin", allowed)
			w.Header().Set("Access-Control-Allow-Methods", strings.Join(rule.Methods, ", "))
			w.Header().Add("Vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method")
			if len(rule.Expose) != 0 {
				w.Header().Set("Access-Control-Expose-Headers", strings.Join(rule.Expose, ", "))
			}
			if rule.MaxAge != nil {
				w.Header().Set("Access-Control-Max-Age", strconv.Itoa(*rule.MaxAge))
			}
			if len(requested) != 0 {
				w.Header().Set("Access-Control-Allow-Headers", strings.Join(requested, ", "))
			}
			if preflight {
				w.WriteHeader(http.StatusOK)
			}
			return
		}
	}
	if preflight {
		writeError(w, r, errCORSForbidden)
	}
}
