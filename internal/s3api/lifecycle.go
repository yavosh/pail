package s3api

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"time"

	"github.com/yavosh/pail/internal/lifecycle"
	"github.com/yavosh/pail/internal/store"
)

func (h *handler) setExpiration(w http.ResponseWriter, r *http.Request, bucket string, info store.ObjectInfo) {
	cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), bucket, "lifecycle")
	if err != nil {
		return
	}
	var c lifecycle.Configuration
	if err := xml.Unmarshal(cfg.XML, &c); err != nil {
		return
	}
	var earliest time.Time
	id := ""
	for _, rule := range c.Rules {
		if !rule.Matches(info.Key, info.Size) {
			continue
		}
		deadline := rule.Expires(info.LastModified)
		if !deadline.IsZero() && (earliest.IsZero() || deadline.Before(earliest)) {
			earliest = deadline
			id = rule.ID
		}
	}
	if !earliest.IsZero() {
		w.Header().Set("x-amz-expiration", `expiry-date="`+earliest.UTC().Format(http.TimeFormat)+`", rule-id="`+url.PathEscape(id)+`"`)
	}
}
