// Package lifecycle describes the actions supported by pail's unversioned store.
package lifecycle

import (
	"encoding/xml"
	"strings"
	"time"
)

// Configuration is an S3 lifecycle document.
type Configuration struct {
	XMLName xml.Name   `xml:"LifecycleConfiguration"`
	Xmlns   string     `xml:"xmlns,attr,omitempty"`
	Rules   []Rule     `xml:"Rule"`
	Unknown []xml.Name `xml:",any"`
}

// Rule selects objects and schedules their expiration or upload cleanup.
type Rule struct {
	ID         string      `xml:"ID,omitempty"`
	Prefix     *string     `xml:"Prefix,omitempty"`
	Filter     *Filter     `xml:"Filter,omitempty"`
	Status     string      `xml:"Status"`
	Expiration *Expiration `xml:"Expiration,omitempty"`
	Abort      *Abort      `xml:"AbortIncompleteMultipartUpload,omitempty"`
	Unknown    []xml.Name  `xml:",any"`
}

// Filter selects keys and exclusive object size bounds.
type Filter struct {
	Prefix  *string    `xml:"Prefix,omitempty"`
	Greater *int64     `xml:"ObjectSizeGreaterThan,omitempty"`
	Less    *int64     `xml:"ObjectSizeLessThan,omitempty"`
	And     *Filter    `xml:"And,omitempty"`
	Unknown []xml.Name `xml:",any"`
}

// Expiration specifies either a positive age or a midnight UTC date.
type Expiration struct {
	Days    *int       `xml:"Days,omitempty"`
	Date    string     `xml:"Date,omitempty"`
	Unknown []xml.Name `xml:",any"`
}

// Abort specifies a positive age for incomplete multipart uploads.
type Abort struct {
	Days    int        `xml:"DaysAfterInitiation"`
	Unknown []xml.Name `xml:",any"`
}

// Matches reports whether an enabled rule selects key and size.
func (r *Rule) Matches(key string, size int64) bool {
	if r.Status != "Enabled" {
		return false
	}
	if r.Prefix != nil && !strings.HasPrefix(key, *r.Prefix) {
		return false
	}
	f := r.Filter
	if f == nil {
		return true
	}
	if f.And != nil {
		f = f.And
	}
	return (f.Prefix == nil || strings.HasPrefix(key, *f.Prefix)) &&
		(f.Greater == nil || size > *f.Greater) && (f.Less == nil || size < *f.Less)
}

// Deadline rounds an age to the following midnight UTC, as S3 does.
func Deadline(created time.Time, days int) time.Time {
	return created.UTC().Truncate(24*time.Hour).AddDate(0, 0, days+1)
}

// Expires returns the expiration date, or zero when no expiration is set.
func (r *Rule) Expires(modified time.Time) time.Time {
	if r.Expiration == nil {
		return time.Time{}
	}
	if r.Expiration.Days != nil {
		return Deadline(modified, *r.Expiration.Days)
	}
	date, _ := time.Parse(time.RFC3339, r.Expiration.Date)
	return date
}
