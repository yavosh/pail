// Package tag holds S3 tags: the type, its limits, and the rules that
// validate a tag set.
package tag

import (
	"errors"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	// MaxObject and MaxBucket are the most tags an object or a bucket holds.
	MaxObject = 10
	MaxBucket = 50

	maxKey   = 128
	maxValue = 256
)

var (
	// ErrTooMany means a tag set is over its limit.
	ErrTooMany = errors.New("too many tags")
	// ErrInvalid means a tag key or value, or the tag set, breaks a rule.
	ErrInvalid = errors.New("invalid tag")
)

// Tag is one key and value pair.
type Tag struct {
	Key   string `xml:"Key" json:"key"`
	Value string `xml:"Value" json:"value"`
}

// Validate checks a tag set against limit. Keys are 1 to 128 characters, unique,
// and do not start with "aws:". Values are up to 256 characters and may be empty.
func Validate(tags []Tag, limit int) error {
	if len(tags) > limit {
		return ErrTooMany
	}
	seen := make(map[string]bool, len(tags))
	for _, t := range tags {
		n := utf8.RuneCountInString(t.Key)
		if n < 1 || n > maxKey || utf8.RuneCountInString(t.Value) > maxValue ||
			strings.HasPrefix(t.Key, "aws:") || seen[t.Key] {
			return ErrInvalid
		}
		seen[t.Key] = true
	}
	return nil
}

// ParseHeader reads the URL query encoding of x-amz-tagging, such as
// "a=1&b=2". A key without "=" has an empty value. The order is kept.
func ParseHeader(s string, limit int) ([]Tag, error) {
	var tags []Tag
	for pair := range strings.SplitSeq(s, "&") {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		key, err := url.QueryUnescape(k)
		if err != nil {
			return nil, ErrInvalid
		}
		value, err := url.QueryUnescape(v)
		if err != nil {
			return nil, ErrInvalid
		}
		tags = append(tags, Tag{Key: key, Value: value})
	}
	if err := Validate(tags, limit); err != nil {
		return nil, err
	}
	return tags, nil
}

// Has reports whether every tag in want is in have, with the same value.
func Has(have, want []Tag) bool {
	for _, w := range want {
		if !slices.Contains(have, w) {
			return false
		}
	}
	return true
}
