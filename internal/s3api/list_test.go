package s3api

import (
	"slices"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/store"
)

func objects(keys ...string) []store.ObjectInfo {
	slices.Sort(keys)
	out := make([]store.ObjectInfo, len(keys))
	for i, k := range keys {
		out[i] = store.ObjectInfo{Key: k}
	}
	return out
}

func filtered(all []store.ObjectInfo, prefix string) []store.ObjectInfo {
	var out []store.ObjectInfo
	for _, o := range all {
		if strings.HasPrefix(o.Key, prefix) {
			out = append(out, o)
		}
	}
	return out
}

func TestListEntries(t *testing.T) {
	all := objects("a", "a/b", "a/c/d", "a/c/e", "b", "dir/", "dir/x", "dir/y/z", "e-1-x", "e-1-y", "e-2")
	tests := []struct {
		name, prefix, delimiter, after string
		limit                          int
		wantKeys, wantPrefixes         []string
		wantTruncated                  bool
		wantLast                       string
	}{
		{"flat", "", "", "", 1000, []string{"a", "a/b", "a/c/d", "a/c/e", "b", "dir/", "dir/x", "dir/y/z", "e-1-x", "e-1-y", "e-2"}, nil, false, "e-2"},
		{"root folders", "", "/", "", 1000, []string{"a", "b", "e-1-x", "e-1-y", "e-2"}, []string{"a/", "dir/"}, false, "e-2"},
		{"inside a folder", "a/", "/", "", 1000, []string{"a/b"}, []string{"a/c/"}, false, "a/c/"},
		{"folder marker key", "dir/", "/", "", 1000, []string{"dir/", "dir/x"}, []string{"dir/y/"}, false, "dir/y/"},
		{"other delimiter", "", "-", "", 1000, []string{"a", "a/b", "a/c/d", "a/c/e", "b", "dir/", "dir/x", "dir/y/z"}, []string{"e-"}, false, "e-"},
		{"page counts prefixes", "", "/", "", 2, []string{"a"}, []string{"a/"}, true, "a/"},
		{"resume after a prefix skips its keys", "", "/", "a/", 2, []string{"b"}, []string{"dir/"}, true, "dir/"},
		{"resume after a key inside a prefix listing", "dir/", "/", "dir/", 1000, []string{"dir/x"}, []string{"dir/y/"}, false, "dir/y/"},
		{"start after a key", "", "", "dir/x", 1000, []string{"dir/y/z", "e-1-x", "e-1-y", "e-2"}, nil, false, "e-2"},
		{"exact page is not truncated", "a/c/", "", "", 2, []string{"a/c/d", "a/c/e"}, nil, false, "a/c/e"},
		{"zero limit", "", "", "", 0, nil, nil, true, ""},
	}
	for _, tt := range tests {
		l := listEntries(filtered(all, tt.prefix), tt.prefix, tt.delimiter, tt.after, tt.limit)
		var keys []string
		for _, o := range l.contents {
			keys = append(keys, o.Key)
		}
		if !slices.Equal(keys, tt.wantKeys) || !slices.Equal(l.prefixes, tt.wantPrefixes) || l.truncated != tt.wantTruncated || l.last != tt.wantLast {
			t.Errorf("%s: listEntries(prefix %q, delimiter %q, after %q, limit %d) = keys %q, prefixes %q, truncated %v, last %q; want %q, %q, %v, %q",
				tt.name, tt.prefix, tt.delimiter, tt.after, tt.limit, keys, l.prefixes, l.truncated, l.last, tt.wantKeys, tt.wantPrefixes, tt.wantTruncated, tt.wantLast)
		}
	}
}
