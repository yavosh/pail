package s3api

import (
	"context"
	"net/http"
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
		{"zero limit", "", "", "", 0, nil, nil, false, ""},
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

func TestListObjectsParameters(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a b", "a+b", "c"} {
		if _, err := st.PutObject(ctx, "bkt", k, strings.NewReader(""), store.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name, query   string
		wantStatus    int
		wantCode      string
		want, wantNot []string
	}{
		{"bad max-keys", "list-type=2&max-keys=abc", http.StatusBadRequest, "InvalidArgument", nil, nil},
		{"negative max-keys", "list-type=2&max-keys=-1", http.StatusBadRequest, "InvalidArgument", nil, nil},
		{"bad encoding-type", "list-type=2&encoding-type=base64", http.StatusBadRequest, "InvalidArgument", nil, nil},
		{"bad token", "list-type=2&continuation-token=%21%21", http.StatusBadRequest, "InvalidArgument", nil, nil},
		{"max-keys zero", "list-type=2&max-keys=0", http.StatusOK, "", []string{"<KeyCount>0</KeyCount>", "<IsTruncated>false</IsTruncated>"}, []string{"<Contents>"}},
		{"max-keys capped", "list-type=2&max-keys=5000", http.StatusOK, "", []string{"<MaxKeys>1000</MaxKeys>"}, nil},
		{"v2 has no owner by default", "list-type=2", http.StatusOK, "", nil, []string{"<Owner>"}},
		{"v2 fetch-owner", "list-type=2&fetch-owner=TRUE", http.StatusOK, "", []string{"<Owner>"}, nil},
		{"v1 always has an owner", "", http.StatusOK, "", []string{"<Owner>"}, nil},
		{"v1 without a delimiter has no NextMarker", "max-keys=1", http.StatusOK, "", []string{"<IsTruncated>true</IsTruncated>"}, []string{"<NextMarker>"}},
		{"v1 with a delimiter has a NextMarker", "max-keys=1&delimiter=%2F", http.StatusOK, "", []string{"<NextMarker>a b</NextMarker>"}, nil},
		{"v1 url encoding", "encoding-type=url&marker=a%20b", http.StatusOK, "", []string{"<Marker>a+b</Marker>", "<Key>a%2Bb</Key>"}, nil},
		{"v2 start-after", "list-type=2&start-after=a%2Bb", http.StatusOK, "", []string{"<Key>c</Key>", "<StartAfter>a+b</StartAfter>"}, []string{"<Key>a b</Key>"}},
	}
	for _, tt := range tests {
		status, code, body := doBody(t, srv, http.MethodGet, "/bkt?"+tt.query, "")
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: GET /bkt?%s = %d %q, want %d %q", tt.name, tt.query, status, code, tt.wantStatus, tt.wantCode)
			continue
		}
		for _, w := range tt.want {
			if !strings.Contains(string(body), w) {
				t.Errorf("%s: GET /bkt?%s body lacks %q:\n%s", tt.name, tt.query, w, body)
			}
		}
		for _, w := range tt.wantNot {
			if strings.Contains(string(body), w) {
				t.Errorf("%s: GET /bkt?%s body has %q, want none:\n%s", tt.name, tt.query, w, body)
			}
		}
	}
}
