package s3api

import (
	"encoding/xml"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/store"
)

// TestVersioningFlow drives one bucket through the versioning states. A "{x}"
// in a target or header value is the version ID saved as x; "-" in want means the header is absent.
func TestVersioningFlow(t *testing.T) {
	srv, _ := storeServer(t, "")
	config := func(inner string) string {
		return `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` + inner + `</VersioningConfiguration>`
	}
	steps := []struct {
		name, method, target, body string
		headers                    map[string]string
		status                     int
		code                       string
		want                       map[string]string
		save                       string // the x-amz-version-id of the response
	}{
		{name: "create bucket", method: http.MethodPut, target: "/bkt", status: 200},
		{name: "never versioned", method: http.MethodGet, target: "/bkt?versioning", status: 200, want: map[string]string{"Content-Type": "-"}},
		{name: "put before versioning", method: http.MethodPut, target: "/bkt/pre", body: "pre", status: 200, want: map[string]string{"x-amz-version-id": "-"}},
		{name: "bad status", method: http.MethodPut, target: "/bkt?versioning", body: config("<Status>Bogus</Status>"), status: 400, code: "MalformedXML"},
		{name: "no status", method: http.MethodPut, target: "/bkt?versioning", body: config(""), status: 400, code: "MalformedXML"},
		{name: "MFA delete", method: http.MethodPut, target: "/bkt?versioning", body: config("<Status>Enabled</Status><MfaDelete>Enabled</MfaDelete>"), status: 403, code: "AccessDenied"},
		{name: "enable", method: http.MethodPut, target: "/bkt?versioning", body: config("<Status>Enabled</Status>"), status: 200},
		{name: "read null object", method: http.MethodGet, target: "/bkt/pre", status: 200, want: map[string]string{"x-amz-version-id": "null"}},
		{name: "first version", method: http.MethodPut, target: "/bkt/k", body: "one", status: 200, save: "v1"},
		{name: "second version", method: http.MethodPut, target: "/bkt/k", body: "two", status: 200, save: "v2"},
		{name: "get latest", method: http.MethodGet, target: "/bkt/k", status: 200, want: map[string]string{"x-amz-version-id": "{v2}"}},
		{name: "get old", method: http.MethodGet, target: "/bkt/k?versionId={v1}", status: 200, want: map[string]string{"x-amz-version-id": "{v1}"}},
		{name: "head old", method: http.MethodHead, target: "/bkt/k?versionId={v1}", status: 200, want: map[string]string{"x-amz-version-id": "{v1}"}},
		{name: "get unknown", method: http.MethodGet, target: "/bkt/k?versionId=" + goodID, status: 404, code: "NoSuchVersion"},
		{name: "attributes of old", method: http.MethodGet, target: "/bkt/k?attributes&versionId={v1}", headers: map[string]string{"x-amz-object-attributes": "ObjectSize"}, status: 200, want: map[string]string{"x-amz-version-id": "{v1}"}},
		{name: "tag old", method: http.MethodPut, target: "/bkt/k?tagging&versionId={v1}", body: `<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`, status: 200, want: map[string]string{"x-amz-version-id": "{v1}"}},
		{name: "tags of old", method: http.MethodGet, target: "/bkt/k?tagging&versionId={v1}", status: 200, want: map[string]string{"x-amz-version-id": "{v1}"}},
		{name: "copy old", method: http.MethodPut, target: "/bkt/copy", headers: map[string]string{"x-amz-copy-source": "bkt/k?versionId={v1}"}, status: 200,
			want: map[string]string{"x-amz-copy-source-version-id": "{v1}", "x-amz-version-id": "+"}},
		{name: "delete creates a marker", method: http.MethodDelete, target: "/bkt/k", status: 204, save: "m1", want: map[string]string{"x-amz-delete-marker": "true"}},
		{name: "get under the marker", method: http.MethodGet, target: "/bkt/k", status: 404, code: "NoSuchKey", want: map[string]string{"x-amz-delete-marker": "true", "x-amz-version-id": "{m1}"}},
		{name: "head under the marker", method: http.MethodHead, target: "/bkt/k", status: 404, want: map[string]string{"x-amz-delete-marker": "true", "x-amz-version-id": "{m1}"}},
		{name: "get the marker", method: http.MethodGet, target: "/bkt/k?versionId={m1}", status: 405, code: "MethodNotAllowed", want: map[string]string{"x-amz-delete-marker": "true", "x-amz-version-id": "{m1}", "Last-Modified": "+"}},
		{name: "copy the marker", method: http.MethodPut, target: "/bkt/copy2", headers: map[string]string{"x-amz-copy-source": "bkt/k?versionId={m1}"}, status: 400, code: "InvalidRequest"},
		{name: "copy under the marker", method: http.MethodPut, target: "/bkt/copy2", headers: map[string]string{"x-amz-copy-source": "bkt/k"}, status: 404, code: "NoSuchKey"},
		{name: "list objects hides the key", method: http.MethodGet, target: "/bkt?list-type=2&prefix=k", status: 200, want: map[string]string{"Content-Type": "application/xml"}},
		{name: "delete the marker", method: http.MethodDelete, target: "/bkt/k?versionId={m1}", status: 204, want: map[string]string{"x-amz-delete-marker": "true", "x-amz-version-id": "{m1}"}},
		{name: "restored", method: http.MethodGet, target: "/bkt/k", status: 200, want: map[string]string{"x-amz-version-id": "{v2}"}},
		{name: "delete a version", method: http.MethodDelete, target: "/bkt/k?versionId={v2}", status: 204, want: map[string]string{"x-amz-version-id": "{v2}", "x-amz-delete-marker": "-"}},
		{name: "promoted", method: http.MethodGet, target: "/bkt/k", status: 200, want: map[string]string{"x-amz-version-id": "{v1}"}},
		{name: "suspend", method: http.MethodPut, target: "/bkt?versioning", body: config("<Status>Suspended</Status>"), status: 200},
		{name: "suspended put", method: http.MethodPut, target: "/bkt/k", body: "three", status: 200, want: map[string]string{"x-amz-version-id": "-"}},
		{name: "get null", method: http.MethodGet, target: "/bkt/k", status: 200, want: map[string]string{"x-amz-version-id": "null"}},
		{name: "suspended delete", method: http.MethodDelete, target: "/bkt/k", status: 204, want: map[string]string{"x-amz-delete-marker": "true", "x-amz-version-id": "null"}},
		{name: "version marker without key", method: http.MethodGet, target: "/bkt?versions&version-id-marker=null", status: 400, code: "InvalidArgument"},
		{name: "bad marker", method: http.MethodGet, target: "/bkt?versions&key-marker=k&version-id-marker=bogus", status: 400, code: "InvalidArgument"},
		{name: "bad max-keys", method: http.MethodGet, target: "/bkt?versions&max-keys=-1", status: 400, code: "InvalidArgument"},
		{name: "missing bucket", method: http.MethodGet, target: "/nobkt?versions", status: 404, code: "NoSuchBucket"},
		{name: "versioning of a missing bucket", method: http.MethodGet, target: "/nobkt?versioning", status: 404, code: "NoSuchBucket"},
	}
	ids := map[string]string{}
	expand := func(s string) string {
		for name, id := range ids {
			s = strings.ReplaceAll(s, "{"+name+"}", id)
		}
		return s
	}
	for _, tt := range steps {
		headers := map[string]string{}
		for k, v := range tt.headers {
			headers[k] = expand(v)
		}
		r := call(t, srv, tt.method, expand(tt.target), tt.body, headers)
		if r.status != tt.status || r.code != tt.code {
			t.Errorf("%s: %s %s = %d %q, want %d %q (%s)", tt.name, tt.method, tt.target, r.status, r.code, tt.status, tt.code, r.body)
			continue
		}
		for name, want := range tt.want {
			got := r.header.Get(name)
			switch want = expand(want); {
			case want == "-" && got != "", want == "+" && got == "", want != "-" && want != "+" && got != want:
				t.Errorf("%s: header %s = %q, want %q", tt.name, name, got, want)
			}
		}
		if tt.save != "" {
			ids[tt.save] = r.header.Get("x-amz-version-id")
			if ids[tt.save] == "" {
				t.Errorf("%s: no x-amz-version-id to save", tt.name)
			}
		}
	}
}

func TestListObjectVersionsPaging(t *testing.T) {
	srv, st := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	if err := st.PutBucketConfiguration(t.Context(), "bkt", "versioning", &store.BucketConfiguration{Status: store.VersioningEnabled}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "a", "b/1", "b/2", "c"} {
		_ = call(t, srv, http.MethodPut, "/bkt/"+key, key, nil)
	}
	_ = call(t, srv, http.MethodDelete, "/bkt/c", "", nil)

	type result struct {
		IsTruncated         bool
		KeyMarker           string
		NextKeyMarker       string
		NextVersionIDMarker string `xml:"NextVersionIdMarker"`
		Version             []struct {
			Key       string
			VersionID string `xml:"VersionId"`
		}
		DeleteMarker   []struct{ Key string }
		CommonPrefixes []struct{ Prefix string }
	}
	fetch := func(query string) result {
		t.Helper()
		r := call(t, srv, http.MethodGet, "/bkt?versions&"+query, "", nil)
		var out result
		if r.status != 200 || xml.Unmarshal([]byte(r.body), &out) != nil {
			t.Fatalf("GET ?versions&%s = %d %s", query, r.status, r.body)
		}
		return out
	}
	// Walk the listing two entries at a time, as a client does.
	var keys []string
	query := "max-keys=2"
	for range 10 {
		page := fetch(query)
		for _, v := range page.Version {
			keys = append(keys, v.Key)
		}
		for _, m := range page.DeleteMarker {
			keys = append(keys, m.Key+"(marker)")
		}
		if !page.IsTruncated {
			break
		}
		query = "max-keys=2&key-marker=" + page.NextKeyMarker + "&version-id-marker=" + page.NextVersionIDMarker
	}
	// Entries of a page come as versions, then markers; the total is what counts.
	slices.Sort(keys)
	if want := []string{"a", "a", "b/1", "b/2", "c", "c(marker)"}; !slices.Equal(keys, want) {
		t.Errorf("paged keys = %v, want %v", keys, want)
	}
	if page := fetch("delimiter=/"); len(page.CommonPrefixes) != 1 || page.CommonPrefixes[0].Prefix != "b/" || len(page.Version) != 3 {
		t.Errorf("delimiter listing = %+v, want prefix b/ and 3 versions", page)
	}
	if page := fetch("key-marker=a"); len(page.Version) != 3 || page.Version[0].Key != "b/1" {
		t.Errorf("key-marker=a listing = %+v, want to start at b/1", page.Version)
	}
	if page := fetch("max-keys=0"); page.IsTruncated || len(page.Version) != 0 {
		t.Errorf("max-keys=0 listing = %+v, want empty and not truncated", page)
	}
}

func TestPageVersions(t *testing.T) {
	entry := func(key, id string, marker bool) store.VersionInfo {
		return store.VersionInfo{Key: key, VersionID: id, DeleteMarker: marker, LastModified: time.Unix(0, 0)}
	}
	entries := []store.VersionInfo{entry("a", "a2", false), entry("a", "a1", false), entry("b/x", "", false), entry("b/y", "y1", true), entry("c", "c1", false)}
	tests := []struct {
		name                        string
		delimiter, keyMarker, vMark string
		limit                       int
		want                        []string // "key:id", prefixes as "prefix/"
		truncated                   bool
		nextKey, nextVersion        string
	}{
		{name: "all", limit: 10, want: []string{"a:a2", "a:a1", "b/x:null", "b/y:y1", "c:c1"}},
		{name: "first page", limit: 2, want: []string{"a:a2", "a:a1"}, truncated: true, nextKey: "a", nextVersion: "a1"},
		{name: "resume inside a key", limit: 2, keyMarker: "a", vMark: "a2", want: []string{"a:a1", "b/x:null"}, truncated: true, nextKey: "b/x", nextVersion: "null"},
		{name: "key marker alone skips the key", limit: 10, keyMarker: "a", want: []string{"b/x:null", "b/y:y1", "c:c1"}},
		{name: "unknown version skips the key", limit: 10, keyMarker: "a", vMark: "zz", want: []string{"b/x:null", "b/y:y1", "c:c1"}},
		{name: "last version of a key", limit: 10, keyMarker: "a", vMark: "a1", want: []string{"b/x:null", "b/y:y1", "c:c1"}},
		{name: "delimiter", delimiter: "/", limit: 10, want: []string{"a:a2", "a:a1", "b/", "c:c1"}},
		{name: "delimiter page", delimiter: "/", limit: 3, want: []string{"a:a2", "a:a1", "b/"}, truncated: true, nextKey: "b/"},
		{name: "marker inside a prefix skips it", delimiter: "/", limit: 10, keyMarker: "b/x", vMark: "null", want: []string{"c:c1"}},
		{name: "zero limit", limit: 0},
	}
	for _, tt := range tests {
		p := pageVersions(entries, "", tt.delimiter, tt.keyMarker, tt.vMark, tt.limit)
		var got []string
		prefixes := p.prefixes
		for _, v := range p.versions {
			got = append(got, v.Key+":"+versionLabel(v.VersionID))
		}
		got = append(got, prefixes...)
		slices.SortStableFunc(got, func(a, b string) int {
			return strings.Compare(strings.SplitN(a, ":", 2)[0], strings.SplitN(b, ":", 2)[0])
		})
		if !slices.Equal(got, tt.want) || p.truncated != tt.truncated || p.truncated && (p.nextKey != tt.nextKey || p.nextVersion != tt.nextVersion) {
			t.Errorf("%s: pageVersions = %v truncated=%v next=%q/%q, want %v truncated=%v next=%q/%q", tt.name, got, p.truncated, p.nextKey, p.nextVersion, tt.want, tt.truncated, tt.nextKey, tt.nextVersion)
		}
	}
}
