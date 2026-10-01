package diff

import (
	"crypto/md5"
	"encoding/hex"
	"net/http"
)

// step is one raw S3 request. key empty addresses the bucket. query is
// already encoded. "{bucket}" in query and header values becomes the bucket.
type step struct {
	name   string
	method string
	key    string
	query  string
	header map[string]string
	body   string
	auth   authMode
}

// scenario runs its steps in order against one fresh bucket name. Scenarios
// never call ListBuckets: on AWS it would expose the account's other buckets.
type scenario struct {
	name  string
	steps []step
}

func etagOf(body string) string {
	sum := md5.Sum([]byte(body))
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func createBucket() step { return step{name: "create-bucket", method: http.MethodPut} }
func deleteBucket() step { return step{name: "delete-bucket", method: http.MethodDelete} }

// specialKeys are keys that path handling tends to break.
var specialKeys = []struct{ name, key string }{
	{"double-slash", "a//b"},
	{"dot-dot", "../x"},
	{"plus", "a+b"},
	{"space", "a b"},
	{"unicode", "✓"},
	{"folder-marker", "dir/"},
}

func scenarios() []scenario {
	const body = "hello world"
	all := []scenario{
		{name: "auth-errors", steps: []step{
			createBucket(),
			{name: "no-credentials", method: http.MethodGet, query: "list-type=2", auth: authNone},
			{name: "unknown-key", method: http.MethodGet, query: "list-type=2", auth: authUnknownKey},
			{name: "bad-signature", method: http.MethodGet, query: "list-type=2", auth: authBadSignature},
			{name: "clock-skew", method: http.MethodGet, query: "list-type=2", auth: authSkewed},
			deleteBucket(),
		}},
		{name: "bucket-lifecycle", steps: []step{
			{name: "head-missing", method: http.MethodHead},
			{name: "delete-missing", method: http.MethodDelete},
			createBucket(),
			{name: "create-again", method: http.MethodPut},
			{name: "head", method: http.MethodHead},
			{name: "location", method: http.MethodGet, query: "location"},
			{name: "put-object", method: http.MethodPut, key: "k", body: "x"},
			{name: "delete-not-empty", method: http.MethodDelete},
			{name: "delete-object", method: http.MethodDelete, key: "k"},
			deleteBucket(),
			{name: "head-after-delete", method: http.MethodHead},
		}},
		{name: "object-basics", steps: []step{
			createBucket(),
			{name: "put", method: http.MethodPut, key: "docs/hello.txt", body: body, header: map[string]string{
				"Content-Type":        "text/plain",
				"Cache-Control":       "max-age=60",
				"Content-Disposition": "attachment",
				"X-Amz-Meta-Color":    "blue",
			}},
			{name: "head", method: http.MethodHead, key: "docs/hello.txt"},
			{name: "get", method: http.MethodGet, key: "docs/hello.txt"},
			{name: "get-range", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"Range": "bytes=0-4"}},
			{name: "get-suffix-range", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"Range": "bytes=-5"}},
			{name: "get-range-unsatisfiable", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"Range": "bytes=100-200"}},
			{name: "get-if-none-match", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"If-None-Match": etagOf(body)}},
			{name: "get-if-match-mismatch", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"If-Match": `"nope"`}},
			{name: "get-override", method: http.MethodGet, key: "docs/hello.txt", query: "response-content-type=application%2Fjson"},
			{name: "put-if-none-match-star", method: http.MethodPut, key: "docs/hello.txt", body: "again", header: map[string]string{"If-None-Match": "*"}},
			{name: "get-missing", method: http.MethodGet, key: "missing"},
			{name: "delete", method: http.MethodDelete, key: "docs/hello.txt"},
			{name: "delete-again", method: http.MethodDelete, key: "docs/hello.txt"},
			deleteBucket(),
		}},
	}

	keys := scenario{name: "special-keys", steps: []step{createBucket()}}
	for _, k := range specialKeys {
		keys.steps = append(keys.steps,
			step{name: "put-" + k.name, method: http.MethodPut, key: k.key, body: k.key},
			step{name: "get-" + k.name, method: http.MethodGet, key: k.key},
		)
	}
	keys.steps = append(keys.steps, step{name: "list", method: http.MethodGet, query: "list-type=2"})
	for _, k := range specialKeys {
		keys.steps = append(keys.steps, step{name: "delete-" + k.name, method: http.MethodDelete, key: k.key})
	}
	keys.steps = append(keys.steps, deleteBucket())
	return append(all, keys, listingScenario())
}

// listingKeys exercise delimiters, nesting, and characters that encoding-type=url changes.
var listingKeys = []struct{ name, key string }{
	{"a", "a"},
	{"a-b", "a/b"},
	{"a-c-d", "a/c/d"},
	{"dir", "dir/"},
	{"dir-x", "dir/x"},
	{"e-1", "e-1"},
	{"e-2", "e-2"},
	{"space-plus", "sp ace+plus/k"},
}

// listingScenario records how AWS pages, groups, and encodes listings.
func listingScenario() scenario {
	sc := scenario{name: "listing", steps: []step{createBucket()}}
	for _, k := range listingKeys {
		sc.steps = append(sc.steps, step{name: "put-" + k.name, method: http.MethodPut, key: k.key})
	}
	lists := []struct{ name, query string }{
		{"v2-root-folders", "list-type=2&delimiter=%2F"},
		{"v2-inside-a-folder", "list-type=2&prefix=a%2F&delimiter=%2F"},
		{"v2-other-delimiter", "list-type=2&delimiter=-"},
		{"v2-first-page", "list-type=2&max-keys=1"},
		{"v2-start-after-inside-a-prefix", "list-type=2&delimiter=%2F&start-after=a%2Fb"},
		{"v2-url-encoding", "list-type=2&encoding-type=url"},
		{"v2-max-keys-zero", "list-type=2&max-keys=0"},
		{"v2-max-keys-over-limit", "list-type=2&max-keys=5000"},
		{"v2-fetch-owner", "list-type=2&prefix=e&fetch-owner=true"},
		{"v1-page-with-delimiter", "max-keys=2&delimiter=%2F"},
		{"v1-page-without-delimiter", "max-keys=2"},
		{"v1-url-encoding-and-marker", "encoding-type=url&marker=a%2Fb"},
	}
	for _, l := range lists {
		sc.steps = append(sc.steps, step{name: l.name, method: http.MethodGet, query: l.query})
	}
	for _, k := range listingKeys {
		sc.steps = append(sc.steps, step{name: "delete-" + k.name, method: http.MethodDelete, key: k.key})
	}
	sc.steps = append(sc.steps, deleteBucket())
	return sc
}
