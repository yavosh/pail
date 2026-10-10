package diff

import (
	"maps"
	"net/http"
)

// copyConditionsOwnerScenario records CopyObject destination preconditions
// (finding B) and x-amz-expected-bucket-owner on many operations (finding E).
func copyConditionsOwnerScenario() scenario {
	const wrongOwner = "111111111111"
	copyTo := func(name, key string, extra map[string]string) step {
		h := map[string]string{"x-amz-copy-source": "{bucket}/src"}
		maps.Copy(h, extra)
		return step{name: name, method: http.MethodPut, key: key, header: h}
	}
	owner := func(value string) map[string]string { return map[string]string{"x-amz-expected-bucket-owner": value} }
	lifecycle := `<LifecycleConfiguration><Rule><ID>r</ID><Filter><Prefix>tmp/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`
	return scenario{name: "copy-conditions-owner", steps: []step{
		createBucket(),
		{name: "put-src", method: http.MethodPut, key: "src", body: "source"},
		{name: "put-dest", method: http.MethodPut, key: "dest", body: "dest"},
		copyTo("copy-if-none-match-existing", "dest", map[string]string{"If-None-Match": "*"}),
		copyTo("copy-if-none-match-new", "fresh", map[string]string{"If-None-Match": "*"}),
		copyTo("copy-if-match-wrong", "dest", map[string]string{"If-Match": `"wrong"`}),
		copyTo("copy-if-match-right", "dest", map[string]string{"If-Match": etagOf("dest")}),
		copyTo("copy-if-match-missing", "missing", map[string]string{"If-Match": etagOf("dest")}),
		copyTo("copy-if-none-match-etag", "dest", map[string]string{"If-None-Match": etagOf("source")}),
		{name: "get-dest", method: http.MethodGet, key: "dest"},
		{name: "get-owner-wrong", method: http.MethodGet, key: "src", header: owner(wrongOwner)},
		{name: "get-owner-malformed", method: http.MethodGet, key: "src", header: owner("abc")},
		{name: "head-owner-wrong", method: http.MethodHead, key: "src", header: owner(wrongOwner)},
		{name: "put-owner-wrong", method: http.MethodPut, key: "owned", body: "x", header: owner(wrongOwner)},
		{name: "list-owner-wrong", method: http.MethodGet, query: "list-type=2", header: owner(wrongOwner)},
		{name: "location-owner-wrong", method: http.MethodGet, query: "location", header: owner(wrongOwner)},
		{name: "lifecycle-owner-wrong", method: http.MethodPut, query: "lifecycle", body: lifecycle,
			header: map[string]string{"x-amz-expected-bucket-owner": wrongOwner, "Content-MD5": md5Base64(lifecycle)}},
		copyTo("copy-source-owner-wrong", "copied", map[string]string{"x-amz-source-expected-bucket-owner": wrongOwner}),
		copyTo("copy-dest-owner-wrong", "copied", owner(wrongOwner)),
		{name: "create-upload-owner-wrong", method: http.MethodPost, key: "mpu", query: "uploads", header: owner(wrongOwner)},
		{name: "delete-owner-wrong", method: http.MethodDelete, key: "src", header: owner(wrongOwner)},
		{name: "get-after-owner-wrong", method: http.MethodGet, key: "src"},
		{name: "delete-src", method: http.MethodDelete, key: "src"},
		{name: "delete-dest", method: http.MethodDelete, key: "dest"},
		{name: "delete-fresh", method: http.MethodDelete, key: "fresh"},
		deleteBucket(),
	}}
}
