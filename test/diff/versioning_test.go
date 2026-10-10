package diff

import "net/http"

// versionHeaders are compared in versioningScenario. Version IDs are masked
// unless they are "null".
var versionHeaders = []string{"X-Amz-Version-Id", "X-Amz-Delete-Marker", "X-Amz-Copy-Source-Version-Id"}

// versioningScenario records bucket versioning: version IDs, version reads,
// listings, delete markers, permanent deletes, null versions, and suspension.
// {versionId} is the latest version a write returned; {previousVersionId} the one before.
func versioningScenario() scenario {
	config := func(status string) string {
		return `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>` + status + `</Status></VersioningConfiguration>`
	}
	setVersioning := func(name, body string) step {
		return step{name: name, method: http.MethodPut, query: "versioning", body: body, header: map[string]string{"Content-MD5": md5Base64(body)}}
	}
	obj := func(name, method, key, query, body string) step {
		return step{name: name, method: method, key: key, query: query, body: body, compare: versionHeaders}
	}
	mfa := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>`
	batch := `<Delete><Object><Key>pre</Key><VersionId>null</VersionId></Object><Object><Key>gone</Key></Object></Delete>`
	return scenario{name: "versioning", steps: []step{
		createBucket(),
		{name: "get-versioning-new", method: http.MethodGet, query: "versioning"},
		obj("put-pre", http.MethodPut, "pre", "", "pre"),
		setVersioning("enable", config("Enabled")),
		{name: "get-versioning-enabled", method: http.MethodGet, query: "versioning"},
		obj("put-v1", http.MethodPut, "k", "", "one"),
		obj("put-v2", http.MethodPut, "k", "", "two"),
		obj("get-latest", http.MethodGet, "k", "", ""),
		obj("get-previous", http.MethodGet, "k", "versionId={previousVersionId}", ""),
		obj("head-previous", http.MethodHead, "k", "versionId={previousVersionId}", ""),
		obj("get-bogus-version", http.MethodGet, "k", "versionId=bogus", ""),
		{name: "list-versions", method: http.MethodGet, query: "versions"},
		{name: "list-versions-page", method: http.MethodGet, query: "max-keys=1&prefix=k&versions"},
		{name: "list-objects", method: http.MethodGet, query: "list-type=2"},
		obj("delete-latest", http.MethodDelete, "k", "", ""),
		obj("get-deleted", http.MethodGet, "k", "", ""),
		obj("head-deleted", http.MethodHead, "k", "", ""),
		obj("get-v2-under-marker", http.MethodGet, "k", "versionId={previousVersionId}", ""),
		{name: "list-versions-with-marker", method: http.MethodGet, query: "versions"},
		obj("get-marker", http.MethodGet, "k", "versionId={versionId}", ""),
		obj("delete-marker", http.MethodDelete, "k", "versionId={versionId}", ""),
		obj("get-restored", http.MethodGet, "k", "", ""),
		obj("delete-v2", http.MethodDelete, "k", "versionId={previousVersionId}", ""),
		obj("get-after-v2-delete", http.MethodGet, "k", "", ""),
		{name: "copy-null-version", method: http.MethodPut, key: "pre-copy", compare: versionHeaders,
			header: map[string]string{"x-amz-copy-source": "{bucket}/pre?versionId=null"}},
		obj("get-pre-null", http.MethodGet, "pre", "versionId=null", ""),
		setVersioning("suspend", config("Suspended")),
		{name: "get-versioning-suspended", method: http.MethodGet, query: "versioning"},
		obj("put-suspended", http.MethodPut, "k", "", "three"),
		{name: "list-versions-suspended", method: http.MethodGet, query: "versions"},
		obj("delete-suspended", http.MethodDelete, "k", "", ""),
		setVersioning("versioning-bogus", config("Bogus")),
		setVersioning("versioning-mfa", mfa),
		{name: "delete-batch", method: http.MethodPost, query: "delete", body: batch, header: map[string]string{"Content-MD5": md5Base64(batch)}},
		{name: "list-versions-final", method: http.MethodGet, query: "versions"},
	}}
}
