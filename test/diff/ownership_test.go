package diff

import "net/http"

// ownershipScenario records bucket ownership controls: BucketOwnerEnforced
// disables ACLs, ObjectWriter enables them, and the configuration API (finding F).
func ownershipScenario() scenario {
	controls := func(ownership string) string {
		return `<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ObjectOwnership>` + ownership + `</ObjectOwnership></Rule></OwnershipControls>`
	}
	putControls := func(name, ownership string) step {
		body := controls(ownership)
		return step{name: name, method: http.MethodPut, query: "ownershipControls", body: body, header: map[string]string{"Content-MD5": md5Base64(body)}}
	}
	acl := func(value string) map[string]string { return map[string]string{"x-amz-acl": value} }
	return scenario{name: "ownership-controls", steps: []step{
		{name: "create-bucket-bogus-ownership", method: http.MethodPut, header: map[string]string{"x-amz-object-ownership": "Bogus"}},
		{name: "create-bucket", method: http.MethodPut, header: map[string]string{"x-amz-object-ownership": "BucketOwnerEnforced"}},
		{name: "get-ownership", method: http.MethodGet, query: "ownershipControls"},
		{name: "get-bucket-acl", method: http.MethodGet, query: "acl"},
		{name: "put-public-read", method: http.MethodPut, key: "k", body: "x", header: acl("public-read")},
		{name: "put-private", method: http.MethodPut, key: "k", body: "x", header: acl("private")},
		{name: "put-owner-full-control", method: http.MethodPut, key: "k", body: "x", header: acl("bucket-owner-full-control")},
		{name: "put-no-acl", method: http.MethodPut, key: "k", body: "x"},
		{name: "get-object-acl", method: http.MethodGet, key: "k", query: "acl"},
		{name: "put-object-acl", method: http.MethodPut, key: "k", query: "acl", header: acl("private")},
		{name: "put-bucket-acl", method: http.MethodPut, query: "acl", header: acl("private")},
		putControls("put-object-writer", "ObjectWriter"),
		{name: "get-ownership-after", method: http.MethodGet, query: "ownershipControls"},
		{name: "put-private-after", method: http.MethodPut, key: "k", body: "x", header: acl("private")},
		{name: "put-object-acl-after", method: http.MethodPut, key: "k", query: "acl", header: acl("private")},
		putControls("put-preferred", "BucketOwnerPreferred"),
		{name: "get-ownership-preferred", method: http.MethodGet, query: "ownershipControls"},
		putControls("put-bogus", "Bogus"),
		{name: "put-no-md5", method: http.MethodPut, query: "ownershipControls", body: controls("ObjectWriter")},
		{name: "put-malformed", method: http.MethodPut, query: "ownershipControls", body: "<OwnershipControls>", header: map[string]string{"Content-MD5": md5Base64("<OwnershipControls>")}},
		{name: "delete-ownership", method: http.MethodDelete, query: "ownershipControls"},
		{name: "get-ownership-deleted", method: http.MethodGet, query: "ownershipControls"},
		{name: "put-private-no-controls", method: http.MethodPut, key: "k", body: "x", header: acl("private")},
		{name: "delete-object", method: http.MethodDelete, key: "k"},
		deleteBucket(),
	}}
}
