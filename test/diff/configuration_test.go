package diff

import (
	"net/http"
	"strings"
)

func corsScenario() scenario {
	const body = `<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><CORSRule><AllowedOrigin>https://*.example.com</AllowedOrigin><AllowedMethod>PUT</AllowedMethod><AllowedMethod>POST</AllowedMethod><AllowedHeader>x-amz-*</AllowedHeader><ExposeHeader>ETag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>`
	return scenario{name: "cors-configuration", steps: []step{
		createBucket(),
		{name: "get-missing", method: http.MethodGet, query: "cors"},
		{name: "put", method: http.MethodPut, query: "cors", body: body, header: map[string]string{"Content-MD5": md5Base64(body)}},
		{name: "get", method: http.MethodGet, query: "cors"},
		{name: "preflight", method: http.MethodOptions, key: "k", auth: authNone, header: map[string]string{"Origin": "https://app.example.com", "Access-Control-Request-Method": "PUT", "Access-Control-Request-Headers": "x-amz-meta-user"}},
		{name: "preflight-denied", method: http.MethodOptions, key: "k", auth: authNone, header: map[string]string{"Origin": "https://elsewhere.example.org", "Access-Control-Request-Method": "PUT"}},
		{name: "put-with-origin", method: http.MethodPut, key: "k", body: "cors", header: map[string]string{"Origin": "https://app.example.com"}},
		{name: "delete-object", method: http.MethodDelete, key: "k"},
		{name: "delete", method: http.MethodDelete, query: "cors"},
		{name: "get-deleted", method: http.MethodGet, query: "cors"},
		deleteBucket(),
	}}
}

func lifecycleRulesScenario() scenario {
	const body = `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>expire</ID><Filter><Prefix>tmp/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule><Rule><ID>abort</ID><Filter></Filter><Status>Disabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`
	return scenario{name: "lifecycle-configuration", steps: []step{
		createBucket(),
		{name: "get-missing", method: http.MethodGet, query: "lifecycle"},
		{name: "put", method: http.MethodPut, query: "lifecycle", body: body, header: map[string]string{"Content-MD5": md5Base64(body)}},
		{name: "get", method: http.MethodGet, query: "lifecycle"},
		{name: "put-invalid", method: http.MethodPut, query: "lifecycle", body: strings.Replace(body, "<Days>7</Days>", "<Days>0</Days>", 1)},
		{name: "get-after-invalid", method: http.MethodGet, query: "lifecycle"},
		{name: "delete", method: http.MethodDelete, query: "lifecycle"},
		{name: "delete-again", method: http.MethodDelete, query: "lifecycle"},
		{name: "get-deleted", method: http.MethodGet, query: "lifecycle"},
		deleteBucket(),
	}}
}

func aclScenario() scenario {
	return scenario{name: "acl-grants", steps: []step{
		{name: "create-bucket", method: http.MethodPut, header: map[string]string{"x-amz-object-ownership": "ObjectWriter"}},
		{name: "get-bucket-acl", method: http.MethodGet, query: "acl"},
		{name: "set-bucket-private", method: http.MethodPut, query: "acl", header: map[string]string{"x-amz-acl": "private"}},
		{name: "put-object", method: http.MethodPut, key: "k", body: "acl"},
		{name: "get-object-acl", method: http.MethodGet, key: "k", query: "acl"},
		{name: "set-object-private", method: http.MethodPut, key: "k", query: "acl", header: map[string]string{"x-amz-acl": "private"}},
		{name: "get-private", method: http.MethodGet, key: "k", query: "acl"},
		{name: "anonymous-get", method: http.MethodGet, key: "k", auth: authNone},
		{name: "delete-object", method: http.MethodDelete, key: "k"},
		deleteBucket(),
	}}
}

func postScenario() scenario {
	fields := map[string]string{"key": "form.txt", "Content-Type": "text/plain", "x-amz-meta-user": "alice", "success_action_status": "201"}
	return scenario{name: "post-policy", steps: []step{
		createBucket(),
		{name: "post", method: http.MethodPost, body: "form bytes", form: fields},
		{name: "get", method: http.MethodGet, key: "form.txt"},
		{name: "bad-signature", method: http.MethodPost, body: "replacement", form: fields, postMutation: "signature"},
		{name: "expired", method: http.MethodPost, body: "replacement", form: fields, postMutation: "expired"},
		{name: "wrong-key", method: http.MethodPost, body: "replacement", form: fields, postMutation: "key"},
		{name: "uncovered", method: http.MethodPost, body: "replacement", form: fields, postMutation: "uncovered"},
		{name: "too-large", method: http.MethodPost, body: strings.Repeat("x", 33), form: fields},
		{name: "get-after-failures", method: http.MethodGet, key: "form.txt"},
		{name: "delete-object", method: http.MethodDelete, key: "form.txt"},
		deleteBucket(),
	}}
}
