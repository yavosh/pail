package diff

import "net/http"

func sigV2Scenario() scenario {
	const key = "dir/a b+c.txt"
	const body = "sigv2 body"
	return scenario{name: "presigned-v2", steps: []step{
		createBucket(),
		{name: "put", method: http.MethodPut, key: key, body: body, auth: authPresignedV2, header: map[string]string{"Content-Type": "text/plain", "Content-MD5": md5Base64(body), "X-Amz-Meta-Color": "blue"}},
		{name: "get", method: http.MethodGet, key: key, auth: authPresignedV2},
		{name: "head", method: http.MethodHead, key: key, auth: authPresignedV2},
		{name: "get-override", method: http.MethodGet, key: key, query: "response-content-type=application%2Fjson", auth: authPresignedV2},
		{name: "get-acl", method: http.MethodGet, key: key, query: "acl", auth: authPresignedV2},
		{name: "expired", method: http.MethodGet, key: key, auth: authPresignedV2Expired},
		{name: "tampered", method: http.MethodGet, key: key, auth: authPresignedV2Tampered},
		{name: "bad-signature", method: http.MethodGet, key: key, auth: authPresignedV2BadSignature},
		{name: "missing-expiry", method: http.MethodGet, key: key, auth: authPresignedV2MissingParam},
		{name: "put-bad-digest", method: http.MethodPut, key: key, body: "replacement", auth: authPresignedV2, header: map[string]string{"Content-MD5": md5Base64(body)}},
		{name: "get-after-failures", method: http.MethodGet, key: key, auth: authPresignedV2},
		{name: "delete", method: http.MethodDelete, key: key, auth: authPresignedV2},
		deleteBucket(),
	}}
}
