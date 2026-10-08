package diff

import (
	"net/http"
	"strings"
)

func conditionalDeleteScenario() scenario {
	const body = "original"
	etag := etagOf(body)
	batch := "<Delete>" +
		"<Object><Key>match</Key><ETag>" + strings.Trim(etag, `"`) + "</ETag></Object>" +
		"<Object><Key>mismatch</Key><ETag>wrong</ETag></Object>" +
		"<Object><Key>wildcard</Key><ETag>*</ETag></Object>" +
		"<Object><Key>stale</Key><ETag>" + etag + "</ETag></Object>" +
		"<Object><Key>missing-etag</Key><ETag>" + etag + "</ETag></Object>" +
		"<Object><Key>missing-wildcard</Key><ETag>*</ETag></Object>" +
		"<Object><Key>missing-unconditional</Key></Object>" +
		"</Delete>"
	quiet := "<Delete><Quiet>true</Quiet>" +
		"<Object><Key>mismatch</Key><ETag>wrong</ETag></Object>" +
		"<Object><Key>stale</Key><ETag>" + etagOf("replacement") + "</ETag></Object>" +
		"<Object><Key>missing-wildcard</Key><ETag>*</ETag></Object>" +
		"</Delete>"
	return scenario{name: "conditional-deletes", steps: []step{
		createBucket(),
		{name: "put", method: http.MethodPut, key: "key", body: body},
		{name: "delete-mismatch", method: http.MethodDelete, key: "key", header: map[string]string{"If-Match": `"wrong"`}},
		{name: "get-after-mismatch", method: http.MethodGet, key: "key"},
		{name: "delete-match", method: http.MethodDelete, key: "key", header: map[string]string{"If-Match": etag}},
		{name: "delete-missing-unconditional", method: http.MethodDelete, key: "key"},
		{name: "delete-missing-etag", method: http.MethodDelete, key: "key", header: map[string]string{"If-Match": etag}},
		{name: "delete-missing-wildcard", method: http.MethodDelete, key: "key", header: map[string]string{"If-Match": "*"}},
		{name: "put-wildcard", method: http.MethodPut, key: "key", body: body},
		{name: "delete-wildcard", method: http.MethodDelete, key: "key", header: map[string]string{"If-Match": "*"}},
		{name: "put-before-overwrite", method: http.MethodPut, key: "key", body: body},
		{name: "overwrite", method: http.MethodPut, key: "key", body: "replacement"},
		{name: "delete-stale", method: http.MethodDelete, key: "key", header: map[string]string{"If-Match": etag}},
		{name: "get-after-stale", method: http.MethodGet, key: "key"},
		{name: "delete-key", method: http.MethodDelete, key: "key"},
		{name: "put-match", method: http.MethodPut, key: "match", body: body},
		{name: "put-mismatch", method: http.MethodPut, key: "mismatch", body: body},
		{name: "put-batch-wildcard", method: http.MethodPut, key: "wildcard", body: body},
		{name: "put-stale", method: http.MethodPut, key: "stale", body: body},
		{name: "overwrite-stale", method: http.MethodPut, key: "stale", body: "replacement"},
		{name: "batch-mixed", method: http.MethodPost, query: "delete", body: batch, header: map[string]string{"Content-MD5": md5Base64(batch)}},
		{name: "get-batch-mismatch", method: http.MethodGet, key: "mismatch"},
		{name: "get-batch-stale", method: http.MethodGet, key: "stale"},
		{name: "get-batch-match", method: http.MethodGet, key: "match"},
		{name: "get-batch-wildcard", method: http.MethodGet, key: "wildcard"},
		{name: "batch-quiet", method: http.MethodPost, query: "delete", body: quiet, header: map[string]string{"Content-MD5": md5Base64(quiet)}},
		{name: "get-after-quiet-mismatch", method: http.MethodGet, key: "mismatch"},
		{name: "get-after-quiet-match", method: http.MethodGet, key: "stale"},
		{name: "delete-remaining", method: http.MethodDelete, key: "mismatch"},
		deleteBucket(),
	}}
}
