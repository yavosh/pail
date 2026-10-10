package diff

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestNormalizeError(t *testing.T) {
	r := response{
		status: http.StatusNotFound,
		header: http.Header{
			"Content-Type":     {"application/xml"},
			"Content-Length":   {"312"},
			"X-Amz-Request-Id": {"ABC"},
			"Date":             {"Wed, 01 Oct 2026 10:00:00 GMT"},
			"Server":           {"AmazonS3"},
		},
		body: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>pail-diff-1</BucketName><RequestId>ABC</RequestId><HostId>xyz</HostId></Error>`),
	}
	got := normalize(step{name: "s", method: http.MethodGet}, "pail-diff-1", r)
	if got.Body != "Error\n  Code: NoSuchBucket\n" {
		t.Errorf("body = %q, want the code only", got.Body)
	}
	want := map[string]string{"Content-Type": "application/xml", "X-Amz-Request-Id": "<present>"}
	if len(got.Headers) != len(want) {
		t.Errorf("headers = %v, want %v", got.Headers, want)
	}
	for k, v := range want {
		if got.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, got.Headers[k], v)
		}
	}
}

func TestNormalizeListing(t *testing.T) {
	r := response{
		status: http.StatusOK,
		header: http.Header{"Content-Type": {"application/xml"}},
		body: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>pail-diff-1</Name><KeyCount>1</KeyCount>
<Contents><Key>a b</Key><LastModified>2026-10-01T10:00:00.000Z</LastModified><ETag>&quot;e&quot;</ETag><Size>3</Size></Contents></ListBucketResult>`),
	}
	got := normalize(step{name: "s", method: http.MethodGet, query: "list-type=2"}, "pail-diff-1", r)
	want := `ListBucketResult
  Name: {bucket}
  KeyCount: 1
  Contents
    Key: a b
    LastModified: <volatile>
    ETag: "e"
    Size: 3
`
	if got.Body != want {
		t.Errorf("body =\n%s\nwant\n%s", got.Body, want)
	}
	if got.Request != "GET /?list-type=2" {
		t.Errorf("request = %q, want %q", got.Request, "GET /?list-type=2")
	}
}

func TestNormalizeObject(t *testing.T) {
	r := response{
		status: http.StatusOK,
		header: http.Header{
			"Etag":                         {`"abc"`},
			"Content-Length":               {"5"},
			"Last-Modified":                {"Wed, 01 Oct 2026 10:00:00 GMT"},
			"X-Amz-Meta-Color":             {"blue"},
			"X-Amz-Server-Side-Encryption": {"AES256"},
		},
		body: []byte("hello"),
	}
	got := normalize(step{name: "s", method: http.MethodGet, key: "k"}, "pail-diff-1", r)
	want := map[string]string{`Etag`: `"abc"`, "Content-Length": "5", "Last-Modified": "<present>", "X-Amz-Meta-Color": "blue"}
	if got.Body != "hello" || len(got.Headers) != len(want) {
		t.Errorf("normalize = %+v, want body hello and headers %v", got, want)
	}
	for k, v := range want {
		if got.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, got.Headers[k], v)
		}
	}
}

func TestCompare(t *testing.T) {
	want := exchange{Step: "get", Status: 200, Headers: map[string]string{"Etag": `"a"`}, Body: "x"}
	got := exchange{Step: "get", Status: 404, Headers: map[string]string{"Content-Type": "text/plain"}, Body: "y"}
	diffs := compare(want, got)
	for _, key := range []string{"get status", "get header:Etag", "get header:Content-Type", "get body"} {
		if _, ok := diffs[key]; !ok {
			t.Errorf("compare missing %q in %v", key, diffs)
		}
	}
	if d := compare(want, want); len(d) != 0 {
		t.Errorf("compare(x, x) = %v, want none", d)
	}
}

func TestReadKnownDiffs(t *testing.T) {
	in := "# comment\n\nobject-basics/get header:Etag pail does not quote yet (#8)\n"
	known, err := readKnownDiffs(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if got := known["object-basics/get header:Etag"]; got != "pail does not quote yet (#8)" {
		t.Errorf("reason = %q, want the rest of the line", got)
	}
	for _, bad := range []string{"no-slash status reason\n", "a/b status\n"} {
		if _, err := readKnownDiffs(strings.NewReader(bad)); err == nil {
			t.Errorf("readKnownDiffs(%q) error = nil, want an error", bad)
		}
	}
}

func TestS3Escape(t *testing.T) {
	tests := []struct{ in, want string }{
		{"a/b.txt", "a/b.txt"},
		{"a b", "a%20b"},
		{"a+b", "a%2Bb"},
		{"✓", "%E2%9C%93"},
		{"a//b", "a//b"},
	}
	for _, tt := range tests {
		if got := s3Escape(tt.in); got != tt.want {
			t.Errorf("s3Escape(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeDropsUncomparedLengths(t *testing.T) {
	tests := []struct {
		name string
		r    response
	}{
		{"xml success", response{status: http.StatusOK, header: http.Header{"Content-Type": {"application/xml"}, "Content-Length": {"120"}}, body: []byte("<LocationConstraint/>")}},
		{"head error without body", response{status: http.StatusNotFound, header: http.Header{"Content-Length": {"243"}}}},
	}
	for _, tt := range tests {
		if got := normalize(step{name: "s", method: http.MethodHead}, "b", tt.r); got.Headers["Content-Length"] != "" {
			t.Errorf("%s: Content-Length = %q, want it dropped", tt.name, got.Headers["Content-Length"])
		}
	}
}

func TestNormalizeBinaryBody(t *testing.T) {
	r := response{status: http.StatusOK, header: http.Header{}, body: []byte{0xff, 0xfe, 'a'}}
	if got := normalize(step{name: "s", method: http.MethodGet}, "b", r); got.Body != "base64://5h" {
		t.Errorf("body = %q, want %q", got.Body, "base64://5h")
	}
}

func TestFingerprintCoversTheWholeRequest(t *testing.T) {
	base := step{method: http.MethodGet, key: "k", header: map[string]string{"Range": "bytes=0-4"}}
	variants := []step{
		{method: http.MethodGet, key: "k", header: map[string]string{"Range": "bytes=0-2"}},
		{method: http.MethodGet, key: "k", header: map[string]string{"Range": "bytes=0-4"}, body: "x"},
		{method: http.MethodGet, key: "k", header: map[string]string{"Range": "bytes=0-4"}, auth: authNone},
	}
	for _, v := range variants {
		if fingerprint(v) == fingerprint(base) {
			t.Errorf("fingerprint(%+v) equals fingerprint(%+v), want different", v, base)
		}
	}
}

func TestFailureNeverEchoesTheBody(t *testing.T) {
	r := response{status: http.StatusForbidden, body: []byte("<Error><Code>ExpiredToken</Code><Token-0>SECRET</Token-0></Error>")}
	got := failure(r, nil)
	if strings.Contains(got, "SECRET") || !strings.Contains(got, "ExpiredToken") {
		t.Errorf("failure() = %q, want the status and code without the token", got)
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv.Close)
	client := &http.Client{CheckRedirect: noRedirect}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("status = %d, want %d returned, not followed", resp.StatusCode, http.StatusTemporaryRedirect)
	}
}

// TestBadSignatureDoesNotPoisonTheSigner guards against the SDK signer's key
// cache: after a bad-signature step, a normal step must still verify.
func TestBadSignatureDoesNotPoisonTheSigner(t *testing.T) {
	tg := pailTarget(t)
	steps := []step{
		{name: "bad", method: http.MethodGet, query: "list-type=2", auth: authBadSignature},
		{name: "good", method: http.MethodGet, query: "list-type=2"},
	}
	var codes []string
	for _, st := range steps {
		resp, err := tg.do(t.Context(), st, newBucketName())
		if err != nil {
			t.Fatal(err)
		}
		code, _, _ := canonicalXML(string(resp.body))
		codes = append(codes, strings.TrimSpace(code))
	}
	if strings.Contains(codes[1], "SignatureDoesNotMatch") {
		t.Errorf("after a bad-signature step, a normal step got %q, want it to pass signing", codes[1])
	}
}

func TestReadPending(t *testing.T) {
	tests := []struct {
		in      string
		want    map[string]string
		wantErr bool
	}{
		{"# comment\n\nobject-basics/get needs GetObject (#8)\n", map[string]string{"object-basics/get": "needs GetObject (#8)"}, false},
		{"object-basics/get\n", nil, true},
		{"object-basics needs objects\n", nil, true},
		{"a/b/c reason\n", nil, true},
		{"object-basics/get status pasted from known-diffs\n", nil, true},
		{"object-basics/get header:Etag pasted from known-diffs\n", nil, true},
	}
	for _, tt := range tests {
		got, err := readPending(strings.NewReader(tt.in))
		if (err != nil) != tt.wantErr {
			t.Errorf("readPending(%q) error = %v, want error %v", tt.in, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && !maps.Equal(got, tt.want) {
			t.Errorf("readPending(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeObjectDataIsExact(t *testing.T) {
	for _, tt := range []struct {
		name, contentType, want, changed string
	}{
		{"XML content", "application/xml", `<ID permission="read">original</ID>`, `<ID permission="write">changed</ID>`},
		{"XML declaration", "text/plain", `<?xml version="1.0"?><ID>one</ID>`, `<?xml version="1.0"?><ID>two</ID>`},
		{"invalid XML", "application/xml", "<", "<<"},
		{"bucket name", "text/plain", "pail-test", "{bucket}"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := step{name: "get", method: http.MethodGet, key: "document"}
			responseFor := func(body string) response {
				return response{status: http.StatusOK, header: http.Header{"Content-Type": {tt.contentType}, "Content-Length": {"123"}}, body: []byte(body)}
			}
			want := normalize(st, "pail-test", responseFor(tt.want))
			got := normalize(st, "pail-test", responseFor(tt.changed))
			if want.Body != tt.want || want.Headers["Content-Length"] != "123" {
				t.Errorf("normalize(%q) = %+v, want exact body and Content-Length", tt.want, want)
			}
			if _, ok := compare(want, got)["get body"]; !ok {
				t.Errorf("compare(%q, %q) missed changed object data", tt.want, tt.changed)
			}
		})
	}
}

func TestNormalizeDotDotRequestIDs(t *testing.T) {
	for _, key := range []string{"../x", "a/../x", "ordinary"} {
		st := step{name: "get", method: http.MethodGet, key: key}
		without := response{status: http.StatusBadRequest, header: http.Header{}}
		with := response{status: http.StatusBadRequest, header: http.Header{"X-Amz-Request-Id": {"id"}, "X-Amz-Id-2": {"host"}}}
		got := len(compare(normalize(st, "bucket", without), normalize(st, "bucket", with)))
		want := 0
		if key == "ordinary" {
			want = 2
		}
		if got != want {
			t.Errorf("%q request ID differences = %d, want %d", key, got, want)
		}
	}
}

func TestCanonicalJSON(t *testing.T) {
	tests := []struct {
		name, in, want string
		wantErr        bool
	}{
		{"nested success", `{"Messages":[{"Body":"hello","ReceiptHandle":"abc","Attributes":{"SentTimestamp":"1","Z":2}}],"Attributes":{"CreatedTimestamp":"9","QueueArn":"arn","Ok":true,"None":null}}`,
			"Attributes.CreatedTimestamp: <volatile>\nAttributes.None: null\nAttributes.Ok: true\nAttributes.QueueArn: arn\n" +
				"Messages[0].Attributes.SentTimestamp: <volatile>\nMessages[0].Attributes.Z: 2\nMessages[0].Body: hello\nMessages[0].ReceiptHandle: <volatile>\n", false},
		{"batch failure drops its message and masks the sender", `{"Failed":[{"Id":"bad","SenderFault":true,"Code":"InvalidMessageContents","Message":"text"}],"Messages":[{"Attributes":{"SenderId":"AIDAEXAMPLE"}}]}`,
			"Failed[0].Code: InvalidMessageContents\nFailed[0].Id: bad\nFailed[0].SenderFault: true\nMessages[0].Attributes.SenderId: <volatile>\n", false},
		{"error keeps the type only", `{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"nope"}`, "__type: com.amazonaws.sqs#QueueDoesNotExist\n", true},
		{"empty body", "", "", false},
		{"empty object", "{}", "{}\n", false},
		{"empty array", `{"Messages":[]}`, "Messages: []\n", false},
		{"notification envelope", `{"Messages":[{"MD5OfBody":"abc","Body":"{\"Type\":\"Notification\",\"MessageId\":\"m\",\"TopicArn\":\"arn\",\"Subject\":\"s\",\"Message\":\"hello\",\"Timestamp\":\"t\",\"SignatureVersion\":\"1\",\"Signature\":\"sig\",\"SigningCertURL\":\"u\",\"UnsubscribeURL\":\"v\",\"MessageAttributes\":{\"color\":{\"Type\":\"String\",\"Value\":\"blue\"}}}"}]}`,
			"Messages[0].Body.Message: hello\nMessages[0].Body.MessageAttributes.color.Type: String\nMessages[0].Body.MessageAttributes.color.Value: blue\n" +
				"Messages[0].Body.MessageId: m\nMessages[0].Body.Signature: <volatile>\nMessages[0].Body.SignatureVersion: 1\nMessages[0].Body.SigningCertURL: <volatile>\n" +
				"Messages[0].Body.Subject: s\nMessages[0].Body.Timestamp: <volatile>\nMessages[0].Body.TopicArn: arn\nMessages[0].Body.Type: Notification\n" +
				"Messages[0].Body.UnsubscribeURL: <volatile>\nMessages[0].MD5OfBody: <volatile>\n", false},
		{"confirmation envelope", `{"Messages":[{"Body":"{\"Type\":\"SubscriptionConfirmation\",\"Token\":\"tok\",\"SubscribeURL\":\"u\",\"MessageId\":\"m\"}"}]}`,
			"Messages[0].Body.MessageId: m\nMessages[0].Body.SubscribeURL: <volatile>\nMessages[0].Body.Token: <volatile>\nMessages[0].Body.Type: SubscriptionConfirmation\n", false},
		{"other JSON bodies stay strings", `{"Messages":[{"Body":"{\"Type\":\"Other\",\"Message\":\"x\"}","MD5OfBody":"abc"},{"Body":"plain"}]}`,
			"Messages[0].Body: {\"Type\":\"Other\",\"Message\":\"x\"}\nMessages[0].MD5OfBody: abc\nMessages[1].Body: plain\n", false},
	}
	for _, tt := range tests {
		got, isErr, err := canonicalJSON(tt.in)
		if err != nil || got != tt.want || isErr != tt.wantErr {
			t.Errorf("%s: canonicalJSON(%q) = %q, %v, %v; want %q, %v", tt.name, tt.in, got, isErr, err, tt.want, tt.wantErr)
		}
	}
	if _, _, err := canonicalJSON("<xml/>"); err == nil {
		t.Error("canonicalJSON(XML) error = nil, want an error")
	}
}

func TestNormalizeSQS(t *testing.T) {
	const name = "pail-diff-1"
	r := response{
		status:   http.StatusOK,
		endpoint: "https://sqs.us-east-1.amazonaws.com",
		header: http.Header{
			"Content-Type":       {"application/x-amz-json-1.0"},
			"Content-Length":     {"99"},
			"X-Amzn-Requestid":   {"5f1b2c3d-0000-0000-0000-000000000000"},
			"X-Amzn-Query-Error": {"AWS.SimpleQueueService.NonExistentQueue;Sender"},
		},
		body: []byte(`{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/pail-diff-1","MessageId":"5f1b2c3d-1111-2222-3333-444455556666"}`),
	}
	got := normalize(step{name: "s", method: http.MethodPost, service: "sqs", target: "AmazonSQS.CreateQueue"}, name, r)
	wantBody := "MessageId: {uuid}\nQueueUrl: {endpoint}/{account}/{name}\n"
	if got.Body != wantBody {
		t.Errorf("body = %q, want %q", got.Body, wantBody)
	}
	want := map[string]string{
		"Content-Type":       "application/x-amz-json-1.0",
		"X-Amzn-Query-Error": "AWS.SimpleQueueService.NonExistentQueue;Sender",
		"X-Amzn-Requestid":   "<present>",
	}
	if !maps.Equal(got.Headers, want) {
		t.Errorf("headers = %v, want %v", got.Headers, want)
	}
}

func TestNormalizeSNSError(t *testing.T) {
	r := response{
		status: http.StatusNotFound,
		header: http.Header{"Content-Type": {"text/xml"}},
		body:   []byte(`<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><Error><Type>Sender</Type><Code>NotFound</Code><Message>Topic does not exist</Message></Error><RequestId>abc</RequestId></ErrorResponse>`),
	}
	got := normalize(step{name: "s", method: http.MethodPost, service: "sns", body: "Action=GetTopicAttributes&Version=2010-03-31"}, "pail-diff-1", r)
	if want := "ErrorResponse\n  Type: Sender\n  Code: NotFound\n"; got.Body != want {
		t.Errorf("body = %q, want %q", got.Body, want)
	}
}

func TestCaptureVars(t *testing.T) {
	vars := map[string]string{}
	bodies := []string{
		`<InitiateMultipartUploadResult><UploadId>up1</UploadId></InitiateMultipartUploadResult>`,
		`{"QueueUrl":"https://q/1"}`,
		`{"Attributes":{"QueueArn":"arn:q"}}`,
		`{"Messages":[{"MessageId":"m1","ReceiptHandle":"rh1"}]}`,
		`<CreateTopicResponse><CreateTopicResult><TopicArn>arn:t</TopicArn></CreateTopicResult></CreateTopicResponse>`,
		`<PublishResponse><PublishResult><MessageId>m2</MessageId></PublishResult></PublishResponse>`,
		`{"__type":"x"}`,
		``,
	}
	for _, b := range bodies {
		captureVars(vars, []byte(b))
	}
	want := map[string]string{
		"uploadId": "up1", "queueUrl": "https://q/1", "queueArn": "arn:q", "receiptHandle": "rh1",
		"messageId": "m2", "topicArn": "arn:t",
	}
	if !maps.Equal(vars, want) {
		t.Errorf("vars = %v, want %v", vars, want)
	}
}

func TestWithVars(t *testing.T) {
	vars := map[string]string{"name": "pail-diff-1", "topicArn": "arn:aws:sns:us-east-1:1:t", "queueUrl": `a"b`}
	tests := []struct {
		name string
		in   step
		want step
	}{
		{"sqs escapes JSON", step{service: "sqs", body: `{"QueueUrl":"{queueUrl}","N":"{name}"}`}, step{service: "sqs", body: `{"QueueUrl":"a\"b","N":"pail-diff-1"}`}},
		{"sns escapes the form", step{service: "sns", body: "TopicArn={topicArn}"}, step{service: "sns", body: "TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A1%3At"}},
		{"s3 expands raw", step{query: "a={topicArn}", header: map[string]string{"H": "{name}"}}, step{query: "a=arn:aws:sns:us-east-1:1:t", header: map[string]string{"H": "pail-diff-1"}}},
		{"uncaptured is empty", step{query: "uploadId={uploadId}"}, step{query: "uploadId="}},
	}
	for _, tt := range tests {
		got, _ := tt.in.withVars(vars)
		if got.query != tt.want.query || got.body != tt.want.body || !maps.Equal(got.header, tt.want.header) {
			t.Errorf("%s: withVars(%+v) = %+v, want %+v", tt.name, tt.in, got, tt.want)
		}
	}
}

func TestFingerprintService(t *testing.T) {
	// Computed before service and target existed: S3 steps keep their fingerprints.
	old := step{method: http.MethodGet, key: "k", header: map[string]string{"Range": "bytes=0-4"}}
	if got, want := fingerprint(old), "108ea647798fa61a"; got != want {
		t.Errorf("fingerprint(%+v) = %s, want %s", old, got, want)
	}
	for _, v := range []step{
		{method: old.method, key: old.key, header: old.header, service: "sqs"},
		{method: old.method, key: old.key, header: old.header, service: "sqs", target: "AmazonSQS.CreateQueue"},
	} {
		if fingerprint(v) == fingerprint(old) {
			t.Errorf("fingerprint(%+v) equals the S3 step's, want different", v)
		}
	}
}

func TestDescribeService(t *testing.T) {
	tests := []struct {
		st   step
		want string
	}{
		{step{method: http.MethodPost, service: "sqs", target: "AmazonSQS.CreateQueue"}, "POST sqs AmazonSQS.CreateQueue"},
		{step{method: http.MethodPost, service: "sns", body: "Action=CreateTopic&Name=x&Version=2010-03-31"}, "POST sns CreateTopic"},
	}
	for _, tt := range tests {
		if got := describe(tt.st); got != tt.want {
			t.Errorf("describe(%+v) = %q, want %q", tt.st, got, tt.want)
		}
	}
}

func TestWithVarsMissing(t *testing.T) {
	st := step{service: "sqs", body: `{"H":"{receiptHandle}","U":"{queueUrl}","N":"{name}"}`, header: map[string]string{"X": "{messageId}"}}
	_, got := st.withVars(map[string]string{"name": "n", "queueUrl": "u"})
	if want := []string{"receiptHandle", "messageId"}; !slices.Equal(got, want) {
		t.Errorf("missing = %v, want %v", got, want)
	}
}

func TestNormalizeServiceKeepsDigestsAndMasksAccounts(t *testing.T) {
	r := response{status: http.StatusOK, header: http.Header{}, body: []byte(`{"MD5OfMessageBody":"5d41402abc4b2a76b9719d911017c592","QueueUrl":"https://q/123456789012/x"}`)}
	got := normalize(step{name: "s", method: http.MethodPost, service: "sqs", target: "AmazonSQS.SendMessage"}, "n", r)
	want := "MD5OfMessageBody: 5d41402abc4b2a76b9719d911017c592\nQueueUrl: https://q/{account}/x\n"
	if got.Body != want {
		t.Errorf("body = %q, want %q", got.Body, want)
	}
}

func TestCanonicalXMLDropsFailedBatchMessages(t *testing.T) {
	const in = `<PublishBatchResponse><PublishBatchResult><Successful><member><Id>a</Id><MessageId>m</MessageId></member></Successful>` +
		`<Failed><member><Id>b</Id><Code>InvalidParameter</Code><Message>text</Message><SenderFault>true</SenderFault></member></Failed></PublishBatchResult></PublishBatchResponse>`
	want := "PublishBatchResponse\n  PublishBatchResult\n    Successful\n      member\n        Id: a\n        MessageId: m\n" +
		"    Failed\n      member\n        Id: b\n        Code: InvalidParameter\n        SenderFault: true\n"
	got, _, err := canonicalXML(in)
	if err != nil || got != want {
		t.Errorf("canonicalXML(%s) = %q, %v; want %q", in, got, err, want)
	}
}
