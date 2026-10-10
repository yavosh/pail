package diff

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// authMode picks how a step signs its request.
type authMode int

const (
	authSigned authMode = iota
	authNone
	authUnknownKey
	authBadSignature
	authSkewed
	// The presigned modes stay last: do treats every mode from authPresigned on as presigned.
	authPresigned              // a query-string signature, valid for 15 minutes unless the step sets X-Amz-Expires
	authPresignedExpired       // signed two hours ago, valid for one hour
	authPresignedTampered      // a valid signature, then a query parameter it does not cover
	authPresignedFuture        // signed one hour from now
	authPresignedBadCredential // credential scope names the ec2 service
	authPresignedMissingParam  // X-Amz-Date removed after signing
	authPresignedV2
	authPresignedV2Expired
	authPresignedV2Tampered
	authPresignedV2BadSignature
	authPresignedV2MissingParam
)

// target is one S3 implementation the suite talks to over HTTP.
type target struct {
	region string
	creds  aws.Credentials
	client *http.Client
	// host returns the virtual-hosted-style host for a bucket.
	host func(bucket string) string
	// serviceHost returns the host of a non-S3 service.
	serviceHost func(service string) string
	scheme      string
	// vars holds placeholders whose value differs by target, such as an HTTP
	// endpoint. Responses mask each value as its placeholder.
	vars map[string]string
}

// startVars returns the variables a scenario starts with.
func (tg *target) startVars(bucket string) map[string]string {
	vars := map[string]string{"name": bucket}
	maps.Copy(vars, tg.vars)
	return vars
}

// normalize normalizes r and masks the values of the target's own variables.
func (tg *target) normalize(st step, bucket string, r response) exchange {
	ex := normalize(st, bucket, r)
	for name, v := range tg.vars {
		ex.Body = strings.ReplaceAll(ex.Body, v, "{"+name+"}")
	}
	return ex
}

// response is one raw HTTP answer.
type response struct {
	status   int
	header   http.Header
	body     []byte
	endpoint string // scheme://host a non-S3 step was sent to, for normalization
}

// initiatedUploadID returns the upload ID in a CreateMultipartUpload response, or "".
func initiatedUploadID(body []byte) string {
	var r struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	if xml.Unmarshal(body, &r) != nil {
		return ""
	}
	return r.UploadID
}

// variables are the placeholders a step can use, besides "{name}".
var variables = []string{"uploadId", "queueUrl", "queueArn", "receiptHandle", "messageId", "topicArn", "subscriptionArn", "httpEndpoint"}

// captureVars updates vars from one response. The latest value wins, and a
// response without a value keeps the earlier one.
func captureVars(vars map[string]string, body []byte) {
	if id := initiatedUploadID(body); id != "" {
		vars["uploadId"] = id
	}
	var j struct {
		QueueURL   string `json:"QueueUrl"`
		MessageID  string `json:"MessageId"`
		Attributes struct {
			QueueArn string
		}
		Messages []struct {
			ReceiptHandle string
			MessageID     string `json:"MessageId"`
		}
	}
	if json.Unmarshal(body, &j) == nil {
		set := func(name, v string) {
			if v != "" {
				vars[name] = v
			}
		}
		set("queueUrl", j.QueueURL)
		set("queueArn", j.Attributes.QueueArn)
		set("messageId", j.MessageID)
		if len(j.Messages) > 0 {
			set("receiptHandle", j.Messages[0].ReceiptHandle)
			set("messageId", j.Messages[0].MessageID)
		}
		return
	}
	// Any element of these names counts, whatever its parent.
	elements := map[string]string{"TopicArn": "topicArn", "SubscriptionArn": "subscriptionArn", "MessageId": "messageId"}
	dec := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			return
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		name := elements[start.Name.Local]
		if name == "" {
			continue
		}
		var text string
		// A pending subscription lists "PendingConfirmation" in place of its ARN.
		if dec.DecodeElement(&text, &start) == nil && text != "" && (name != "subscriptionArn" || strings.HasPrefix(text, "arn:")) {
			vars[name] = text
		}
	}
}

// serviceSigner signs non-S3 requests: the SigV4 rule is to encode paths twice.
var serviceSigner = v4.NewSigner()

// signer signs like the S3 SDKs: S3 paths are encoded once, never twice.
var signer = newSigner()

func newSigner() *v4.Signer {
	return v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
}

// do sends one step to the target, addressed virtual-hosted-style at bucket.
func (tg *target) do(ctx context.Context, st step, bucket string) (response, error) {
	if st.service != "" {
		return tg.doService(ctx, st)
	}
	expand := func(s string) string { return strings.ReplaceAll(s, "{bucket}", bucket) }
	u := &url.URL{
		Scheme:   tg.scheme,
		Host:     tg.host(bucket),
		Path:     "/" + st.key,
		RawPath:  "/" + s3Escape(st.key),
		RawQuery: expand(st.query),
	}
	req, err := http.NewRequestWithContext(ctx, st.method, u.String(), bytes.NewReader([]byte(st.body)))
	if err != nil {
		return response{}, err
	}
	for k, v := range st.header {
		req.Header.Set(k, expand(v))
	}
	if st.form != nil {
		if err := tg.encodePost(req, st, bucket); err != nil {
			return response{}, err
		}
	}
	sum := sha256.Sum256([]byte(st.body))
	payloadHash := hex.EncodeToString(sum[:])
	if st.stream != "" {
		payloadHash = st.stream
		setStreamHeaders(req, st)
	}
	// A presigned request has no x-amz-content-sha256: it would be an unsigned x-amz header.
	presigned := st.auth >= authPresigned
	if !presigned && st.form == nil {
		req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}

	creds, signTime := tg.creds, time.Now()
	if st.form == nil {
		switch st.auth {
		case authPresigned, authPresignedExpired, authPresignedTampered, authPresignedFuture, authPresignedBadCredential, authPresignedMissingParam:
			err = tg.presign(ctx, req, st.auth, signTime)
		case authPresignedV2, authPresignedV2Expired, authPresignedV2Tampered, authPresignedV2BadSignature, authPresignedV2MissingParam:
			tg.presignV2(req, bucket, st.auth, signTime)
		default:
			creds, err = tg.signHeader(ctx, st.auth, req, payloadHash, "s3", signer, newSigner, signTime)
		}
	}
	if err != nil {
		return response{}, fmt.Errorf("sign: %w", err)
	}
	if st.stream != "" {
		amzDate := req.Header.Get("X-Amz-Date")
		scope := amzDate[:8] + "/" + tg.region + "/s3/aws4_request"
		_, seed, _ := strings.Cut(req.Header.Get("Authorization"), "Signature=")
		enc := encodeChunked(st, signingKey(creds.SecretAccessKey, amzDate[:8], tg.region), amzDate, scope, seed)
		req.Body, req.GetBody = io.NopCloser(bytes.NewReader(enc)), nil
	}

	resp, err := tg.client.Do(req)
	if err != nil {
		// A presigned query is a credential, and can hold a session token:
		// keep it out of test logs.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return response{}, fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

// signHeader signs req with an Authorization header, as the auth mode says,
// and returns the credentials it used. fresh makes a signer for a wrong secret:
// the shared signer caches keys by access key, so it would break later requests.
func (tg *target) signHeader(ctx context.Context, auth authMode, req *http.Request, payloadHash, service string, shared *v4.Signer, fresh func() *v4.Signer, signTime time.Time) (aws.Credentials, error) {
	creds, sg := tg.creds, shared
	switch auth {
	case authNone:
		return creds, nil
	case authUnknownKey:
		creds = aws.Credentials{AccessKeyID: "AKIAPAILDIFFUNKNOWN0", SecretAccessKey: "unknown"}
	case authBadSignature:
		creds.SecretAccessKey += "-wrong"
		sg = fresh()
	case authSkewed:
		signTime = signTime.Add(-20 * time.Minute)
	}
	return creds, sg.SignHTTP(ctx, creds, req, payloadHash, service, tg.region, signTime)
}

// doService sends one SQS or SNS step. These services take no
// x-amz-content-sha256 and no bucket.
func (tg *target) doService(ctx context.Context, st step) (response, error) {
	switch {
	case st.auth >= authPresigned:
		return response{}, fmt.Errorf("%s step: presigned auth mode %d is S3-only", st.service, st.auth)
	case st.form != nil:
		return response{}, fmt.Errorf("%s step: a form is S3-only", st.service)
	case st.stream != "":
		return response{}, fmt.Errorf("%s step: a stream is S3-only", st.service)
	}
	endpoint := tg.scheme + "://" + tg.serviceHost(st.service)
	u := &url.URL{Scheme: tg.scheme, Host: tg.serviceHost(st.service), Path: "/", RawQuery: st.query}
	req, err := http.NewRequestWithContext(ctx, st.method, u.String(), strings.NewReader(st.body))
	if err != nil {
		return response{}, err
	}
	if st.service == "sqs" {
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", st.target)
	} else {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	}
	for k, v := range st.header {
		req.Header.Set(k, v)
	}
	payloadHash := sha256Hex(st.body)
	_, err = tg.signHeader(ctx, st.auth, req, payloadHash, st.service, serviceSigner, func() *v4.Signer { return v4.NewSigner() }, time.Now())
	if err != nil {
		return response{}, fmt.Errorf("sign: %w", err)
	}
	resp, err := tg.client.Do(req)
	if err != nil {
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return response{}, fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{status: resp.StatusCode, header: resp.Header, body: body, endpoint: endpoint}, nil
}

// presign replaces req.URL with a presigned URL. The payload is unsigned.
func (tg *target) presign(ctx context.Context, req *http.Request, auth authMode, signTime time.Time) error {
	expires := "900"
	switch auth {
	case authPresignedExpired:
		signTime, expires = signTime.Add(-2*time.Hour), "3600"
	case authPresignedFuture:
		signTime = signTime.Add(time.Hour)
	}
	q := req.URL.Query()
	if !q.Has("X-Amz-Expires") {
		q.Set("X-Amz-Expires", expires)
	}
	req.URL.RawQuery = q.Encode()
	signedURL, _, err := signer.PresignHTTP(ctx, tg.creds, req, "UNSIGNED-PAYLOAD", "s3", tg.region, signTime)
	if err != nil {
		return err
	}
	req.URL, err = url.Parse(signedURL)
	if err != nil {
		return err
	}
	switch auth {
	case authPresignedTampered:
		req.URL.RawQuery += "&x-id=Tampered"
	case authPresignedBadCredential:
		q := req.URL.Query()
		q.Set("X-Amz-Credential", strings.Replace(q.Get("X-Amz-Credential"), "/s3/aws4_request", "/ec2/aws4_request", 1))
		req.URL.RawQuery = q.Encode()
	case authPresignedMissingParam:
		q := req.URL.Query()
		q.Del("X-Amz-Date")
		req.URL.RawQuery = q.Encode()
	}
	return nil
}

const (
	streamSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	streamSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// setStreamHeaders adds the aws-chunked headers a step does not set itself;
// an empty value in the step removes one. Signatures have a fixed length, so
// a dummy encoding gives the Content-Length to sign.
func setStreamHeaders(req *http.Request, st step) {
	defaults := map[string]string{
		"Content-Encoding":             "aws-chunked",
		"X-Amz-Decoded-Content-Length": strconv.Itoa(len(st.body)),
	}
	if name, _, ok := strings.Cut(st.trailer, ":"); ok {
		defaults["X-Amz-Trailer"] = name
	}
	for k, v := range defaults {
		if _, set := st.header[k]; !set {
			req.Header.Set(k, v)
		}
		if req.Header.Get(k) == "" {
			req.Header.Del(k)
		}
	}
	req.ContentLength = int64(len(encodeChunked(st, nil, "", "", "")))
}

// encodeChunked aws-chunk encodes st.body, chaining chunk signatures from
// seed. The suite signs chunks itself, so recording checks this code against
// AWS, independently of pail's verifier.
func encodeChunked(st step, key []byte, amzDate, scope, seed string) []byte {
	prev := seed
	sign := func(stringToSign string) string {
		prev = hex.EncodeToString(hmacSHA256(key, stringToSign))
		return prev
	}
	size := cmp.Or(st.chunk, 8<<10)
	var b bytes.Buffer
	for rest := st.body; ; {
		data := rest[:min(size, len(rest))]
		rest = rest[len(data):]
		if st.stream == streamUnsignedTrailer {
			fmt.Fprintf(&b, "%x\r\n", len(data))
		} else {
			sig := sign("AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" + prev + "\n" + sha256Hex("") + "\n" + sha256Hex(data))
			if st.badChunkSig && b.Len() == 0 {
				sig = strings.Repeat("0", len(sig))
			}
			fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(data), sig)
		}
		if data == "" {
			break
		}
		b.WriteString(data + "\r\n")
	}
	if st.trailer != "" {
		b.WriteString(st.trailer + "\r\n")
		if st.stream == streamSignedTrailer {
			sig := sign("AWS4-HMAC-SHA256-TRAILER\n" + amzDate + "\n" + scope + "\n" + prev + "\n" + sha256Hex(st.trailer+"\n"))
			b.WriteString("x-amz-trailer-signature:" + sig + "\r\n")
		}
	}
	b.WriteString("\r\n")
	return b.Bytes()
}

func signingKey(secret, date, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	for _, s := range []string{region, "s3", "aws4_request"} {
		k = hmacSHA256(k, s)
	}
	return k
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// s3Escape percent-encodes a key the way S3 clients do: every byte except
// unreserved characters and "/".
func s3Escape(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || strings.IndexByte("-._~/", c) >= 0 {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// cleanupBucket deletes every object, upload, and finally the bucket. It logs
// instead of failing, and never logs a raw error body: AWS can echo the token.
func cleanupBucket(t *testing.T, tg *target, bucket string) {
	t.Helper()
	if !strings.HasPrefix(bucket, "pail-diff-") {
		t.Fatalf("cleanup refuses bucket %q: not created by this suite", bucket)
	}
	ctx := context.Background()
list:
	for range 100 {
		resp, err := tg.do(ctx, step{method: http.MethodGet, query: "list-type=2"}, bucket)
		if err != nil || resp.status != http.StatusOK {
			break
		}
		var list struct {
			Contents []struct{ Key string }
		}
		if err := xml.Unmarshal(resp.body, &list); err != nil || len(list.Contents) == 0 {
			break
		}
		for _, c := range list.Contents {
			resp, err := tg.do(ctx, step{method: http.MethodDelete, key: c.Key}, bucket)
			if err != nil || resp.status != http.StatusNoContent {
				t.Logf("cleanup: delete %q: %s", c.Key, failure(resp, err))
				break list
			}
		}
	}
	if resp, err := tg.do(ctx, step{method: http.MethodGet, query: "uploads"}, bucket); err == nil && resp.status == http.StatusOK {
		var uploads struct {
			Upload []struct {
				Key      string
				UploadID string `xml:"UploadId"`
			}
		}
		if xml.Unmarshal(resp.body, &uploads) == nil {
			for _, u := range uploads.Upload {
				_, _ = tg.do(ctx, step{method: http.MethodDelete, key: u.Key, query: "uploadId=" + url.QueryEscape(u.UploadID)}, bucket)
			}
		}
	}
	resp, err := tg.do(ctx, step{method: http.MethodDelete}, bucket)
	if err != nil || resp.status != http.StatusNoContent && resp.status != http.StatusNotFound {
		t.Logf("cleanup: delete bucket %s: %s", bucket, failure(resp, err))
	}
}

// cleanupQueue deletes an SQS queue if it exists. It logs instead of failing.
func cleanupQueue(t *testing.T, tg *target, name string) {
	t.Helper()
	if !strings.HasPrefix(name, "pail-diff-") {
		t.Fatalf("cleanup refuses queue %q: not created by this suite", name)
	}
	ctx := context.Background()
	sqs := func(op, body string) (response, error) {
		return tg.do(ctx, step{method: http.MethodPost, service: "sqs", target: "AmazonSQS." + op, body: body}, "")
	}
	resp, err := sqs("GetQueueUrl", `{"QueueName":"`+name+`"}`)
	if err != nil || resp.status != http.StatusOK {
		return
	}
	var q struct {
		QueueURL string `json:"QueueUrl"`
	}
	if json.Unmarshal(resp.body, &q) != nil || q.QueueURL == "" {
		return
	}
	body, err := json.Marshal(map[string]string{"QueueUrl": q.QueueURL})
	if err != nil {
		return
	}
	if resp, err := sqs("DeleteQueue", string(body)); err != nil || resp.status != http.StatusOK {
		t.Logf("cleanup: delete queue %s: %s", name, failure(resp, err))
	}
}

// cleanupTopic deletes an SNS topic by its captured ARN. Without one, it falls
// back to CreateTopic, which is idempotent and returns the ARN, so scenarios
// must create topics without attributes or tags. It logs instead of failing.
func cleanupTopic(t *testing.T, tg *target, name, capturedARN string) {
	t.Helper()
	if !strings.HasPrefix(name, "pail-diff-") {
		t.Fatalf("cleanup refuses topic %q: not created by this suite", name)
	}
	ctx := context.Background()
	sns := func(form string) (response, error) {
		return tg.do(ctx, step{method: http.MethodPost, service: "sns", body: form + "&Version=2010-03-31"}, "")
	}
	arn := capturedARN
	if !strings.HasSuffix(arn, ":"+name) {
		resp, err := sns("Action=CreateTopic&Name=" + url.QueryEscape(name))
		if err != nil || resp.status != http.StatusOK {
			t.Logf("cleanup: create topic %s: %s", name, failure(resp, err))
			return
		}
		vars := map[string]string{}
		captureVars(vars, resp.body)
		if arn = vars["topicArn"]; arn == "" {
			return
		}
	}
	resp, err := sns("Action=DeleteTopic&TopicArn=" + url.QueryEscape(arn))
	if err != nil || resp.status != http.StatusOK && resp.status != http.StatusNotFound {
		t.Logf("cleanup: delete topic %s: %s", name, failure(resp, err))
	}
}

// failure describes a failed cleanup call by status and error code only.
func failure(resp response, err error) string {
	if err != nil {
		return err.Error()
	}
	s := fmt.Sprintf("status %d", resp.status)
	if body, isErr, _ := canonicalXML(string(resp.body)); isErr {
		s += " " + strings.Join(strings.Fields(body), " ")
	}
	if body, isErr, _ := canonicalJSON(string(resp.body)); isErr {
		s += " " + strings.TrimSpace(body)
	}
	return s
}

func (tg *target) presignV2(req *http.Request, bucket string, auth authMode, now time.Time) {
	expires := now.Add(15 * time.Minute)
	if auth == authPresignedV2Expired {
		expires = now.Add(-time.Hour)
	}
	if tg.creds.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", tg.creds.SessionToken)
	}
	expiration := strconv.FormatInt(expires.Unix(), 10)
	stringToSign := req.Method + "\n" + req.Header.Get("Content-MD5") + "\n" + req.Header.Get("Content-Type") + "\n" + expiration + "\n"
	for _, name := range slices.Sorted(maps.Keys(req.Header)) {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") {
			stringToSign += strings.ToLower(name) + ":" + strings.TrimSpace(req.Header.Get(name)) + "\n"
		}
	}
	resource := "/" + bucket + req.URL.EscapedPath()
	q := req.URL.Query()
	var parameters []string
	for _, name := range []string{"acl", "partNumber", "response-content-disposition", "response-content-type", "uploadId", "versionId"} {
		if !q.Has(name) {
			continue
		}
		parameter := name
		if q.Get(name) != "" {
			parameter += "=" + q.Get(name)
		}
		parameters = append(parameters, parameter)
	}
	if len(parameters) > 0 {
		resource += "?" + strings.Join(parameters, "&")
	}
	mac := hmac.New(sha1.New, []byte(tg.creds.SecretAccessKey))
	_, _ = mac.Write([]byte(stringToSign + resource))
	q.Set("AWSAccessKeyId", tg.creds.AccessKeyID)
	q.Set("Expires", expiration)
	q.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	switch auth {
	case authPresignedV2Tampered:
		q.Set("response-content-type", "text/html")
	case authPresignedV2BadSignature:
		q.Set("Signature", base64.StdEncoding.EncodeToString(make([]byte, 20)))
	case authPresignedV2MissingParam:
		q.Del("Expires")
	}
	req.URL.RawQuery = q.Encode()
}

func (tg *target) encodePost(req *http.Request, st step, bucket string) error {
	now := time.Now().UTC()
	expiration := now.Add(time.Hour)
	if st.postMutation == "expired" {
		expiration = now.Add(-time.Hour)
	}
	fields := maps.Clone(st.form)
	fields["x-amz-algorithm"] = "AWS4-HMAC-SHA256"
	fields["x-amz-credential"] = tg.creds.AccessKeyID + "/" + now.Format("20060102") + "/" + tg.region + "/s3/aws4_request"
	fields["x-amz-date"] = now.Format("20060102T150405Z")
	if tg.creds.SessionToken != "" {
		fields["x-amz-security-token"] = tg.creds.SessionToken
	}
	conditions := []any{map[string]string{"bucket": bucket}, []any{"content-length-range", 1, 32}}
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		conditions = append(conditions, map[string]string{key: fields[key]})
	}
	policy, err := json.Marshal(map[string]any{"expiration": expiration.Format(time.RFC3339), "conditions": conditions})
	if err != nil {
		return err
	}
	fields["policy"] = base64.StdEncoding.EncodeToString(policy)
	fields["x-amz-signature"] = hex.EncodeToString(hmacSHA256(signingKey(tg.creds.SecretAccessKey, now.Format("20060102"), tg.region), fields["policy"]))
	switch st.postMutation {
	case "signature":
		fields["x-amz-signature"] = strings.Repeat("0", 64)
	case "key":
		fields["key"] = "forbidden"
	case "uncovered":
		fields["x-amz-meta-uncovered"] = "extra"
	}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		if err := mw.WriteField(key, fields[key]); err != nil {
			return err
		}
	}
	part, err := mw.CreateFormFile("file", "form.txt")
	if err != nil {
		return err
	}
	if _, err := io.WriteString(part, st.body); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))
	req.ContentLength = int64(body.Len())
	return nil
}
