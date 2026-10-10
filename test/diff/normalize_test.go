package diff

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

// exchange is one step's normalized result, as stored in a golden file.
type exchange struct {
	Step    string `json:"step"`
	Request string `json:"request"`
	// Fingerprint covers everything a step sends, so any change to the
	// step makes its golden file stale.
	Fingerprint string            `json:"fingerprint"`
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"`
}

// golden is the recorded AWS behavior for one scenario.
type golden struct {
	Scenario  string     `json:"scenario"`
	Exchanges []exchange `json:"exchanges"`
}

// comparedHeaders are compared by value. Others are not compared, except
// presenceHeaders, whose values change on every run.
var comparedHeaders = []string{
	"Accept-Ranges", "Cache-Control", "Content-Disposition", "Content-Encoding",
	"Content-Language", "Content-Length", "Content-Range", "Content-Type", "Etag",
	"Expires", "Location", "X-Amz-Bucket-Region", "X-Amz-Checksum-Crc32",
	"X-Amz-Checksum-Crc32c", "X-Amz-Checksum-Crc64nvme", "X-Amz-Checksum-Sha1",
	"X-Amz-Checksum-Sha256", "X-Amz-Checksum-Type",
	"Access-Control-Allow-Origin", "Access-Control-Allow-Methods", "Access-Control-Allow-Headers",
	"Access-Control-Expose-Headers", "Access-Control-Allow-Credentials", "Access-Control-Max-Age",
	"Vary", "X-Amz-Transition-Default-Minimum-Object-Size", "X-Amzn-Query-Error",
}

var presenceHeaders = []string{"Last-Modified", "X-Amz-Id-2", "X-Amz-Request-Id", "X-Amzn-Requestid"}

// volatileElements change on every run, so only their presence is kept.
var volatileElements = map[string]bool{
	"CreationDate": true, "DisplayName": true, "ID": true, "Initiated": true,
	"LastModified": true, "Location": true, // Location is a URL on the server's own host
	"ContinuationToken": true, "NextContinuationToken": true,
	"NextUploadIdMarker": true, "UploadId": true, "UploadIdMarker": true,
}

// volatileJSONKeys change on every run; the last path segment decides.
var volatileJSONKeys = map[string]bool{
	"ReceiptHandle": true, "SequenceNumber": true, "NextToken": true, "SentTimestamp": true,
	"ApproximateFirstReceiveTimestamp": true, "CreatedTimestamp": true, "LastModifiedTimestamp": true,
	"SenderId": true, // the recording identity's IAM unique ID; never store it
}

// envelopeVolatileKeys change on every run in an SNS envelope that an SQS
// message carries as its Body. MessageId, TopicArn, and the other keys stay compared.
var envelopeVolatileKeys = map[string]bool{
	"Signature": true, "SigningCertURL": true, "Timestamp": true, "UnsubscribeURL": true, "SubscribeURL": true, "Token": true,
}

// droppedJSONKeys hold text that differs between servers, such as the Message of a
// failed batch entry. Error bodies keep only __type, so this applies to successes.
var droppedJSONKeys = map[string]bool{"Message": true}

// Non-S3 bodies hold run-specific values that normalizeService masks.
var (
	uuidPattern    = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	accountPattern = regexp.MustCompile(`\b[0-9]{12}\b`)
)

// droppedElements are per-request identifiers.
var droppedElements = map[string]bool{"HostId": true, "RequestId": true}

// normalize turns a raw response into a comparable exchange.
func normalize(st step, bucket string, r response) exchange {
	if st.service != "" {
		return normalizeService(st, bucket, r)
	}
	ex := exchange{Step: st.name, Request: describe(st), Fingerprint: fingerprint(st), Status: r.status, Headers: map[string]string{}}
	body := string(r.body)
	// A successful object read is opaque data, even with an XML content type.
	// GET with uploadId lists parts instead.
	q, _ := url.ParseQuery(st.query)
	objectData := st.key != "" && (st.method == http.MethodGet || st.method == http.MethodHead) &&
		(r.status == http.StatusOK || r.status == http.StatusPartialContent) && !q.Has("uploadId") && !q.Has("acl")
	if !objectData {
		body = strings.ReplaceAll(body, bucket, "{bucket}")
	}
	isXML := !objectData && looksXML(r.header.Get("Content-Type"), body)
	switch {
	case isXML:
		var err error
		if body, _, err = canonicalXML(body); err != nil {
			body = "unparsable XML: " + err.Error()
		}
	case !utf8.ValidString(body):
		// JSON would replace invalid UTF-8, so binary bodies are stored encoded.
		body = "base64:" + base64.StdEncoding.EncodeToString(r.body)
	}
	ex.Body = body
	for _, h := range comparedHeaders {
		if v := r.header.Get(h); v != "" {
			if h == "Location" && st.form != nil && r.status == http.StatusCreated {
				if location, err := url.Parse(v); err == nil {
					v = location.EscapedPath()
				}
			}
			ex.Headers[h] = strings.ReplaceAll(v, bucket, "{bucket}")
		}
	}
	for h := range r.header {
		if strings.HasPrefix(h, "X-Amz-Meta-") {
			ex.Headers[h] = r.header.Get(h)
		}
	}
	for _, h := range presenceHeaders {
		if r.header.Get(h) != "" {
			ex.Headers[h] = "<present>"
		}
	}
	if isXML || r.status >= 400 {
		// XML formatting and error Message text differ between servers, and
		// neither is compared, so their length is not either.
		delete(ex.Headers, "Content-Length")
	}
	// AWS front ends vary in whether bare dot-dot rejections carry request IDs.
	if r.status == http.StatusBadRequest && len(r.body) == 0 && slices.Contains(strings.Split(st.key, "/"), "..") {
		delete(ex.Headers, "X-Amz-Request-Id")
		delete(ex.Headers, "X-Amz-Id-2")
	}
	if len(ex.Headers) == 0 {
		ex.Headers = nil
	}
	return ex
}

// normalizeService normalizes an SQS or SNS response. The scenario name, the
// endpoint, UUIDs, and account IDs become placeholders.
func normalizeService(st step, name string, r response) exchange {
	ex := exchange{Step: st.name, Request: describe(st), Fingerprint: fingerprint(st), Status: r.status, Headers: map[string]string{}}
	var body string
	var err error
	if st.service == "sqs" {
		body, _, err = canonicalJSON(string(r.body))
		if err != nil {
			body = "unparsable JSON: " + err.Error()
		}
	} else {
		body, _, err = canonicalXML(string(r.body))
		if err != nil {
			body = "unparsable XML: " + err.Error()
		}
	}
	if r.endpoint != "" {
		body = strings.ReplaceAll(body, r.endpoint, "{endpoint}")
	}
	body = strings.ReplaceAll(body, name, "{name}")
	body = uuidPattern.ReplaceAllString(body, "{uuid}")
	ex.Body = accountPattern.ReplaceAllString(body, "{account}")
	for _, h := range comparedHeaders {
		// JSON and XML formatting differ between servers, so length is not compared.
		if v := r.header.Get(h); v != "" && h != "Content-Length" {
			ex.Headers[h] = strings.ReplaceAll(v, name, "{name}")
		}
	}
	for _, h := range presenceHeaders {
		if r.header.Get(h) != "" {
			ex.Headers[h] = "<present>"
		}
	}
	if len(ex.Headers) == 0 {
		ex.Headers = nil
	}
	return ex
}

// fingerprint hashes everything a step sends.
func fingerprint(st step) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\n%s\n%s\n%d\n", st.method, st.key, st.query, st.auth)
	for _, k := range slices.Sorted(maps.Keys(st.header)) {
		fmt.Fprintf(h, "%s: %s\n", k, st.header[k])
	}
	h.Write([]byte(st.body))
	if st.stream != "" { // only here, so older steps keep their fingerprints
		fmt.Fprintf(h, "\nstream %s %d %q %v", st.stream, st.chunk, st.trailer, st.badChunkSig)
	}
	if st.service != "" { // only here, so older steps keep their fingerprints
		fmt.Fprintf(h, "\nservice %s %q", st.service, st.target)
	}
	if st.form != nil {
		for _, key := range slices.Sorted(maps.Keys(st.form)) {
			fmt.Fprintf(h, "\nform %q=%q", key, st.form[key])
		}
		fmt.Fprintf(h, "\npostMutation %q", st.postMutation)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// describe is the request line a reader sees in a golden file.
func describe(st step) string {
	if st.service != "" {
		action := st.target
		if st.service == "sns" {
			form, _ := url.ParseQuery(st.body)
			action = form.Get("Action")
		}
		return st.method + " " + st.service + " " + action
	}
	s := st.method + " /" + st.key
	if st.query != "" {
		s += "?" + st.query
	}
	return s
}

func looksXML(contentType, body string) bool {
	return strings.Contains(contentType, "xml") || strings.HasPrefix(strings.TrimSpace(body), "<?xml")
}

type xmlNode struct {
	name     string
	text     string
	children []*xmlNode
}

// canonicalXML renders XML one element per line. For an S3 error it keeps
// the Code only: Message text and diagnostic fields differ between servers.
func canonicalXML(body string) (string, bool, error) {
	dec := xml.NewDecoder(strings.NewReader(body))
	var stack []*xmlNode
	var root *xmlNode
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", false, err
		}
		switch tok := tok.(type) {
		case xml.StartElement:
			n := &xmlNode{name: tok.Name.Local}
			if len(stack) > 0 {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, n)
			} else {
				root = n
			}
			stack = append(stack, n)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				stack[len(stack)-1].text += strings.TrimSpace(string(tok))
			}
		}
	}
	if root == nil {
		return "", false, nil
	}
	if root.name == "ErrorResponse" {
		var typ, code string
		for _, e := range root.children {
			if e.name != "Error" {
				continue
			}
			for _, c := range e.children {
				switch c.name {
				case "Type":
					typ = c.text
				case "Code":
					code = c.text
				}
			}
		}
		return "ErrorResponse\n  Type: " + typ + "\n  Code: " + code + "\n", true, nil
	}
	if root.name == "Error" {
		code := ""
		for _, c := range root.children {
			if c.name == "Code" {
				code = c.text
			}
		}
		return "Error\n  Code: " + code + "\n", true, nil
	}
	// AWS lists DeleteObjects results in no fixed order.
	if root.name == "DeleteResult" {
		slices.SortStableFunc(root.children, func(a, b *xmlNode) int { return strings.Compare(nodeText(a), nodeText(b)) })
	}
	dropFailedMessages(root)
	maskSubscriptionPrincipal(root)
	var b strings.Builder
	writeNode(&b, root, 0)
	return b.String(), false, nil
}

// maskSubscriptionPrincipal keeps only the presence of the SubscriptionPrincipal
// attribute: on AWS it is the ARN of the recording identity, which a golden file must not hold.
func maskSubscriptionPrincipal(n *xmlNode) {
	for _, c := range n.children {
		maskSubscriptionPrincipal(c)
	}
	if n.name != "entry" || !slices.ContainsFunc(n.children, func(c *xmlNode) bool { return c.name == "key" && c.text == "SubscriptionPrincipal" }) {
		return
	}
	for _, c := range n.children {
		if c.name == "value" {
			c.text = "<volatile>"
		}
	}
}

// dropFailedMessages removes the Message of every failed batch entry, as the
// SQS JSON normalization does: its text differs between servers.
func dropFailedMessages(n *xmlNode) {
	for _, c := range n.children {
		dropFailedMessages(c)
		if c.name != "Failed" {
			continue
		}
		for _, member := range c.children {
			member.children = slices.DeleteFunc(member.children, func(e *xmlNode) bool { return e.name == "Message" })
		}
	}
}

// canonicalJSON renders a JSON body as one "path: value" line per leaf. For an
// error it keeps the __type only: message text differs between servers.
func canonicalJSON(body string) (string, bool, error) {
	if strings.TrimSpace(body) == "" {
		return "", false, nil
	}
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", false, err
	}
	if obj, ok := v.(map[string]any); ok {
		if typ, ok := obj["__type"]; ok {
			return fmt.Sprintf("__type: %v\n", typ), true, nil
		}
	}
	var b strings.Builder
	writeJSON(&b, "", "", v)
	if b.Len() == 0 {
		return "{}\n", false, nil // distinct from an empty body
	}
	return b.String(), false, nil
}

// writeJSON writes the leaves under v. key is v's own key, for volatile keys.
func writeJSON(b *strings.Builder, path, key string, v any) {
	switch v := v.(type) {
	case map[string]any:
		if len(v) == 0 && path != "" {
			fmt.Fprintf(b, "%s: {}\n", path)
		}
		for _, k := range slices.Sorted(maps.Keys(v)) {
			if droppedJSONKeys[k] {
				continue
			}
			p := k
			if path != "" {
				p = path + "." + k
			}
			// An envelope body carries a timestamp and a signature, so its checksum changes too.
			if body, _ := v["Body"].(string); k == "MD5OfBody" && body != "" {
				if _, ok := snsEnvelope(body); ok {
					fmt.Fprintf(b, "%s: <volatile>\n", p)
					continue
				}
			}
			writeJSON(b, p, k, v[k])
		}
	case []any:
		if len(v) == 0 {
			fmt.Fprintf(b, "%s: []\n", path)
		}
		for i, e := range v {
			writeJSON(b, fmt.Sprintf("%s[%d]", path, i), "", e)
		}
	case nil:
		fmt.Fprintf(b, "%s: null\n", path)
	default:
		if s, ok := v.(string); ok {
			if env, ok := snsEnvelope(s); ok {
				writeEnvelope(b, path, env)
				return
			}
		}
		if volatileJSONKeys[key] {
			v = "<volatile>"
		}
		fmt.Fprintf(b, "%s: %v\n", path, v)
	}
}

// snsEnvelope reports whether s is the JSON envelope of an SNS notification or
// subscription confirmation, which SNS delivers as an SQS message body.
func snsEnvelope(s string) (map[string]any, bool) {
	if !strings.HasPrefix(s, "{") {
		return nil, false
	}
	var env map[string]any
	if json.Unmarshal([]byte(s), &env) != nil {
		return nil, false
	}
	typ, _ := env["Type"].(string)
	return env, typ == "Notification" || typ == "SubscriptionConfirmation"
}

// writeEnvelope writes an envelope as a nested object under path, so a golden
// file shows Body.Message, and masks the keys that change on every run. Message
// stays compared, unlike the Message of a failed batch entry.
func writeEnvelope(b *strings.Builder, path string, env map[string]any) {
	for _, k := range slices.Sorted(maps.Keys(env)) {
		p := path + "." + k
		switch v := env[k].(type) {
		case map[string]any:
			writeJSON(b, p, k, v)
		default:
			if envelopeVolatileKeys[k] {
				v = "<volatile>"
			}
			fmt.Fprintf(b, "%s: %v\n", p, v)
		}
	}
}

func nodeText(n *xmlNode) string {
	var b strings.Builder
	writeNode(&b, n, 0)
	return b.String()
}

func writeNode(b *strings.Builder, n *xmlNode, depth int) {
	if droppedElements[n.name] {
		return
	}
	indent := strings.Repeat("  ", depth)
	switch {
	case volatileElements[n.name]:
		fmt.Fprintf(b, "%s%s: <volatile>\n", indent, n.name)
	case len(n.children) == 0:
		fmt.Fprintf(b, "%s%s: %s\n", indent, n.name, n.text)
	default:
		fmt.Fprintf(b, "%s%s\n", indent, n.name)
		for _, c := range n.children {
			writeNode(b, c, depth+1)
		}
	}
}

// compare returns one line per differing field, keyed "<step> <field>".
func compare(want, got exchange) map[string]string {
	diffs := map[string]string{}
	if want.Status != got.Status {
		diffs[want.Step+" status"] = fmt.Sprintf("status: want %d, got %d", want.Status, got.Status)
	}
	names := slices.Sorted(maps.Keys(want.Headers))
	for h := range got.Headers {
		if _, ok := want.Headers[h]; !ok {
			names = append(names, h)
		}
	}
	for _, h := range names {
		if want.Headers[h] != got.Headers[h] {
			diffs[want.Step+" header:"+h] = fmt.Sprintf("header %s: want %q, got %q", h, want.Headers[h], got.Headers[h])
		}
	}
	if want.Body != got.Body {
		diffs[want.Step+" body"] = fmt.Sprintf("body:\n--- want\n%s--- got\n%s", want.Body, got.Body)
	}
	return diffs
}

func readGolden(path string) (golden, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return golden{}, err
	}
	var g golden
	if err := json.Unmarshal(b, &g); err != nil {
		return golden{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return g, nil
}

func writeGolden(path string, g golden) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(g); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// readKnownDiffs parses "<scenario>/<step> <field> <reason>" lines. Blank
// lines and lines starting with "#" are skipped. Every entry needs a reason.
func readKnownDiffs(r io.Reader) (map[string]string, error) {
	known := map[string]string{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 || !strings.Contains(fields[0], "/") {
			return nil, fmt.Errorf("line %d: want \"<scenario>/<step> <field> <reason>\", got %q", n, line)
		}
		known[fields[0]+" "+fields[1]] = strings.Join(fields[2:], " ")
	}
	return known, sc.Err()
}

// readPending parses "<scenario>/<step> <reason>" lines: steps recorded before
// pail implements them. Every entry needs a reason.
func readPending(r io.Reader) (map[string]string, error) {
	pending := map[string]string{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		// A known-diffs line pasted here would make its whole step pending.
		if len(fields) < 2 || strings.Count(fields[0], "/") != 1 || isDiffField(fields[1]) {
			return nil, fmt.Errorf("line %d: want \"<scenario>/<step> <reason>\", got %q", n, line)
		}
		pending[fields[0]] = strings.Join(fields[1:], " ")
	}
	return pending, sc.Err()
}

// isDiffField reports whether s names a compared field, as known-diffs lines do.
func isDiffField(s string) bool {
	return s == "status" || s == "body" || strings.HasPrefix(s, "header:")
}
