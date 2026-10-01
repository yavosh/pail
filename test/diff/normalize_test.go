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
	"os"
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
}

var presenceHeaders = []string{"Last-Modified", "X-Amz-Id-2", "X-Amz-Request-Id"}

// volatileElements change on every run, so only their presence is kept.
var volatileElements = map[string]bool{
	"CreationDate": true, "DisplayName": true, "ID": true, "Initiated": true,
	"LastModified": true, "ContinuationToken": true, "NextContinuationToken": true,
	"NextUploadIdMarker": true, "UploadId": true, "UploadIdMarker": true,
}

// droppedElements are per-request identifiers.
var droppedElements = map[string]bool{"HostId": true, "RequestId": true}

// normalize turns a raw response into a comparable exchange.
func normalize(st step, bucket string, r response) exchange {
	ex := exchange{Step: st.name, Request: describe(st), Fingerprint: fingerprint(st), Status: r.status, Headers: map[string]string{}}
	body := strings.ReplaceAll(string(r.body), bucket, "{bucket}")
	isXML := looksXML(r.header.Get("Content-Type"), body)
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
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// describe is the request line a reader sees in a golden file.
func describe(st step) string {
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
	if root.name == "Error" {
		code := ""
		for _, c := range root.children {
			if c.name == "Code" {
				code = c.text
			}
		}
		return "Error\n  Code: " + code + "\n", true, nil
	}
	var b strings.Builder
	writeNode(&b, root, 0)
	return b.String(), false, nil
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
