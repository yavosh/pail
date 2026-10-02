package sigv4

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// maxChunkLine bounds a chunk header line and a trailer line.
const maxChunkLine = 4096

// minChunkSize is the smallest signed chunk AWS accepts, except the last one.
const minChunkSize = 8 << 10

const (
	chunkSignaturePrefix = "chunk-signature="
	trailerSignature     = "x-amz-trailer-signature"
	emptySHA256          = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// chunkParams describes one aws-chunked request body.
type chunkParams struct {
	signed     bool   // each chunk carries a signature chained from seed
	trailers   bool   // trailing headers follow the final chunk
	key        []byte // signing key
	amzDate    string
	scope      string
	seed       string // the request signature, hex
	decodedLen int64
	declared   []string    // lowercase trailer names from x-amz-trailer
	trailer    http.Header // filled once the body reaches EOF
}

// chunkedReader decodes an aws-chunked body and verifies it as it reads. A
// chunk's data may reach the caller before its signature is checked, so the
// caller must not commit before EOF.
type chunkedReader struct {
	body io.Closer
	br   *bufio.Reader
	chunkParams
	prev      string // signature of the previous chunk, hex
	chunkSig  string
	chunkHash hash.Hash
	remaining int64 // data bytes left in the current chunk
	short     bool  // the previous data chunk was under minChunkSize
	decoded   int64
	seen      map[string]bool // declared trailer name -> received
	err       error           // sticky
}

func newChunkedReader(body io.ReadCloser, p chunkParams) *chunkedReader {
	seen := make(map[string]bool, len(p.declared))
	for _, name := range p.declared {
		seen[name] = false
	}
	return &chunkedReader{
		body:        body,
		br:          bufio.NewReaderSize(body, maxChunkLine),
		chunkParams: p,
		prev:        p.seed,
		chunkHash:   sha256.New(),
		seen:        seen,
	}
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	n, err := c.read(p)
	c.err = err
	return n, err
}

func (c *chunkedReader) Close() error { return c.body.Close() }

func (c *chunkedReader) read(p []byte) (int, error) {
	for c.remaining == 0 {
		if err := c.nextChunk(); err != nil {
			return 0, err
		}
	}
	p = p[:min(int64(len(p)), c.remaining)]
	n, err := c.br.Read(p)
	c.remaining -= int64(n)
	c.decoded += int64(n)
	if c.signed {
		c.chunkHash.Write(p[:n])
	}
	if err != nil {
		return n, readErr(err)
	}
	if c.remaining == 0 {
		return n, c.endChunk()
	}
	return n, nil
}

// nextChunk reads a chunk header. It returns io.EOF after a valid final chunk
// and trailers.
func (c *chunkedReader) nextChunk() error {
	line, err := c.readLine()
	if err != nil {
		return err
	}
	size, sig, err := c.parseHeader(line)
	if err != nil {
		return err
	}
	if size > c.decodedLen-c.decoded {
		return fmt.Errorf("chunk of %d bytes exceeds the decoded length: %w", size, ErrMalformedChunk)
	}
	c.chunkHash.Reset()
	if size > 0 {
		if c.signed && c.short {
			return fmt.Errorf("a chunk under %d bytes is not the last: %w", minChunkSize, ErrChunkTooSmall)
		}
		c.remaining, c.chunkSig, c.short = size, sig, size < minChunkSize
		return nil
	}
	if c.signed {
		if err := c.verifyChunk(sig); err != nil {
			return err
		}
	}
	return c.finish()
}

// parseHeader reads "<hex size>" plus, in signed modes, the chunk signature.
func (c *chunkedReader) parseHeader(line string) (size int64, sig string, err error) {
	hexSize, ext, hasExt := strings.Cut(line, ";")
	n, err := strconv.ParseUint(hexSize, 16, 63)
	if err != nil {
		return 0, "", fmt.Errorf("chunk size %q: %w", hexSize, ErrMalformedChunk)
	}
	if !c.signed {
		if hasExt {
			return 0, "", fmt.Errorf("chunk extension %q: %w", ext, ErrMalformedChunk)
		}
		return int64(n), "", nil
	}
	sig, ok := strings.CutPrefix(ext, chunkSignaturePrefix)
	if !ok || !isLowerHex64(sig) {
		return 0, "", fmt.Errorf("chunk extension %q: %w", ext, ErrMalformedChunk)
	}
	return int64(n), sig, nil
}

// endChunk reads the CRLF after a chunk's data and checks its signature.
func (c *chunkedReader) endChunk() error {
	var crlf [2]byte
	if _, err := io.ReadFull(c.br, crlf[:]); err != nil {
		return readErr(err)
	}
	if string(crlf[:]) != "\r\n" {
		return fmt.Errorf("chunk data is not followed by CRLF: %w", ErrMalformedChunk)
	}
	if c.signed {
		return c.verifyChunk(c.chunkSig)
	}
	return nil
}

// finish reads the trailers and checks that the body ends where it should.
// It returns io.EOF on success.
func (c *chunkedReader) finish() error {
	fields, err := c.readTrailers()
	if err != nil {
		return err
	}
	if c.decoded != c.decodedLen {
		return fmt.Errorf("decoded %d bytes, want %d: %w", c.decoded, c.decodedLen, ErrMalformedChunk)
	}
	_, err = c.br.ReadByte()
	if err == nil {
		return fmt.Errorf("data after the final chunk: %w", ErrMalformedChunk)
	}
	if !errors.Is(err, io.EOF) {
		return readErr(err)
	}
	for _, f := range fields {
		c.trailer.Set(f[0], f[1])
	}
	return io.EOF
}

// readTrailers reads the lines after the final chunk header up to the empty
// line. Without trailers, that line comes first.
func (c *chunkedReader) readTrailers() (fields [][2]string, err error) {
	var canonical strings.Builder
	sig := ""
	for {
		line, err := c.readLine()
		if err != nil {
			return nil, err
		}
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		name, value = strings.ToLower(name), strings.TrimSpace(value)
		received, declared := c.seen[name]
		switch {
		case sig != "" || !ok || !c.trailers:
			return nil, fmt.Errorf("unexpected line %.40q after the final chunk: %w", line, ErrMalformedChunk)
		case c.signed && name == trailerSignature:
			if !isLowerHex64(value) {
				return nil, fmt.Errorf("trailer signature %q: %w", value, ErrMalformedChunk)
			}
			sig = value
		case !declared || received:
			return nil, fmt.Errorf("trailer %q is undeclared or repeated: %w", name, ErrMalformedChunk)
		default:
			c.seen[name] = true
			fields = append(fields, [2]string{name, value})
			canonical.WriteString(name + ":" + value + "\n")
		}
	}
	for name, received := range c.seen {
		if !received {
			return nil, fmt.Errorf("declared trailer %q is missing: %w", name, ErrMalformedChunk)
		}
	}
	if c.signed && c.trailers {
		if sig == "" {
			return nil, fmt.Errorf("missing %s: %w", trailerSignature, ErrMalformedChunk)
		}
		sum := sha256.Sum256([]byte(canonical.String()))
		stringToSign := "AWS4-HMAC-SHA256-TRAILER\n" + c.amzDate + "\n" + c.scope + "\n" + c.prev + "\n" + hex.EncodeToString(sum[:])
		if !c.signatureMatches(stringToSign, sig) {
			return nil, fmt.Errorf("trailer signature: %w", ErrSignatureMismatch)
		}
	}
	return fields, nil
}

// verifyChunk checks sig against the chunk data hashed so far and chains it.
func (c *chunkedReader) verifyChunk(sig string) error {
	stringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + c.amzDate + "\n" + c.scope + "\n" + c.prev + "\n" +
		emptySHA256 + "\n" + hex.EncodeToString(c.chunkHash.Sum(nil))
	if !c.signatureMatches(stringToSign, sig) {
		return fmt.Errorf("chunk signature: %w", ErrSignatureMismatch)
	}
	c.prev = sig
	return nil
}

func (c *chunkedReader) signatureMatches(stringToSign, sig string) bool {
	want := hex.EncodeToString(hmacSHA256(c.key, stringToSign))
	return hmac.Equal([]byte(want), []byte(sig))
}

// readLine reads one CRLF-terminated line of at most maxChunkLine bytes.
func (c *chunkedReader) readLine() (string, error) {
	line, err := c.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", fmt.Errorf("line over %d bytes: %w", maxChunkLine, ErrMalformedChunk)
	}
	if err != nil {
		return "", readErr(err)
	}
	s, ok := strings.CutSuffix(string(line), "\r\n")
	if !ok {
		return "", fmt.Errorf("line without CRLF: %w", ErrMalformedChunk)
	}
	return s, nil
}

// readErr turns an early end of the underlying body into io.ErrUnexpectedEOF.
func readErr(err error) error {
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("read aws-chunked body: %w", err)
}

func isLowerHex64(s string) bool {
	return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == ""
}
