package checksum

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestKnownValues(t *testing.T) {
	tests := []struct {
		algorithm, input, want string
	}{
		// The CRC64NVME values were recorded from AWS S3 (test/diff golden files).
		{CRC64NVME, "", "AAAAAAAAAAA="},
		{CRC64NVME, "hello world", "jSnVw/bqjr4="},
		{CRC64NVME, "x", "Lb1nAmRU5LE="},
		// The CRC-64/NVME catalog check value for "123456789" is 0xAE8B14860A799888.
		{CRC64NVME, "123456789", "rosUhgp5mIg="},
		// IEEE check value 0xCBF43926, Castagnoli 0xE3069283.
		{CRC32, "123456789", "y/Q5Jg=="},
		{CRC32C, "123456789", "4waSgw=="},
		{SHA1, "abc", "qZk+NkcGgWq6PiVxeFDCbJzQ2J0="},
		{SHA256, "abc", "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="},
		{SHA512, "abc", "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw=="},
		{MD5, "abc", "kAFQmDzST7DWlj99KOF/cg=="},
		{XXHASH64, "", "70bbN1HY6Zk="},
	}
	for _, tt := range tests {
		h, ok := New(tt.algorithm)
		if !ok {
			t.Fatalf("New(%q) not supported", tt.algorithm)
		}
		h.Write([]byte(tt.input))
		if got := Encode(h.Sum(nil)); got != tt.want {
			t.Errorf("%s(%q) = %s, want %s", tt.algorithm, tt.input, got, tt.want)
		}
	}
}

func TestNamesAndDecode(t *testing.T) {
	if got := Canonical("crc32c"); got != CRC32C {
		t.Errorf("Canonical(crc32c) = %q, want %q", got, CRC32C)
	}
	if got := Canonical("md5"); got != MD5 {
		t.Errorf("Canonical(md5) = %q, want %q", got, MD5)
	}
	for _, name := range []string{"xxhash3", "xxhash128", "bogus"} {
		if got := Canonical(name); got != "" {
			t.Errorf("Canonical(%s) = %q, want empty", name, got)
		}
	}
	if got := Header(CRC64NVME); got != "x-amz-checksum-crc64nvme" {
		t.Errorf("Header(CRC64NVME) = %q, want %q", got, "x-amz-checksum-crc64nvme")
	}
	tests := []struct {
		algorithm, value string
		ok               bool
	}{
		{CRC32, "y/Q5Jg==", true},
		{CRC32, "AAAAAAAAAAA=", false}, // 8 bytes for a 4-byte sum
		{CRC32, "not base64!", false},
		{SHA512, "y/Q5Jg==", false},
		{MD5, "kAFQmDzST7DWlj99KOF/cg==", true},
		{XXHASH64, "70bbN1HY6Zk=", true},
		{XXHASH64, "y/Q5Jg==", false},
	}
	for _, tt := range tests {
		if _, ok := Decode(tt.algorithm, tt.value); ok != tt.ok {
			t.Errorf("Decode(%s, %q) ok = %v, want %v", tt.algorithm, tt.value, ok, tt.ok)
		}
	}
}

// The XXH64 vectors come from the xxHash reference implementation, seed 0.
func TestXXHash64(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"", "ef46db3751d8e999"},
		{"a", "d24ec4f1a98c6e5b"},
		{"abc", "44bc2cf5ad770999"},
		{"Nobody inspects the spammish repetition", "fbcea83c8a378bf1"},
	}
	for _, tt := range tests {
		// Every split of the input must give the same digest.
		for chunk := 1; chunk <= len(tt.input)+1; chunk++ {
			h, _ := New(XXHASH64)
			for rest := tt.input; rest != ""; rest = rest[min(chunk, len(rest)):] {
				h.Write([]byte(rest[:min(chunk, len(rest))]))
			}
			if got := hex.EncodeToString(h.Sum(nil)); got != tt.want {
				t.Errorf("XXHASH64(%q) in %d-byte writes = %s, want %s", tt.input, chunk, got, tt.want)
			}
		}
	}
}

func TestXXHash64Reset(t *testing.T) {
	h, _ := New(XXHASH64)
	h.Write([]byte(strings.Repeat("x", 100)))
	h.Reset()
	h.Write([]byte("a"))
	if got, want := hex.EncodeToString(h.Sum(nil)), "d24ec4f1a98c6e5b"; got != want {
		t.Errorf("XXHASH64(a) after Reset = %s, want %s", got, want)
	}
}
