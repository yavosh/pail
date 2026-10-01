package checksum

import (
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
	if got := Canonical("md5"); got != "" {
		t.Errorf("Canonical(md5) = %q, want empty", got)
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
		{"MD5", "y/Q5Jg==", false},
	}
	for _, tt := range tests {
		if _, ok := Decode(tt.algorithm, tt.value); ok != tt.ok {
			t.Errorf("Decode(%s, %q) ok = %v, want %v", tt.algorithm, tt.value, ok, tt.ok)
		}
	}
}
