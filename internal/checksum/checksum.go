// Package checksum implements the S3 flexible checksum algorithms.
package checksum

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"slices"
	"strings"
)

// Algorithm names as S3 writes them, and the default for new objects.
const (
	CRC32     = "CRC32"
	CRC32C    = "CRC32C"
	CRC64NVME = "CRC64NVME"
	SHA1      = "SHA1"
	SHA256    = "SHA256"
	SHA512    = "SHA512"
	MD5       = "MD5"
	XXHASH64  = "XXHASH64"
	Default   = CRC64NVME
)

// Checksum types. A single PUT produces only FullObject; a multipart upload
// produces either.
const (
	FullObject = "FULL_OBJECT"
	Composite  = "COMPOSITE"
)

// nvme is CRC-64/NVME: polynomial 0xAD93D23594C93659, here bit-reversed
// because hash/crc64 works on reflected input, as NVMe CRC-64 does.
var nvme = crc64.MakeTable(0x9A6C9329AC4BC9B5)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Algorithms lists every supported algorithm.
var Algorithms = []string{CRC32, CRC32C, CRC64NVME, SHA1, SHA256, SHA512, MD5, XXHASH64}

// New returns a hash for algorithm, or false for an unknown one. The name is
// case-insensitive, as in the x-amz-sdk-checksum-algorithm header.
func New(algorithm string) (hash.Hash, bool) {
	switch strings.ToUpper(algorithm) {
	case CRC32:
		return crc32.NewIEEE(), true
	case CRC32C:
		return crc32.New(castagnoli), true
	case CRC64NVME:
		return crc64.New(nvme), true
	case SHA1:
		return sha1.New(), true
	case SHA256:
		return sha256.New(), true
	case SHA512:
		return sha512.New(), true
	case MD5:
		return md5.New(), true
	case XXHASH64:
		return newXXH64(), true
	}
	return nil, false
}

// Canonical returns the S3 spelling of algorithm, or "" for an unknown one.
func Canonical(algorithm string) string {
	if a := strings.ToUpper(algorithm); slices.Contains(Algorithms, a) {
		return a
	}
	return ""
}

// Header is the request and response header that carries algorithm's value.
func Header(algorithm string) string {
	return "x-amz-checksum-" + strings.ToLower(algorithm)
}

// Encode is the base64 form S3 uses for a checksum.
func Encode(sum []byte) string {
	return base64.StdEncoding.EncodeToString(sum)
}

// Decode parses a base64 checksum and checks its length for algorithm.
func Decode(algorithm, value string) ([]byte, bool) {
	h, ok := New(algorithm)
	if !ok {
		return nil, false
	}
	b, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(b) != h.Size() {
		return nil, false
	}
	return b, true
}
