package queue

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"slices"
	"strings"
)

// MD5OfBody returns the hex MD5 of a message body.
func MD5OfBody(body string) string {
	sum := md5.Sum([]byte(body))
	return hex.EncodeToString(sum[:])
}

// MD5OfAttributes returns the hex MD5 of a message attribute map, or "" for an
// empty map. This is the SQS algorithm as the Java SDK implements it;
// aws-sdk-go-v2 validates only the body MD5, so PR 4 records an AWS response to confirm it.
func MD5OfAttributes(attrs map[string]MessageAttribute) string {
	if len(attrs) == 0 {
		return ""
	}
	h := md5.New()
	field := func(b []byte) {
		h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(b))))
		h.Write(b)
	}
	for _, name := range slices.Sorted(maps.Keys(attrs)) {
		a := attrs[name]
		field([]byte(name))
		field([]byte(a.DataType))
		if strings.HasPrefix(a.DataType, "Binary") {
			h.Write([]byte{2})
			field(a.BinaryValue)
			continue
		}
		h.Write([]byte{1})
		field([]byte(a.StringValue))
	}
	return hex.EncodeToString(h.Sum(nil))
}
