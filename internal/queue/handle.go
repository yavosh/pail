package queue

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
)

const macLen = 16

// handleMAC signs a receipt handle. Handles validate themselves, so a deleted
// message's handle is recognized without state, and a restart invalidates them.
func (e *Engine) handleMAC(queue string, id [16]byte, seq uint32) []byte {
	h := hmac.New(sha256.New, e.key)
	h.Write([]byte(queue))
	h.Write([]byte{0})
	h.Write(id[:])
	h.Write(binary.BigEndian.AppendUint32(nil, seq))
	return h.Sum(nil)[:macLen]
}

func (e *Engine) newHandle(queue string, id [16]byte, seq uint32) string {
	b := append(id[:], binary.BigEndian.AppendUint32(nil, seq)...)
	b = append(b, e.handleMAC(queue, id, seq)...)
	return base64.RawURLEncoding.EncodeToString(b)
}

// parseHandle returns the message ID and sequence of a handle this engine
// issued for queue.
func (e *Engine) parseHandle(queue, handle string) (id [16]byte, seq uint32, ok bool) {
	b, err := base64.RawURLEncoding.DecodeString(handle)
	if err != nil || len(b) != 16+4+macLen {
		return id, 0, false
	}
	copy(id[:], b[:16])
	seq = binary.BigEndian.Uint32(b[16:20])
	if !hmac.Equal(b[20:], e.handleMAC(queue, id, seq)) {
		return [16]byte{}, 0, false
	}
	return id, seq, true
}
