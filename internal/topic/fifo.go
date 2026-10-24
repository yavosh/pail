package topic

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"math/big"
	"strings"
	"time"
)

const dedupWindow = 5 * time.Minute

// fifoState is transient, like the messages in SQS. fifoPublish protects it.
type fifoState struct {
	sequence big.Int
	dedup    map[dedupKey]dedupEntry
}

type dedupKey struct{ group, id string }

type dedupEntry struct {
	output  PublishOutput
	expires time.Time
}

func newFIFOState() *fifoState { return &fifoState{dedup: map[dedupKey]dedupEntry{}} }

func isFIFO(name string) bool { return strings.HasSuffix(name, ".fifo") }

func checkFIFOType(name string, attrs map[string]string) error {
	if isFIFO(name) != (attrs["FifoTopic"] == "true") {
		return invalid("a .fifo name requires FifoTopic true, and FifoTopic true requires a .fifo name")
	}
	return nil
}

func validMessageID(id string) bool {
	return len(id) > 0 && len(id) <= 128 && !strings.ContainsFunc(id, func(r rune) bool { return r < 0x21 || r > 0x7e })
}

// prepareFIFO validates the topic-dependent fields and derives the deduplication key.
// The caller holds mu and, for FIFO topics, fifoPublish.
func prepareFIFO(t *topicDef, in *PublishInput) (dedupKey, error) {
	if !isFIFO(t.Name) {
		if in.DeduplicationID != "" {
			return dedupKey{}, invalid("MessageDeduplicationId is only supported on FIFO topics")
		}
		return dedupKey{}, nil
	}
	if !validMessageID(in.GroupID) {
		return dedupKey{}, invalid("MessageGroupId must be 1 to 128 printable ASCII characters")
	}
	if in.DeduplicationID == "" && t.Attributes["ContentBasedDeduplication"] == "true" {
		sum := sha256.Sum256([]byte(in.Message))
		in.DeduplicationID = hex.EncodeToString(sum[:])
	}
	if !validMessageID(in.DeduplicationID) {
		return dedupKey{}, invalid("MessageDeduplicationId must be 1 to 128 printable ASCII characters, or enable ContentBasedDeduplication")
	}
	key := dedupKey{id: in.DeduplicationID}
	if t.Attributes["FifoThroughputScope"] == "MessageGroup" {
		key.group = in.GroupID
	}
	return key, nil
}

func (f *fifoState) duplicate(key dedupKey, now time.Time) (PublishOutput, bool) {
	maps.DeleteFunc(f.dedup, func(_ dedupKey, entry dedupEntry) bool { return !now.Before(entry.expires) })
	entry, ok := f.dedup[key]
	return entry.output, ok
}

func (f *fifoState) nextSequence() string {
	f.sequence.Add(&f.sequence, big.NewInt(1))
	return f.sequence.String()
}
