package store

import (
	"context"
	"fmt"

	"github.com/yavosh/pail/internal/tag"
)

// PutObjectTags replaces the tags of the current object under its key lock.
// It keeps the ETag and the modification time. Empty tags remove them.
func (s *Store) PutObjectTags(ctx context.Context, bucket, key string, tags []tag.Tag) error {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return err
	}
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	kl := s.keyLock(bucket, key)
	kl.Lock()
	defer kl.Unlock()
	rec, err := s.readRecord(bucket, key)
	if err != nil {
		return s.missing(ctx, bucket, err)
	}
	rec.Tags = tags
	if err := s.writeJSON(metaFile(bucket, key), rec); err != nil {
		return fmt.Errorf("write object tags: %w", err)
	}
	return nil
}
