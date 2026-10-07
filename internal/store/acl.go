package store

import (
	"context"
	"fmt"

	"github.com/yavosh/pail/internal/acl"
)

// PutObjectACL changes only the ACL of the current object under its write lock.
func (s *Store) PutObjectACL(ctx context.Context, bucket, key string, policy acl.Policy) error {
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
	if rec.ACL != nil && rec.ACL.Owner.ID != policy.Owner.ID {
		return ErrAccessDenied
	}
	rec.ACL = &policy
	if err := s.writeJSON(metaFile(bucket, key), rec); err != nil {
		return fmt.Errorf("write object ACL: %w", err)
	}
	return nil
}
