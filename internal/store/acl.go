package store

import (
	"context"
	"fmt"

	"github.com/yavosh/pail/internal/acl"
)

// PutObjectACL changes only the ACL of a version under its key lock. An empty
// versionID names the current version.
func (s *Store) PutObjectACL(ctx context.Context, bucket, key, versionID string, policy acl.Policy) error {
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
	rec, current, err := s.lookupObject(bucket, key, versionID)
	if err != nil {
		return s.missing(ctx, bucket, err)
	}
	if rec.ACL != nil && rec.ACL.Owner.ID != policy.Owner.ID {
		return ErrAccessDenied
	}
	rec.ACL = &policy
	if err := s.updateRecord(bucket, rec, current); err != nil {
		return fmt.Errorf("write object ACL: %w", err)
	}
	return nil
}
