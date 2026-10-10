package store

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/yavosh/pail/internal/lifecycle"
)

// SweepLifecycle expires eligible objects and aborts eligible uploads.
func (s *Store) SweepLifecycle(ctx context.Context, now time.Time) error {
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, b := range buckets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.sweepBucket(ctx, b.Name, now); err != nil && !errors.Is(err, ErrNoSuchBucket) {
			failures = append(failures, fmt.Errorf("lifecycle bucket %s: %w", b.Name, err))
		}
	}
	return errors.Join(failures...)
}

func (s *Store) sweepBucket(ctx context.Context, bucket string, now time.Time) error {
	// The exclusive bucket lock excludes writers and configuration changes.
	// Read current metadata under it, so cleanup cannot delete a replacement.
	l := s.bucketLock(bucket)
	l.Lock()
	defer l.Unlock()
	cfg, err := s.GetBucketConfiguration(ctx, bucket, "lifecycle")
	if errors.Is(err, ErrNoSuchConfiguration) {
		return nil
	}
	if err != nil {
		return err
	}
	var rules lifecycle.Configuration
	if err := xml.Unmarshal(cfg.XML, &rules); err != nil {
		return fmt.Errorf("decode lifecycle: %w", err)
	}
	objects, err := s.readAllObjects(ctx, bucket)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, rule := range rules.Rules {
			deadline := rule.Expires(obj.LastModified)
			if !rule.Matches(obj.Key, obj.Size, obj.Tags) || deadline.IsZero() || now.Before(deadline) {
				continue
			}
			if err := s.fs.Remove(metaFile(bucket, obj.Key)); err != nil {
				return fmt.Errorf("expire metadata: %w", err)
			}
			_ = s.fs.Remove(path.Join(blobsDir(bucket), obj.Blob))
			break
		}
	}
	uploads, err := s.ListUploads(ctx, bucket)
	if err != nil {
		return err
	}
	for _, up := range uploads {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, rule := range rules.Rules {
			if rule.Abort == nil || !rule.Matches(up.Key, 0, nil) || now.Before(lifecycle.Deadline(up.Initiated, rule.Abort.Days)) {
				continue
			}
			if err := s.markEnded(bucket, up.ID); err != nil {
				return err
			}
			if err := s.removeUpload(bucket, up.ID); err != nil {
				return err
			}
			break
		}
	}
	return nil
}
