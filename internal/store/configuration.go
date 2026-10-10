package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
)

// ErrNoSuchConfiguration means the bucket has no requested configuration.
var ErrNoSuchConfiguration = errors.New("no such bucket configuration")

// BucketConfiguration stores the XML and optional response header atomically.
type BucketConfiguration struct {
	XML               []byte `json:"xml"`
	TransitionMinimum string `json:"transitionMinimum,omitempty"`
}

func configurationFile(bucket, kind string) (string, error) {
	if kind != "cors" && kind != "lifecycle" && kind != "acl" && kind != "ownership" {
		return "", fmt.Errorf("unknown configuration %q", kind)
	}
	return path.Join(bucketDir(bucket), kind+".json"), nil
}

// GetBucketConfiguration reads a persisted bucket configuration.
func (s *Store) GetBucketConfiguration(ctx context.Context, bucket, kind string) (BucketConfiguration, error) {
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return BucketConfiguration{}, err
	}
	name, err := configurationFile(bucket, kind)
	if err != nil {
		return BucketConfiguration{}, err
	}
	var cfg BucketConfiguration
	if err := s.readJSON(name, &cfg); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return BucketConfiguration{}, ErrNoSuchConfiguration
		}
		return BucketConfiguration{}, fmt.Errorf("read %s configuration: %w", kind, err)
	}
	return cfg, nil
}

// PutBucketConfiguration replaces a configuration. Nil removes it.
func (s *Store) PutBucketConfiguration(ctx context.Context, bucket, kind string, cfg *BucketConfiguration) error {
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	name, err := configurationFile(bucket, kind)
	if err != nil {
		return err
	}
	l := s.bucketLock(bucket)
	l.Lock()
	defer l.Unlock()
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	if cfg != nil {
		if err := s.writeJSON(name, cfg); err != nil {
			return fmt.Errorf("write %s configuration: %w", kind, err)
		}
		return nil
	}
	if err := s.fs.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete %s configuration: %w", kind, err)
	}
	return nil
}
