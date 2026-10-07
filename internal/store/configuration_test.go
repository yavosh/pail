package store

import (
	"errors"
	"reflect"
	"testing"
)

func TestBucketConfigurationPersistence(t *testing.T) {
	for _, kind := range []string{"cors", "lifecycle", "acl"} {
		t.Run(kind, func(t *testing.T) {
			s, fsys := newStore(t)
			mustCreate(t, s, "bucket")
			want := BucketConfiguration{XML: []byte("<configuration/>"), TransitionMinimum: "all_storage_classes_128K"}
			if err := s.PutBucketConfiguration(t.Context(), "bucket", kind, &want); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(t.Context(), fsys)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reopened.GetBucketConfiguration(t.Context(), "bucket", kind)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("reopened %s = %+v, %v, want %+v", kind, got, err, want)
			}
			if err := reopened.DeleteBucket(t.Context(), "bucket"); err != nil {
				t.Fatal(err)
			}
			mustCreate(t, reopened, "bucket")
			if _, err := reopened.GetBucketConfiguration(t.Context(), "bucket", kind); !errors.Is(err, ErrNoSuchConfiguration) {
				t.Fatalf("recreated %s error = %v, want ErrNoSuchConfiguration", kind, err)
			}
		})
	}
}
