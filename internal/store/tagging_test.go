package store

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/lifecycle"
	"github.com/yavosh/pail/internal/tag"
)

func TestObjectTagsPersistence(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "bucket")
	before := mustPut(t, s, "bucket", "key", "body")
	tags := []tag.Tag{{Key: "b", Value: "2"}, {Key: "a", Value: ""}}
	if err := s.PutObjectTags(t.Context(), "bucket", "key", "", tags); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	body, after := mustGet(t, reopened, "bucket", "key")
	if body != "body" || after.ETag != before.ETag || !after.LastModified.Equal(before.LastModified) || after.Checksum != before.Checksum || !reflect.DeepEqual(after.Tags, tags) {
		t.Fatalf("tag update: before %+v, after %+v, body %q, want the same ETag, time, and checksum, and tags %v", before, after, body, tags)
	}
	if err := s.PutObjectTags(t.Context(), "bucket", "key", "", nil); err != nil {
		t.Fatal(err)
	}
	if info, err := s.HeadObject(t.Context(), "bucket", "key"); err != nil || len(info.Tags) != 0 {
		t.Fatalf("tags after clearing = %v, %v, want none", info.Tags, err)
	}
}

func TestPutObjectTagsErrors(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	tests := []struct {
		name, bucket, key string
		want              error
	}{
		{"missing key", "bucket", "missing", ErrNoSuchKey},
		{"missing bucket", "nobucket", "key", ErrNoSuchBucket},
	}
	for _, tt := range tests {
		if err := s.PutObjectTags(t.Context(), tt.bucket, tt.key, "", []tag.Tag{{Key: "a"}}); !errors.Is(err, tt.want) {
			t.Errorf("%s: PutObjectTags error = %v, want %v", tt.name, err, tt.want)
		}
	}
}

func TestPutOptionsTagsReplaceOnOverwrite(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	if _, err := s.PutObject(t.Context(), "bucket", "key", strings.NewReader("one"), PutOptions{Tags: []tag.Tag{{Key: "a", Value: "1"}}}); err != nil {
		t.Fatal(err)
	}
	info := mustPut(t, s, "bucket", "key", "two")
	if len(info.Tags) != 0 {
		t.Fatalf("tags after an untagged overwrite = %v, want none", info.Tags)
	}
}

func TestBucketTaggingConfiguration(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	if _, err := s.GetBucketConfiguration(t.Context(), "bucket", "tagging"); !errors.Is(err, ErrNoSuchConfiguration) {
		t.Fatalf("Get before Put error = %v, want ErrNoSuchConfiguration", err)
	}
	cfg := &BucketConfiguration{XML: []byte("<Tagging/>")}
	if err := s.PutBucketConfiguration(t.Context(), "bucket", "tagging", cfg); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetBucketConfiguration(t.Context(), "bucket", "tagging"); err != nil || string(got.XML) != "<Tagging/>" {
		t.Fatalf("Get = %q, %v, want <Tagging/>", got.XML, err)
	}
}

func TestSweepLifecycleTagFilter(t *testing.T) {
	a, b := tag.Tag{Key: "a", Value: "1"}, tag.Tag{Key: "b", Value: "2"}
	prefix := "dir/"
	tests := []struct {
		name   string
		filter lifecycle.Filter
		gone   []string
	}{
		{"one tag", lifecycle.Filter{Tags: []tag.Tag{b}}, []string{"dir/both", "both", "wrong-a"}},
		{"and of tags", lifecycle.Filter{And: &lifecycle.Filter{Tags: []tag.Tag{a, b}}}, []string{"dir/both", "both"}},
		{"and of prefix and tag", lifecycle.Filter{And: &lifecycle.Filter{Prefix: &prefix, Tags: []tag.Tag{a}}}, []string{"dir/both"}},
	}
	for _, tt := range tests {
		synctest.Test(t, func(t *testing.T) {
			s, _ := newStore(t)
			mustCreate(t, s, "bucket")
			objects := map[string][]tag.Tag{
				"dir/both": {a, b}, "both": {a, b}, "only-a": {a}, "wrong-a": {{Key: "a", Value: "9"}, b}, "untagged": nil,
			}
			for key, tags := range objects {
				if _, err := s.PutObject(t.Context(), "bucket", key, strings.NewReader("x"), PutOptions{Tags: tags}); err != nil {
					t.Fatal(err)
				}
			}
			days := 1
			putLifecycle(t, s, "bucket", []lifecycle.Rule{{ID: "r", Status: "Enabled", Filter: &tt.filter, Expiration: &lifecycle.Expiration{Days: &days}}})
			time.Sleep(72 * time.Hour)
			if err := s.SweepLifecycle(t.Context(), time.Now()); err != nil {
				t.Fatal(err)
			}
			for key := range objects {
				_, err := s.HeadObject(t.Context(), "bucket", key)
				if gone, want := errors.Is(err, ErrNoSuchKey), slices.Contains(tt.gone, key); gone != want {
					t.Errorf("%s: object %q gone = %v (%v), want %v", tt.name, key, gone, err, want)
				}
			}
		})
	}
}
