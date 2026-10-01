package test

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// putKeys stores an empty object under each key, a few at a time.
func putKeys(t *testing.T, c *s3.Client, bucket string, keys []string) {
	t.Helper()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, k := range keys {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			if _, err := c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(k), Body: strings.NewReader("")}); err != nil {
				t.Errorf("PutObject(%q) error = %v", k, err)
			}
		})
	}
	wg.Wait()
}

func TestListObjectsV2Paginator(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		mustBucket(t, c, "many")
		var want []string
		for i := range 2500 {
			want = append(want, fmt.Sprintf("k%05d", i))
		}
		putKeys(t, c, "many", want)

		var got []string
		pages := 0
		p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String("many")})
		for p.HasMorePages() {
			page, err := p.NextPage(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			pages++
			for _, o := range page.Contents {
				got = append(got, aws.ToString(o.Key))
				if o.StorageClass != types.ObjectStorageClassStandard || o.LastModified == nil || aws.ToString(o.ETag) == "" {
					t.Errorf("listed object %+v lacks StorageClass, LastModified, or ETag", o)
				}
			}
		}
		if pages != 3 || len(got) != len(want) {
			t.Errorf("paginator walked %d keys in %d pages, want %d keys in 3 pages", len(got), pages, len(want))
		}
		for i := range min(len(got), len(want)) {
			if got[i] != want[i] {
				t.Errorf("paginator key %d = %q, want %q (keys must be in order with no gaps or repeats)", i, got[i], want[i])
				break
			}
		}
	})
}

func TestListObjectsDelimiters(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "tree")
		putKeys(t, c, "tree", []string{"a", "a/b", "a/c/d", "dir/", "dir/x", "e-1", "e-2", "sp ace+plus/k", "✓/k"})

		tests := []struct {
			prefix, delimiter      string
			wantKeys, wantPrefixes []string
		}{
			{"", "/", []string{"a", "e-1", "e-2"}, []string{"a/", "dir/", "sp ace+plus/", "✓/"}},
			{"a/", "/", []string{"a/b"}, []string{"a/c/"}},
			{"dir/", "/", []string{"dir/", "dir/x"}, nil},
			{"", "-", []string{"a", "a/b", "a/c/d", "dir/", "dir/x", "sp ace+plus/k", "✓/k"}, []string{"e-"}},
		}
		for _, tt := range tests {
			// aws-sdk-go-v2 leaves encoding-type=url keys encoded; decoding them must round-trip.
			out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("tree"), Prefix: aws.String(tt.prefix), Delimiter: aws.String(tt.delimiter), EncodingType: types.EncodingTypeUrl})
			if err != nil {
				t.Fatal(err)
			}
			var keys, prefixes []string
			for _, o := range out.Contents {
				keys = append(keys, unescape(t, aws.ToString(o.Key)))
			}
			for _, p := range out.CommonPrefixes {
				prefixes = append(prefixes, unescape(t, aws.ToString(p.Prefix)))
			}
			if !slices.Equal(keys, tt.wantKeys) || !slices.Equal(prefixes, tt.wantPrefixes) {
				t.Errorf("ListObjectsV2(prefix %q, delimiter %q) = %q, %q, want %q, %q", tt.prefix, tt.delimiter, keys, prefixes, tt.wantKeys, tt.wantPrefixes)
			}
			if got := int(aws.ToInt32(out.KeyCount)); got != len(tt.wantKeys)+len(tt.wantPrefixes) {
				t.Errorf("KeyCount = %d, want %d", got, len(tt.wantKeys)+len(tt.wantPrefixes))
			}
		}

		// V1 with a delimiter pages by NextMarker, prefixes included.
		var seen []string
		marker := ""
		for range 10 {
			out, err := c.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String("tree"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(2), Marker: aws.String(marker)})
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range out.Contents {
				seen = append(seen, aws.ToString(o.Key))
			}
			for _, p := range out.CommonPrefixes {
				seen = append(seen, aws.ToString(p.Prefix))
			}
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			marker = aws.ToString(out.NextMarker)
		}
		slices.Sort(seen)
		if want := []string{"a", "a/", "dir/", "e-1", "e-2", "sp ace+plus/", "✓/"}; !slices.Equal(seen, want) {
			t.Errorf("ListObjects pages = %q, want %q", seen, want)
		}

		_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("missing")})
		if code := errorCode(err); code != "NoSuchBucket" {
			t.Errorf("ListObjectsV2(missing bucket) error = %v, want NoSuchBucket", err)
		}
	})
}

func unescape(t *testing.T, s string) string {
	t.Helper()
	u, err := url.QueryUnescape(s)
	if err != nil {
		t.Fatalf("QueryUnescape(%q) error = %v", s, err)
	}
	return u
}

func TestListObjectsStartAfterAndOwner(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "owned")
		putKeys(t, c, "owned", []string{"a", "b", "c"})

		out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("owned"), StartAfter: aws.String("a")})
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
			if o.Owner != nil {
				t.Errorf("ListObjectsV2 without FetchOwner returned an owner for %s", aws.ToString(o.Key))
			}
		}
		if want := []string{"b", "c"}; !slices.Equal(keys, want) {
			t.Errorf("ListObjectsV2(StartAfter a) = %q, want %q", keys, want)
		}

		out, err = c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("owned"), FetchOwner: aws.Bool(true)})
		if err != nil || len(out.Contents) == 0 || out.Contents[0].Owner == nil || aws.ToString(out.Contents[0].Owner.ID) == "" {
			t.Errorf("ListObjectsV2(FetchOwner) = %+v, %v, want an owner ID on each object", out, err)
		}
	})
}
