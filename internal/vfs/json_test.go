package vfs_test

import (
	"errors"
	"io/fs"
	"reflect"
	"testing"

	"github.com/yavosh/pail/internal/vfs"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

type doc struct {
	Name string   `json:"name"`
	N    int      `json:"n"`
	Tags []string `json:"tags"`
}

func openFS(t *testing.T) vfs.FS {
	t.Helper()
	f, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatalf("localdisk.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestJSONRoundTrip(t *testing.T) {
	fsys := openFS(t)
	want := doc{Name: "a", N: 3, Tags: []string{"x", "y"}}
	if err := vfs.WriteJSON(fsys, "d.json", want); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}
	var got doc
	if err := vfs.ReadJSON(fsys, "d.json", &got); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadJSON() = %+v, want %+v", got, want)
	}
}

func TestReadJSONMissing(t *testing.T) {
	var got doc
	err := vfs.ReadJSON(openFS(t), "missing.json", &got)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadJSON(missing) error = %v, want fs.ErrNotExist", err)
	}
}

func TestWriteJSONReplaces(t *testing.T) {
	fsys := openFS(t)
	for _, d := range []doc{{Name: "first", N: 1}, {Name: "second", N: 2}} {
		if err := vfs.WriteJSON(fsys, "d.json", d); err != nil {
			t.Fatalf("WriteJSON(%+v) error = %v", d, err)
		}
	}
	var got doc
	if err := vfs.ReadJSON(fsys, "d.json", &got); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
	if got.Name != "second" || got.N != 2 {
		t.Errorf("ReadJSON() = %+v, want the second write", got)
	}
}
