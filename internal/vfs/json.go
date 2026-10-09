package vfs

import (
	"encoding/json"
	"fmt"
	"io"
)

// WriteJSON commits v as JSON to name through a temp file.
func WriteJSON(fsys FS, name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	tf, err := fsys.CreateTemp()
	if err != nil {
		return fmt.Errorf("create temp file for %s: %w", name, err)
	}
	defer func() { _ = tf.Abort() }()
	if _, err := tf.Write(b); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := tf.Commit(name); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}
	return nil
}

// ReadJSON decodes the JSON file name into v.
func ReadJSON(fsys FS, name string, v any) error {
	f, err := fsys.Open(name)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}
