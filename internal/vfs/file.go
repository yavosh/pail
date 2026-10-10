package vfs

import (
	"fmt"
	"io"
)

// WriteFile commits data to name through a temp file.
func WriteFile(fsys FS, name string, data []byte) error {
	tf, err := fsys.CreateTemp()
	if err != nil {
		return fmt.Errorf("create temp file for %s: %w", name, err)
	}
	defer func() { _ = tf.Abort() }()
	if _, err := tf.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := tf.Commit(name); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}
	return nil
}

// ReadFile returns the contents of name.
func ReadFile(fsys FS, name string) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return b, nil
}
