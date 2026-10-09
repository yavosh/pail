package vfs

import (
	"encoding/json"
	"io"
)

// WriteJSON commits v as JSON to name through a temp file.
func WriteJSON(fsys FS, name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tf, err := fsys.CreateTemp()
	if err != nil {
		return err
	}
	defer func() { _ = tf.Abort() }()
	if _, err := tf.Write(b); err != nil {
		return err
	}
	return tf.Commit(name)
}

// ReadJSON decodes the JSON file name into v.
func ReadJSON(fsys FS, name string, v any) error {
	f, err := fsys.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
