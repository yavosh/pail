package s3api

import "log/slog"

// clogS3api returns the logger for this package. Every line carries a component.
func clogS3api() *slog.Logger { return slog.With("component", "s3api") }
