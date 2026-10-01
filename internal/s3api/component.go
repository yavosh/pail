package s3api

import "log/slog"

// Subsystem logger, so a backend selects lines by field rather than by message.

func clogS3api() *slog.Logger { return slog.With("component", "s3api") }
