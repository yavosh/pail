package kmsapi

import "log/slog"

func clogKmsapi() *slog.Logger { return slog.With("component", "kmsapi") }
