package snsapi

import "log/slog"

// clogSnsapi returns the logger for this package. Every line carries a component.
func clogSnsapi() *slog.Logger { return slog.With("component", "snsapi") }
