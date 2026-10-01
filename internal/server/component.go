package server

import "log/slog"

// clogServer returns the logger for this package. Every line carries a component.
func clogServer() *slog.Logger { return slog.With("component", "server") }
