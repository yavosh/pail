package queue

import "log/slog"

// clogQueue returns the logger for this package. Every line carries a component.
func clogQueue() *slog.Logger { return slog.With("component", "queue") }
