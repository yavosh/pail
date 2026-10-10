package topic

import "log/slog"

// clogTopic returns the logger for this package. Every line carries a component.
func clogTopic() *slog.Logger { return slog.With("component", "topic") }
