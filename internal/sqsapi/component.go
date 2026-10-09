package sqsapi

import "log/slog"

// clogSqsapi returns the logger for this package. Every line carries a component.
func clogSqsapi() *slog.Logger { return slog.With("component", "sqsapi") }
