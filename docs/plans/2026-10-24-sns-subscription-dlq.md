# SNS subscription dead-letter queues (#85)

Class: feature. Add the existing SNS subscription `RedrivePolicy` attribute.

## Plan

1. Validate policy JSON in a small `internal/topic/redrive.go` helper.
   Preserve accepted policy text. Require a same-account, same-region SQS ARN.
   Match the subscription endpoint's queue type for FIFO topics.
   Do not require the destination queue to exist during configuration.
2. Distinguish omitted `AttributeValue` from an explicit empty value in
   `internal/snsapi/subscriptions.go` and the topic engine's setter.
   Use a nullable string parameter; update existing internal callers.
   Only omission clears a redrive policy. Preserve other attributes' behavior.
3. Integrate validation, response attributes, and persistence in subscription
   creation, updates, and loading. Keep queue calls outside the topic mutex.
4. Snapshot the redrive destination during publish. Preserve the delivered
   envelope or raw body, attributes, group ID, and deduplication ID.
   Return SQS delivery errors to the fanout caller and redrive failures once.
   A failed dead-letter send is logged and never recursively redriven.
5. Carry dead-letter delivery data with HTTP notification jobs. Redrive after
   permanent failure or retry exhaustion, including local queue overflow.
   Do not redrive confirmations, successful deliveries, or shutdown cancellation.
   Reuse existing workers and retry schedules; add no background goroutine.
6. Add failing-first SDK coverage, deterministic topic tests, and AWS differential
   scenarios for policy validation and standard, raw, and FIFO redrive payloads.
   Verify message identity and FIFO identifiers before normalization.
7. Update README, AGENTS, SQS/SNS compatibility, limitations, and S3 notification
   documentation. Explain immediate local SQS failure handling, transient jobs,
   and the existing IAM/KMS limitations.

## Evidence and gates

Base: `fe376a45da5d969eb38dcf3df57193a79183cce6`. Baseline race tests passed.
Live AWS probes verified policy validation, omission-only clearing, preserved
policy formatting, and successful standard, raw, and FIFO dead-letter delivery.
FIFO topic subscriptions require a DLQ matching their endpoint queue type.
Standard topics accept FIFO DLQ ARNs during configuration.
Missing queues are accepted during configuration. Different regions and accounts fail.
Dead-letter payloads preserve the delivery envelope or raw body and attributes.
FIFO dead-letter messages preserve the publication group and deduplication IDs.

Gates: scoped formatting and modernization, golangci-lint, full race tests,
CGO-free build, govulncheck, fresh AWS recordings, local replay, independent review,
and green CI before the requested merge.

## Risks and non-goals

Preserve FIFO fanout ordering and deduplication, cancellation, and lock ordering.
A failed primary delivery must not fail Publish or suppress other subscriptions.
No IAM/KMS enforcement, durable message jobs, or 23-day AWS-managed endpoint retry loop.
Issue #86 owns the public HTTP callback comparison. Its findings may refine payload behavior.
SQS FIFO delay updates and dead-letter retention are separate bugs.
