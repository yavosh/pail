# SQS FIFO receive retries (#84)

Class: feature. Add the existing AWS `ReceiveRequestAttemptId` parameter.
The API currently drops it, so a retry cannot retrieve its invisible messages.

## Plan

1. Record AWS validation, repeated tokens, changed parameters, empty receives,
   and retries after deletion or visibility changes.
2. Pass an optional attempt ID through `internal/sqsapi/messages.go` and
   `queue.ReceiveInput`. Distinguish an absent ID from an empty one.
3. Add queue-owned, transient retry records under the existing engine mutex.
   Reuse the original selection without increasing receive counts, reset
   visibility, and reject modified selections or incompatible parameters.
   Expire records after five minutes without extending the window on retries.
4. Store message references and validity metadata instead of copying bodies.
   Limit each queue to 10,000 live attempt records. Reject new attempts with
   `OverLimit` at capacity; never evict a valid retry record early.
5. Preserve cancellation, long polling, FIFO group locks, queue deletion,
   and standard-queue behavior. Add no background goroutine.
6. Add deterministic engine tests, SDK tests, and AWS differential scenarios.
   Check message identity and receive counts before normalization masks IDs.
7. Update README, AGENTS, limitations, SQS/SNS compatibility, and the S3
   notification documentation's link to the SQS receive contract.

## Evidence and gates

The base is `4f01cb4454974e51407998e6226a65ce5c2884bd`.
The baseline `go test ./... -race` passed.
Run scoped formatting, modernization, lint, race tests, a CGO-free build,
govulncheck, and replay of newly recorded AWS scenarios before committing.

AWS probes show that standard queues ignore attempt IDs, including invalid
ones. FIFO queues reject empty, whitespace, Unicode, and oversized IDs.
Retries preserve message IDs and receive counts. Changed maximum counts or
visibility values fail. Empty results are remembered. Deleting a selected
message or changing its visibility invalidates the attempt.
An expired visibility timeout invalidates a populated attempt too. Explicit
default parameters match omitted defaults. A successful purge clears attempts,
including cached empty results.
AWS returned different opaque receipt strings on identical retries; verify
their usability rather than assuming their bytes must match.

## Risks and non-goals

Retry state must not retain expired messages indefinitely or bypass visibility
and deletion rules. Concurrent retries must share one selection.
FIFO delay updates and dead-letter retention are separate bugs.
SNS subscription redrive and live HTTP verification belong to #85 and #86.
