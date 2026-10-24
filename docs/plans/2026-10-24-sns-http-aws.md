# Live SNS HTTP delivery comparison (#86)

Class: feature. Add a maintainer-run callback comparison to `test/diff`.
Existing scenarios cover subscription APIs without receiving actual callbacks.

## Interface

Add `TestSNSHTTPDelivery` using the existing `-record` mode.
Recording requires `-sns-http-public-url` and `-sns-http-listen`.
The public HTTPS URL forwards only to a dedicated callback listener.
Local replay creates its own loopback HTTPS receiver and ignores public settings.
A small target-construction helper may enable TLS verification bypass only for
that receiver's test certificate. Existing tests retain their current defaults.

## Plan

1. Add a bounded callback receiver with unique per-case paths and configurable
   response sequences. Accept POST only, limit request bodies and callback count,
   and close the server with test cleanup. Expose no public control API.
2. Reuse the target signer, unique resource names, cleanup helpers, and golden
   read/write utilities. Keep callback comparison code in focused test files.
   Record normalized callbacks and relevant API results. Never store tokens.
3. Verify both signature versions before normalization. Fetch certificates only
   from the expected AWS regional host or the local target, without redirects.
   Check notification IDs against Publish, topic/subscription header relationships,
   timestamps, confirmation tokens, and trusted link destinations in memory.
4. Cover signed and unsigned confirmation, envelope headers and fields, subject
   omission, raw delivery, typed attributes, HTTPS-specific JSON messages,
   and matching/excluded filter-policy messages.
5. Cover success, permanent 400, redirect, 429, and 503 responses. Use a short
   custom delivery policy and an SQS dead-letter queue to observe exhaustion.
   Check attempt counts and recovery. Measure AWS timing, then set explicit
   broad tolerances for jitter and transport delay; never compare exact times.
6. Verify unsigned unsubscribe links, authenticated unsubscribe rejection, and
   signed removal. Check unsubscribe confirmation and restoration when AWS sends it.
7. Record live AWS fixtures through a temporary HTTPS tunnel to loopback.
   Fix demonstrated pail differences with regression tests. Never hand-edit
   recordings or hide meaningful content behind normalization.
8. Update README, AGENTS, compatibility and limitations docs, and test/diff/README.
   Document commands, receiver isolation, verified coverage, and remaining gaps.

## Gates

Base: `02d064645b823f5197c6a96d19aa2f6b1de41342`. Baseline race tests passed.
Use scoped formatting/modernization, lint, full race tests, a CGO-free build,
govulncheck, live recording, local replay, independent review, and green CI.
Use deterministic unit tests for scheduler changes. The live comparison must
observe real network timing, like the existing AWS differential suite.

## Risks and non-goals

Keep credentials, confirmation tokens, and raw certificate signatures out of
fixtures and failure logs. Local replay must never contact the public receiver.
Only temporary pail-diff resources and the disposable receiver may be changed.
Do not change existing DNS or services. The tunnel ends after testing.
No default-policy multi-day retry test, IAM/KMS enforcement, or SQS delay/retention fixes.
Live callbacks use HTTPS; existing local tests cover the shared HTTP delivery path.

## Recorded evidence

Eleven AWS callback scenarios recorded 155 exchanges. Signatures and identifier
relationships passed before normalization. Restoration preserved the subscription ARN.
AWS retained String, Number, and Binary attributes with HTTPS JSON selection.
The recordings exposed a missing subscription ARN header on pail's unsubscribe
confirmation. A regression failed for both signature versions before the fix.
It also showed that confirmations honor the configured request content type;
a failing policy regression now covers that correction. Subscription attributes
are compared as a protocol map, retaining every key and value.

Observed retry elapsed times reached 37.151 seconds for the short custom policy.
Permanent 400, 403, and 302 responses produced one callback and no dead-letter
message within 100 seconds, including a probe started 20 seconds after confirmation.
Keep pail's immediate permanent-error redrive and document this unverified AWS
arrival behavior. Bounded nonarrival does not prove AWS never redrives.

The temporary receivers and tunnels were stopped after recording. The downloaded
tunnel client was removed. Local replay needs no public service or AWS credentials.

Review found two missing lifecycle assertions. Both confirmation paths now check
that Subscribe and ConfirmSubscription return the same ARN. Both unsubscribe
paths check that signed removal returns NotFound on a subsequent attribute read
and produces no callbacks during a five-second observation window.
