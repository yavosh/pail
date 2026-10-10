# SNS and SQS follow-ups

## Goal

Close the PR 8 leftovers and the SNS and SQS behavior that pail still implements from the AWS documentation only. Each item ends as a recorded scenario in `test/diff` or as a documented difference.

## PR 1: delivery policies, token expiry, and unsubscribe confirmation

- Validate and store `DeliveryPolicy` on topics and subscriptions, as recorded in `sns-delivery-policy`.
- Apply the effective policy to HTTP and HTTPS deliveries: retry schedule, throttle, and `Content-Type`.
- Expire pending subscriptions and their tokens after 3 days.
- Post an `UnsubscribeConfirmation` after an unsigned `Unsubscribe` of an HTTP or HTTPS subscription. A visit to its `SubscribeURL` restores the subscription.
- Add `--sns-tls-skip-verify` for local HTTPS endpoints with self-signed certificates.

## PR 2: record the unverified SQS and SNS error cases

Status: done in this branch. The `sqs-edge-cases` and `sns-edge-cases` recordings pass with no known differences. `ConfirmSubscription` on a confirmed subscription and `Subscribe` with a `DeliveryPolicy` on an SQS endpoint stay unverified; no scenario records them.

- Record the cases that the compatibility page marks as unverified: error codes and messages, `Subscribe` with a `DeliveryPolicy`, `ConfirmSubscription` on a confirmed subscription, and attribute positions.
- Fix pail where a recording differs, or list the difference in `known-diffs.txt`.

## PR 3: verify HTTP delivery against AWS

- Run a capture endpoint inside the AWS account. The endpoint records each request that SNS sends, so the golden files never hold a public address.
- Compare the confirmation, notification, and `UnsubscribeConfirmation` bodies, the headers, the retry timing, and the content types.
- This PR needs the maintainer's approval before it creates any IAM or Lambda resource.
