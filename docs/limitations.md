# Limitations

This page lists what pail doesn't do. For setup and a summary, see the [README](../README.md).

pail isn't a production object store:

- It has no clustering, replication, or multi-tenant support.
- It serves one access key pair and one region.
- S3 event notifications go to SQS queues and SNS topics only, not to Lambda or EventBridge. See [Event notifications](s3-compatibility.md#event-notifications).
- It has no MFA Delete, bucket policies, or storage tiers. It stores and returns encryption, storage class, and website redirect settings, but it doesn't encrypt, archive, or lock anything.
- Unsupported S3 operations fail with `501 NotImplemented`.
- SQS messages live in memory and are lost when pail restarts. Queue definitions persist.
- SQS FIFO queues don't model high-throughput quotas, and `ReceiveRequestAttemptId` is ignored. Dead-letter queues move a message on the next receive, not in the background.
- SQS uses the AWS JSON protocol only, which current SDKs and the AWS CLI send. It doesn't support the legacy query protocol.
- SNS delivers to SQS queues and to HTTP and HTTPS endpoints. It supports subscription filter policies on message attributes and on the message body. It has no FIFO topics, SMS, email, Lambda, or mobile push.
- SNS signs notifications with pail's own certificate, which pail serves at `/_pail/sns/signing-cert.pem`. A verifier that requires an `amazonaws.com` host rejects it.
