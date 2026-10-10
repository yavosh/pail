#!/usr/bin/env bash
# Runs the AWS CLI against a running pail. Needs SMOKE_ENDPOINT, SMOKE_ACCESS_KEY,
# and SMOKE_SECRET_KEY. Never uses real AWS credentials or profiles.
set -euo pipefail

: "${SMOKE_ENDPOINT:?set SMOKE_ENDPOINT}"
: "${SMOKE_ACCESS_KEY:?set SMOKE_ACCESS_KEY}"
: "${SMOKE_SECRET_KEY:?set SMOKE_SECRET_KEY}"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Isolate from any real AWS setup: smoke keys, temp config files, no profile.
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_SESSION_TOKEN AWS_SECURITY_TOKEN \
  AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL_SQS AWS_ENDPOINT_URL_SNS AWS_CA_BUNDLE AWS_REGION AWS_CLI_AUTO_PROMPT \
  AWS_REQUEST_CHECKSUM_CALCULATION AWS_RESPONSE_CHECKSUM_VALIDATION \
  HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy
export AWS_EC2_METADATA_DISABLED=true
export AWS_ACCESS_KEY_ID=$SMOKE_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$SMOKE_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1
export AWS_CONFIG_FILE=$work/aws-config
export AWS_SHARED_CREDENTIALS_FILE=$work/aws-credentials
printf '[default]\nregion = us-east-1\n' >"$AWS_CONFIG_FILE"
printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$SMOKE_ACCESS_KEY" "$SMOKE_SECRET_KEY" >"$AWS_SHARED_CREDENTIALS_FILE"

aws --version

# run echoes the command, then runs it. On failure it prints the CLI's error
# (with the S3 error code) and exits. Standard output stays on standard output.
run() {
  echo "+ $*" >&2
  if ! "$@" 2>"$work/stderr"; then
    echo "FAILED: $*" >&2
    cat "$work/stderr" >&2
    exit 1
  fi
}

# s3, s3api, sqs, and sns run the aws CLI against pail; every call passes --endpoint-url.
s3() { run aws --endpoint-url "$SMOKE_ENDPOINT" s3 "$@"; }
s3api() { run aws --endpoint-url "$SMOKE_ENDPOINT" s3api "$@"; }
sqs() { run aws --endpoint-url "$SMOKE_ENDPOINT" sqs "$@"; }
sns() { run aws --endpoint-url "$SMOKE_ENDPOINT" sns "$@"; }

fail() {
  echo "FAILED: $*" >&2
  exit 1
}

bucket=smoke-cli-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
echo "bucket: $bucket"

s3 mb "s3://$bucket"

# Small object, up and back.
echo "hello from the smoke test" >"$work/small.txt"
s3 cp "$work/small.txt" "s3://$bucket/small.txt" --no-progress
s3 cp "s3://$bucket/small.txt" "$work/small.back" --no-progress
cmp "$work/small.txt" "$work/small.back" || fail "small object differs after round trip"

# 20 MiB goes multipart with the CLI default threshold of 8 MB.
head -c 20971520 /dev/urandom >"$work/big.bin"
s3 cp "$work/big.bin" "s3://$bucket/big.bin" --no-progress
s3 cp "s3://$bucket/big.bin" "$work/big.back" --no-progress
cmp "$work/big.bin" "$work/big.back" || fail "20 MiB object differs after round trip"

listing=$(s3 ls "s3://$bucket/")
grep -q ' small.txt$' <<<"$listing" || fail "s3 ls does not list small.txt"
grep -q ' big.bin$' <<<"$listing" || fail "s3 ls does not list big.bin"

# Sync a tree with a nested path up, then down, and compare.
mkdir -p "$work/tree/nested/deeper" "$work/tree-back"
echo one >"$work/tree/one.txt"
echo two >"$work/tree/two.txt"
echo three >"$work/tree/nested/three.txt"
echo four >"$work/tree/nested/deeper/four.txt"
s3 sync "$work/tree" "s3://$bucket/synced/" --no-progress
s3 sync "s3://$bucket/synced/" "$work/tree-back" --no-progress
diff -r "$work/tree" "$work/tree-back" || fail "synced tree differs"

# Presigned GET, fetched without the CLI.
url=$(s3 presign "s3://$bucket/small.txt")
curl -fsS -o "$work/small.presigned" "$url" || fail "presigned GET failed: $url"
cmp "$work/small.txt" "$work/small.presigned" || fail "presigned object differs"

length=$(s3api head-object --bucket "$bucket" --key small.txt --query ContentLength --output text)
want=$(wc -c <"$work/small.txt" | tr -d ' ')
[ "$length" = "$want" ] || fail "head-object ContentLength is $length, want $want"

s3api copy-object --bucket "$bucket" --key copy.txt --copy-source "$bucket/small.txt"
length=$(s3api head-object --bucket "$bucket" --key copy.txt --query ContentLength --output text)
[ "$length" = "$want" ] || fail "copy ContentLength is $length, want $want"

# Bucket settings and ACL grants through the CLI's default checksum settings.
s3api put-bucket-cors --bucket "$bucket" --cors-configuration \
  '{"CORSRules":[{"AllowedOrigins":["https://app.example.com"],"AllowedMethods":["PUT","POST"],"AllowedHeaders":["*"]}]}'
s3api get-bucket-cors --bucket "$bucket"
s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration \
  '{"Rules":[{"ID":"temporary","Status":"Enabled","Filter":{"Prefix":"tmp/"},"Expiration":{"Days":7}}]}'
s3api get-bucket-lifecycle-configuration --bucket "$bucket"
s3api get-bucket-acl --bucket "$bucket"
s3api put-object-acl --bucket "$bucket" --key small.txt --grant-read \
  'uri="http://acs.amazonaws.com/groups/global/AllUsers"'
curl -fsS -o "$work/small.public" "$SMOKE_ENDPOINT/$bucket/small.txt"
cmp "$work/small.txt" "$work/small.public" || fail "public object differs"
s3api put-object-acl --bucket "$bucket" --key small.txt --acl private
s3api delete-bucket-cors --bucket "$bucket"
s3api delete-bucket-lifecycle --bucket "$bucket"

s3 rm "s3://$bucket" --recursive
# shellcheck disable=SC2016
left=$(s3api list-objects-v2 --bucket "$bucket" --query 'length(Contents || `[]`)' --output text)
[ "$left" = "0" ] || fail "$left objects left after rm --recursive"

s3 rb "s3://$bucket"

# SQS: one message through a queue, with the CLI's default settings.
queue=smoke-cli-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
sqs create-queue --queue-name "$queue"
queue_url=$(sqs get-queue-url --queue-name "$queue" --query QueueUrl --output text)
sqs send-message --queue-url "$queue_url" --message-body "hello from the smoke test"
received=$(sqs receive-message --queue-url "$queue_url" --wait-time-seconds 1 \
  --query 'Messages[0].[Body,ReceiptHandle]' --output text)
IFS=$'\t' read -r body handle <<<"$received"
[ "$body" = "hello from the smoke test" ] || fail "received message body is '$body'"
sqs delete-message --queue-url "$queue_url" --receipt-handle "$handle"
timeout=$(sqs get-queue-attributes --queue-url "$queue_url" --attribute-names All \
  --query Attributes.VisibilityTimeout --output text)
[ "$timeout" = "30" ] || fail "VisibilityTimeout is $timeout, want 30"
sqs delete-queue --queue-url "$queue_url"

# SNS: publish to a topic with an SQS subscription, with the CLI's default settings.
name=smoke-cli-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
topic_arn=$(sns create-topic --name "$name" --query TopicArn --output text)
queue_url=$(sqs create-queue --queue-name "$name" --query QueueUrl --output text)
queue_arn=$(sqs get-queue-attributes --queue-url "$queue_url" --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)
sns subscribe --topic-arn "$topic_arn" --protocol sqs --notification-endpoint "$queue_arn"
sns publish --topic-arn "$topic_arn" --message hello
body=$(sqs receive-message --queue-url "$queue_url" --wait-time-seconds 1 \
  --query 'Messages[0].Body' --output text)
case $body in
  '{'*'"Type":"Notification"'*'"Message":"hello"'*) ;;
  *) fail "SNS delivery body is '$body'" ;;
esac
sns delete-topic --topic-arn "$topic_arn"
sqs delete-queue --queue-url "$queue_url"
