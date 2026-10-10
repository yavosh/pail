#!/usr/bin/env python3
"""Runs boto3 with its default settings against a running pail.

Needs SMOKE_ENDPOINT, SMOKE_ACCESS_KEY, and SMOKE_SECRET_KEY. Uses no real AWS setup.
"""
import base64
import hashlib
import os
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request

# Isolate from any real AWS setup before boto3 reads the environment.
for name in (
    "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT_URL_SQS", "AWS_ENDPOINT_URL_SNS",
    "AWS_CA_BUNDLE", "AWS_REGION", "AWS_REQUEST_CHECKSUM_CALCULATION",
    "AWS_RESPONSE_CHECKSUM_VALIDATION", "HTTP_PROXY", "HTTPS_PROXY",
    "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy",
):
    os.environ.pop(name, None)

endpoint = os.environ["SMOKE_ENDPOINT"]
tmp = tempfile.TemporaryDirectory()
config_file = os.path.join(tmp.name, "aws-config")
credentials_file = os.path.join(tmp.name, "aws-credentials")
open(config_file, "w").close()
open(credentials_file, "w").close()
os.environ.update(
    AWS_ACCESS_KEY_ID=os.environ["SMOKE_ACCESS_KEY"],
    AWS_SECRET_ACCESS_KEY=os.environ["SMOKE_SECRET_KEY"],
    AWS_DEFAULT_REGION="us-east-1",
    AWS_EC2_METADATA_DISABLED="true",
    AWS_CONFIG_FILE=config_file,
    AWS_SHARED_CREDENTIALS_FILE=credentials_file,
)

import boto3  # noqa: E402
import botocore  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

print(f"boto3 {boto3.__version__}, botocore {botocore.__version__}")

s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
sqs = boto3.client("sqs", endpoint_url=endpoint, region_name="us-east-1")
bucket = "smoke-py-" + os.urandom(4).hex()
print(f"bucket: {bucket}")

# Remember the parameters of the latest call, minus bodies, for the error report.
last_params = {}


def record_params(params, model, **kwargs):
    last_params.clear()
    last_params.update({k: v for k, v in params.items() if k != "Body"})


s3.meta.events.register("provide-client-params.s3.*", record_params)


class Step:
    """Names the failing step and prints the S3 error of a ClientError."""

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        print(f"+ {self.name}", flush=True)

    def __exit__(self, exc_type, exc, tb):
        if exc is None:
            return False
        print(f"FAILED step: {self.name}", file=sys.stderr)
        if isinstance(exc, ClientError):
            meta = exc.response.get("ResponseMetadata", {})
            err = exc.response.get("Error", {})
            print(f"  operation:  {exc.operation_name}", file=sys.stderr)
            print(f"  parameters: {last_params}", file=sys.stderr)
            print(f"  error code: {err.get('Code')}", file=sys.stderr)
            print(f"  message:    {err.get('Message')}", file=sys.stderr)
            print(f"  request id: {meta.get('RequestId')}", file=sys.stderr)
            print(f"  status:     {meta.get('HTTPStatusCode')}", file=sys.stderr)
        elif isinstance(exc, urllib.error.HTTPError):
            print(f"  {exc.url}: HTTP {exc.code}", file=sys.stderr)
            print(f"  {exc.read().decode(errors='replace')}", file=sys.stderr)
        else:
            print(f"  {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check(ok, message):
    if not ok:
        raise AssertionError(message)


small = b"hello from the smoke test\n"

with Step("create_bucket"):
    s3.create_bucket(Bucket=bucket)

with Step("put_object and get_object"):
    s3.put_object(Bucket=bucket, Key="small.txt", Body=small, Metadata={"purpose": "smoke"})
    response = s3.get_object(Bucket=bucket, Key="small.txt")
    check(response["Metadata"] == {"purpose": "smoke"}, f"unexpected metadata: {response['Metadata']}")
    got = response["Body"].read()
    check(got == small, "small object differs after round trip")

big = os.path.join(tmp.name, "big.bin")
big_back = os.path.join(tmp.name, "big.back")
with open(big, "wb") as f:
    f.write(os.urandom(20 * 1024 * 1024))

# The default TransferConfig switches to multipart above 8 MB.
with Step("upload_file 20 MiB"):
    s3.upload_file(big, bucket, "big.bin")

with Step("download_file 20 MiB"):
    s3.download_file(bucket, "big.bin", big_back)
    check(sha256_file(big) == sha256_file(big_back), "20 MiB object differs")

with Step("list_objects_v2"):
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])}
    check({"small.txt", "big.bin"} <= keys, f"missing keys in listing: {keys}")

# Stands in for sync: upload a tree under a prefix and list it.
tree = {
    "synced/one.txt": b"one\n",
    "synced/two.txt": b"two\n",
    "synced/nested/three.txt": b"three\n",
    "synced/nested/deeper/four.txt": b"four\n",
}
with Step("upload a tree under a prefix and list it"):
    for key, body in tree.items():
        path = os.path.join(tmp.name, "tree-file")
        with open(path, "wb") as f:
            f.write(body)
        s3.upload_file(path, bucket, key)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="synced/")
    keys = {o["Key"] for o in resp.get("Contents", [])}
    check(keys == set(tree), f"unexpected keys under synced/: {keys}")

with Step("generate_presigned_url"):
    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "small.txt"}
    )
    with urllib.request.urlopen(url) as resp:  # noqa: S310
        check(resp.read() == small, "presigned object differs")

with Step("generate_presigned_url for put_object"):
    digest = base64.b64encode(hashlib.md5(small).digest()).decode()
    key = "sigv2/a b+c.txt"
    url = s3.generate_presigned_url(
        "put_object", Params={"Bucket": bucket, "Key": key,
                              "ContentType": "text/plain", "ContentMD5": digest,
                              "Metadata": {"purpose": "sigv2"}},
    )
    request = urllib.request.Request(
        url, data=small, method="PUT",
        headers={"Content-Type": "text/plain", "Content-MD5": digest,
                 "x-amz-meta-purpose": "sigv2"},
    )
    with urllib.request.urlopen(request) as resp:  # noqa: S310
        check(resp.status == 200, f"presigned PUT status is {resp.status}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    check(obj["Body"].read() == small, "presigned PUT object differs")
    obj["Body"].close()
    check(obj["ContentType"] == "text/plain", "presigned PUT content type differs")
    check(obj["Metadata"] == {"purpose": "sigv2"}, "presigned PUT metadata differs")

with Step("modified and expired presigned URLs"):
    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "small.txt"}
    )
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    check("AWSAccessKeyId" in query and "Signature" in query, "default presign is not SigV2")
    modified = url + "&response-content-type=text%2Fhtml"
    expired = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "small.txt"}, ExpiresIn=-60
    )
    for name, rejected in (("modified", modified), ("expired", expired)):
        try:
            with urllib.request.urlopen(rejected):  # noqa: S310
                raise AssertionError(f"{name} URL was accepted")
        except urllib.error.HTTPError as exc:
            check(exc.code == 403, f"{name} URL status is {exc.code}")
            exc.close()

with Step("bucket CORS and lifecycle configuration"):
    cors = [{"AllowedOrigins": ["https://app.example.com"],
             "AllowedMethods": ["GET", "PUT", "POST"],
             "AllowedHeaders": ["*"], "ExposeHeaders": ["ETag"]}]
    s3.put_bucket_cors(Bucket=bucket, CORSConfiguration={"CORSRules": cors})
    check(s3.get_bucket_cors(Bucket=bucket)["CORSRules"] == cors, "CORS rules differ")
    rules = [{"ID": "forms", "Status": "Enabled", "Filter": {"Prefix": "form/"},
              "Expiration": {"Days": 7}}]
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration={"Rules": rules})
    check(s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"] == rules,
          "lifecycle rules differ")

with Step("generate_presigned_post with ACL and CORS"):
    # Browser form policies still require SigV4.
    post_presigner = boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    fields = {"Content-Type": "text/plain", "acl": "public-read",
              "success_action_status": "201", "x-amz-meta-purpose": "form"}
    post = post_presigner.generate_presigned_post(
        Bucket=bucket, Key="form/${filename}", Fields=fields,
        Conditions=[{key: value} for key, value in fields.items()]
        + [["content-length-range", 1, 1024]],
    )
    boundary = "pail-" + os.urandom(16).hex()
    parts = []
    for name, value in post["fields"].items():
        parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"'
                      f'\r\n\r\n{value}\r\n').encode())
    parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
                  'filename="form.txt"\r\nContent-Type: text/plain\r\n\r\n').encode())
    parts.extend([small, f"\r\n--{boundary}--\r\n".encode()])
    request = urllib.request.Request(
        post["url"], data=b"".join(parts), method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}",
                 "Origin": "https://app.example.com"},
    )
    with urllib.request.urlopen(request) as resp:  # noqa: S310
        check(resp.status == 201, f"POST status is {resp.status}")
        check(resp.headers["Access-Control-Allow-Origin"] == "https://app.example.com",
              "POST CORS header differs")
    obj = s3.get_object(Bucket=bucket, Key="form/form.txt")
    check(obj["Body"].read() == small, "POST object differs")
    obj["Body"].close()
    check(obj["Metadata"] == {"purpose": "form"}, "POST metadata differs")
    check("Expiration" in obj, "POST object has no lifecycle expiration header")
    acl = s3.get_object_acl(Bucket=bucket, Key="form/form.txt")
    check(any(g["Grantee"].get("URI") == "http://acs.amazonaws.com/groups/global/AllUsers"
              and g["Permission"] == "READ" for g in acl["Grants"]), "POST ACL differs")
    with urllib.request.urlopen(f"{endpoint}/{bucket}/form/form.txt") as resp:  # noqa: S310
        check(resp.read() == small, "public object differs")
    s3.put_object_acl(Bucket=bucket, Key="form/form.txt", ACL="private")
    try:
        urllib.request.urlopen(f"{endpoint}/{bucket}/form/form.txt")  # noqa: S310
    except urllib.error.HTTPError as exc:
        check(exc.code == 403, f"private object status is {exc.code}")
    else:
        raise AssertionError("private object was readable anonymously")
    s3.delete_bucket_cors(Bucket=bucket)
    s3.delete_bucket_lifecycle(Bucket=bucket)

with Step("head_object"):
    head = s3.head_object(Bucket=bucket, Key="small.txt")
    check(head["Metadata"] == {"purpose": "smoke"}, f"unexpected metadata: {head['Metadata']}")
    length = head["ContentLength"]
    check(length == len(small), f"ContentLength is {length}, want {len(small)}")

with Step("copy_object"):
    s3.copy_object(
        Bucket=bucket, Key="copy.txt",
        CopySource={"Bucket": bucket, "Key": "small.txt"},
    )
    length = s3.head_object(Bucket=bucket, Key="copy.txt")["ContentLength"]
    check(length == len(small), f"copy ContentLength is {length}, want {len(small)}")

with Step("delete_objects"):
    resp = s3.list_objects_v2(Bucket=bucket)
    objects = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
    out = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    check(not out.get("Errors"), f"delete_objects errors: {out.get('Errors')}")
    check(len(out.get("Deleted", [])) == len(objects), "not every key deleted")
    check(s3.list_objects_v2(Bucket=bucket)["KeyCount"] == 0, "bucket not empty")

with Step("delete_bucket"):
    s3.delete_bucket(Bucket=bucket)

queue_name = "smoke-py-" + os.urandom(4).hex()
queue_url = None

with Step("sqs create_queue, send_message, and receive_message"):
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    sqs.send_message(
        QueueUrl=queue_url, MessageBody="hello from the smoke test",
        MessageAttributes={"color": {"DataType": "String", "StringValue": "blue"}},
    )
    messages = sqs.receive_message(
        QueueUrl=queue_url, MessageAttributeNames=["All"], WaitTimeSeconds=1,
    ).get("Messages", [])
    check(len(messages) == 1, f"received {len(messages)} messages, want 1")
    message = messages[0]
    check(message["Body"] == "hello from the smoke test", f"unexpected body: {message['Body']}")
    check(message["MessageAttributes"]["color"]["StringValue"] == "blue",
          f"unexpected attributes: {message['MessageAttributes']}")

with Step("sqs delete_message"):
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])

with Step("sqs send_message_batch"):
    out = sqs.send_message_batch(QueueUrl=queue_url, Entries=[
        {"Id": "a", "MessageBody": "one"},
        {"Id": "b", "MessageBody": "two"},
    ])
    check(len(out.get("Successful", [])) == 2 and not out.get("Failed"), f"batch result: {out}")

with Step("sqs delete_queue"):
    sqs.delete_queue(QueueUrl=queue_url)

tmp.cleanup()
