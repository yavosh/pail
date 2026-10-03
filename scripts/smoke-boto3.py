#!/usr/bin/env python3
"""Runs boto3 with its default settings against a running pail.

Needs SMOKE_ENDPOINT, SMOKE_ACCESS_KEY, and SMOKE_SECRET_KEY. Uses no real AWS setup.
"""
import hashlib
import os
import sys
import tempfile
import urllib.error
import urllib.request

# Isolate from any real AWS setup before boto3 reads the environment.
for name in (
    "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
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
    s3.put_object(Bucket=bucket, Key="small.txt", Body=small)
    got = s3.get_object(Bucket=bucket, Key="small.txt")["Body"].read()
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

# boto3 presigns with SigV2 by default, which pail does not support. Use SigV4,
# as the AWS CLI does.
presigner = boto3.client(
    "s3", endpoint_url=endpoint, region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

with Step("generate_presigned_url"):
    url = presigner.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "small.txt"}
    )
    with urllib.request.urlopen(url) as resp:  # noqa: S310
        check(resp.read() == small, "presigned object differs")

with Step("head_object"):
    length = s3.head_object(Bucket=bucket, Key="small.txt")["ContentLength"]
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

tmp.cleanup()
