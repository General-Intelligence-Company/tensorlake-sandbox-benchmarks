"""Refresh the private, read-only S3 network probe without printing its URL."""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.request

FIXTURE_DIR = Path(__file__).resolve().parent
PROFILE = "YOUR_AWS_PROFILE"
BUCKET = "YOUR_TEST_BUCKET"
OBJECT_KEY = "library-test/tensorlake/readme.txt"
EXPECTED_SHA256 = "939084d46e7d5ef62dc63130b6580dbeb96c847333cf2d9a836f6b46649b14af"


def run_aws_cli(*args):
    result = subprocess.run(
        ["aws", *args, "--profile", PROFILE],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def refresh_network_probe():
    # Verify the configured AWS account before generating temporary read
    # access. The resulting signed URL must remain private.
    identity = json.loads(run_aws_cli("sts", "get-caller-identity", "--output", "json"))
    assert identity["Account"] == "YOUR_AWS_ACCOUNT_ID", "Wrong fixture account"
    credentials = json.loads(
        run_aws_cli("configure", "export-credentials", "--format", "process")
    )
    generated_at = dt.datetime.now(dt.timezone.utc)
    credential_expiry = dt.datetime.fromisoformat(
        credentials["Expiration"].replace("Z", "+00:00")
    )
    if (credential_expiry - generated_at).total_seconds() < 1200:
        raise RuntimeError(
            "Shared AWS credentials have under 20 minutes remaining; refresh SSO first"
        )
    # Use the same seed for HTTPS and mounted reads. The dual-stack S3
    # endpoint also permits a separate forced IPv6 probe.
    url = run_aws_cli(
        "s3",
        "presign",
        f"s3://{BUCKET}/{OBJECT_KEY}",
        "--region",
        "us-east-1",
        "--endpoint-url",
        "https://s3.dualstack.us-east-1.amazonaws.com",
        "--expires-in",
        "3600",
    )
    with urllib.request.urlopen(url, timeout=30) as response:
        body = response.read()
        status = response.status
    assert status == 200 and hashlib.sha256(body).hexdigest() == EXPECTED_SHA256
    # Write the signed URL with owner-only permissions. Its actual lifetime
    # cannot exceed the expiration of the signing AWS credentials.
    target = FIXTURE_DIR / "probe-private.json"
    file_descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(file_descriptor, 0o600)
    with os.fdopen(file_descriptor, "w") as stream:
        json.dump(
            {
                "url": url,
                "generated_at": generated_at.isoformat(),
                "expires_at": min(
                    generated_at + dt.timedelta(hours=1), credential_expiry
                ).isoformat(),
                "expected_sha256": EXPECTED_SHA256,
                "expected_bytes": len(body),
                "expected_status": 200,
            },
            stream,
            indent=2,
        )
    print(
        json.dumps(
            {
                "probe_file": str(target),
                "expires_at": credential_expiry.isoformat(),
                "read_verified": True,
            }
        )
    )


if __name__ == "__main__":
    refresh_network_probe()
