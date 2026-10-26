"""Exercise the actual verifier without importing cloud/configuration setup."""

import ast
import asyncio
import concurrent.futures
import hashlib
import io
import random
import time
from pathlib import Path
from types import SimpleNamespace
from botocore.exceptions import (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
    ResponseStreamingError,
    ClientError,
)

controller_source = ast.parse(Path(__file__).with_name("run_benchmark.py").read_text())
verify_method = next(
    n
    for c in controller_source.body
    if isinstance(c, ast.ClassDef) and c.name == "Runner"
    for n in c.body
    if isinstance(n, ast.AsyncFunctionDef) and n.name == "verify_s3_write"
)
verify_namespace = {
    **globals(),
    "FIXTURE_CONFIG": {"bucket": "fixture"},
    "utc_now": lambda: "test",
}
exec(
    compile(
        ast.Module(body=[verify_method], type_ignores=[]), "run_benchmark.py", "exec"
    ),
    verify_namespace,
)


async def check_s3_read_retries():
    data = b"fixture"
    expected_write = {
        "key": "fixture/key",
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    dropped_connection = ConnectionClosedError(endpoint_url="https://example.invalid")
    permission_error = ClientError(
        {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"RequestId": "denied"},
        },
        "GetObject",
    )
    throttle_error = ClientError(
        {
            "Error": {"Code": "SlowDown"},
            "ResponseMetadata": {"RequestId": "slow", "HTTPStatusCode": 503},
        },
        "GetObject",
    )
    throttle_without_metadata = ClientError(
        {
            "Error": {"Code": "SlowDown", "Message": "original throttle"},
            "ResponseMetadata": {},
        },
        "GetObject",
    )
    throttle_without_metadata.response["ResponseMetadata"] = None
    denial_without_error_fields = ClientError(
        {
            "Error": {"Code": "AccessDenied", "Message": "original denial"},
            "ResponseMetadata": {},
        },
        "GetObject",
    )
    denial_without_error_fields.response["Error"] = None
    for sequence, expected_attempts, expected_passed, expected_recovered in [
        ([dropped_connection, data], 2, True, True),
        ([dropped_connection] * 3, 3, False, False),
        ([permission_error], 1, False, False),
        ([throttle_error, data], 2, True, True),
        ([throttle_error, throttle_error, data], 3, True, True),
        ([throttle_error] * 3, 3, False, False),
        ([dropped_connection, throttle_error, data], 3, True, True),
        ([throttle_without_metadata, data], 2, True, True),
        ([denial_without_error_fields], 1, False, False),
        ([b"wrong"], 1, False, False),
        ([data], 1, True, False),
    ]:
        get_requests = []
        opened_bodies = []
        recorded_logs = []

        def get_object(**kw):
            value = sequence[len(get_requests)]
            get_requests.append(kw)
            if isinstance(value, Exception):
                raise value
            body = io.BytesIO(value)
            opened_bodies.append(body)
            return {"Body": body, "ResponseMetadata": {"RequestId": "ok"}}

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            verifier = SimpleNamespace(
                s3_slots=asyncio.Semaphore(1),
                active_s3_reads=0,
                s3_read=SimpleNamespace(get_object=get_object),
                s3_executor=pool,
                redact_secrets=lambda e: type(e).__name__ + ": " + str(e),
                append_log=lambda *a: recorded_logs.append(a),
            )
            verification = await verify_namespace["verify_s3_write"](
                verifier, expected_write, {"case": "test", "index": 1}
            )
        assert (
            len(get_requests) == expected_attempts
            and len(verification["attempts"]) == expected_attempts
        )
        assert (
            verification["passed"] == expected_passed
            and verification["recovered"] == expected_recovered
        )
        assert verification["first_attempt_passed"] == (sequence[0] == data)
        assert (
            all(b.closed for b in opened_bodies)
            and verifier.active_s3_reads == 0
            and len(recorded_logs) == 1
        )
        if isinstance(sequence[0], Exception):
            assert verification["attempts"][0]["error"].endswith(str(sequence[0]))
        for a in verification["attempts"]:
            if a["retry_reason"] == "s3_slowdown":
                assert (
                    a["error_code"] == "SlowDown"
                    and 0 <= a["retry_delay_s"] <= 0.5 * 2 ** (a["attempt"] - 1) + 0.1
                )
        if sequence[0] is throttle_error:
            assert (
                verification["attempts"][0]["http_status"] == 503
                and verification["attempts"][0]["request_id"] == "slow"
            )
        if sequence == [throttle_error] * 3:
            assert verification["exhausted"]
    print(
        "Transport/SlowDown recovery, exhaustion, original errors with None metadata, non-retry permission/mismatch and body closure passed; no cloud calls."
    )


if __name__ == "__main__":
    asyncio.run(check_s3_read_retries())
