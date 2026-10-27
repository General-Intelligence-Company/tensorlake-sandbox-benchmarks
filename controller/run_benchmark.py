# Run the scripted VM lifecycle benchmark. The controller owns timing and
# outside-S3 verification; context/sandbox_workload.py owns the work inside each VM.

import asyncio
import concurrent.futures
import hashlib
import json
import os
import queue
import random
import resource
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import boto3
import httpx
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
    ResponseStreamingError,
)
from tensorlake.sandbox import AsyncSandboxClient, SnapshotType, SandboxClient
from tensorlake.sandbox.exceptions import SandboxNotFoundError

# Configure the local fixture and image before running the benchmark.
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs/benchmark"
FIXTURE = ROOT / "fixture"
FIXTURE_CONFIG = json.loads((FIXTURE / "manifest.json").read_text())
IMAGE = "tensorlake-benchmark-session"
# Sandbox count is VMs per case, not simultaneous requests. Separate limits bound
# launches, controller I/O and outside-S3 verification.
SANDBOX_COUNT = 1000
IDLE_TIMEOUT_SECONDS = 7200
API_CONCURRENCY = 128
LAUNCH_CONCURRENCY = 64
S3_CONCURRENCY = 64


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json_atomically(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, default=str) + "\n")
    temp.replace(path)


# Spread guest-control traffic across connection pools so a single pool
# cannot serialize observations of otherwise independent VMs.
def create_control_clients():
    return [
        httpx.AsyncClient(
            headers={"Authorization": "Bearer " + os.environ["TENSORLAKE_API_KEY"]},
            limits=httpx.Limits(
                max_connections=64, max_keepalive_connections=64, keepalive_expiry=120
            ),
            timeout=30,
            follow_redirects=False,
        )
        for _ in range(32)
    ]


# Use the configured AWS profile instead of inherited AWS environment
# variables, and verify its account before reads or run-prefix cleanup.
def load_credentials(refresh_probe=True):
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    if refresh_probe:
        p = subprocess.run(
            ["python3", str(FIXTURE / "prepare_network_probe.py")],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if p.returncode:
            raise RuntimeError("Approved fixture refresh failed: " + p.stderr[-500:])
    d = json.loads(
        subprocess.check_output(
            [
                "aws",
                "configure",
                "export-credentials",
                "--profile",
                FIXTURE_CONFIG["aws_profile_for_shared_fixture"],
                "--format",
                "process",
            ],
            env=env,
        )
    )
    aws = dict(
        zip(
            ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"],
            [d["AccessKeyId"], d["SecretAccessKey"], d["SessionToken"]],
        )
    )
    probe = json.loads((FIXTURE / "probe-private.json").read_text())
    aws["N100_PROBE_URL"] = probe["url"]
    session = boto3.Session(
        aws_access_key_id=d["AccessKeyId"],
        aws_secret_access_key=d["SecretAccessKey"],
        aws_session_token=d["SessionToken"],
        region_name=FIXTURE_CONFIG["region"],
    )
    # One total attempt preserves AWS throttling as an observed error.
    # A failed outside read does not itself establish a guest mount failure.
    s3 = session.client(
        "s3", config=Config(max_pool_connections=128, retries={"total_max_attempts": 1})
    )
    # Only independent GET verification may replay transport failures/SlowDown.
    # Keep mutations single-attempt; short socket timeouts bound each read.
    s3_read = session.client(
        "s3",
        config=Config(
            max_pool_connections=128,
            connect_timeout=2,
            read_timeout=2,
            retries={"total_max_attempts": 1},
        ),
    )
    assert (
        session.client("sts").get_caller_identity()["Account"]
        == FIXTURE_CONFIG["expected_aws_account"]
    )
    return aws, s3, s3_read, d["Expiration"], probe["expires_at"]


class Runner:
    # One run owns its evidence, provider connections and disposable resources.
    # Each trial keeps its own timestamps and outcome in the persisted state.
    def __init__(self, state, refresh_probe=True):
        self.state = state
        self.results_dir = OUT / state["run_id"]
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.guest_env, self.s3, self.s3_read, expiry, probe_expiry = load_credentials(
            refresh_probe
        )
        self.state.setdefault("credential_observations", []).append(
            {
                "at": utc_now(),
                "expiry": expiry,
                "probe_expiry": probe_expiry,
                "probe_refreshed": refresh_probe,
            }
        )
        self.secrets = list(self.guest_env.values()) + [
            os.environ["TENSORLAKE_API_KEY"]
        ]
        self.control_clients = create_control_clients()
        self.client = AsyncSandboxClient(
            api_key=os.environ["TENSORLAKE_API_KEY"],
            max_retries=0,
            request_timeout=180,
            _internal=True,
        )
        self.api_slots = asyncio.Semaphore(API_CONCURRENCY)
        self.launch_slots = asyncio.Semaphore(LAUNCH_CONCURRENCY)
        self.s3_slots = asyncio.Semaphore(S3_CONCURRENCY)
        self.s3_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=S3_CONCURRENCY
        )
        self.write_queue = queue.Queue()
        self.active_api_requests = 0
        self.active_s3_reads = 0
        self.finished = False
        self.completed_calls = 0
        self.owned_ids = set(state.get("owned_ids", []))
        self.sandboxes = {}
        self.evidence_writer = threading.Thread(target=self.write_evidence, daemon=True)
        self.evidence_writer.start()

    def redact_secrets(self, value):
        text = (
            type(value).__name__ + ": " + str(value)
            if isinstance(value, BaseException)
            else str(value)
        )
        for v in self.secrets:
            if v:
                text = text.replace(v, "[redacted]")
        return text

    # Evidence persistence
    # Write evidence off the async loop so saving large results does not
    # block requests and readiness observations for other VMs.
    def write_evidence(self):
        while True:
            kind, name, value = self.write_queue.get()
            try:
                p = self.results_dir / name
                p.parent.mkdir(parents=True, exist_ok=True)
                if kind == "json":
                    write_json_atomically(p, value)
                elif kind == "bytes":
                    p.write_bytes(value)
                else:
                    with p.open("a") as f:
                        f.write(json.dumps(value, default=str) + "\n")
            finally:
                self.write_queue.task_done()

    def append_log(self, name, value):
        self.write_queue.put(("line", name, value))

    def save_state(self):
        self.state["owned_ids"] = sorted(self.owned_ids)
        self.write_queue.put(("json", "state.json", self.state))
        write_json_atomically(OUT / "current.json", {"run_id": self.state["run_id"]})

    def save_trial(self, trial):
        self.write_queue.put(
            ("json", f"trials/{trial['case']}/{trial['index']}/result.json", trial)
        )

    # Provider requests and readiness
    # Record controller queue time separately from API time. For create/resume,
    # t0 is set immediately before dispatch; readiness shares this one origin.
    async def call_provider(self, kind, fn, trial=None):
        queued = time.monotonic()
        async with self.api_slots:
            begin = time.monotonic()
            self.active_api_requests += 1
            trace = None
            error = None
            if trial is not None and kind in ["create", "resume"]:
                trial["t0"] = begin
                trial["dispatch_at"] = utc_now()
            if trial is not None and kind == "suspend":
                trial["_suspend_dispatch_monotonic"] = begin
            try:
                result = await fn()
                trace = getattr(result, "trace_id", None)
                return result
            except Exception as e:
                error = self.redact_secrets(e)
                raise
            finally:
                self.active_api_requests -= 1
                self.completed_calls += 1
                self.append_log(
                    "controller-calls.jsonl",
                    {
                        "at": utc_now(),
                        "kind": kind,
                        "queue_s": begin - queued,
                        "duration_s": time.monotonic() - begin,
                        "case": trial and trial["case"],
                        "index": trial and trial["index"],
                        "trace_id": trace,
                        "error": error,
                    },
                )

    async def run_command(self, sandbox, args, timeout=180, trial=None):
        out = await asyncio.wait_for(
            self.call_provider(
                "command",
                lambda: sandbox.run(args[0], args[1:], timeout=timeout, user="root"),
                trial,
            ),
            timeout + 5,
        )
        self.append_log(
            "commands.jsonl",
            {
                "at": utc_now(),
                "case": trial and trial["case"],
                "index": trial and trial["index"],
                "argv": args,
                "exit_code": out.exit_code,
                "trace_id": out.trace_id,
                "stdout": self.redact_secrets(out.stdout[-3000:]),
                "stderr": self.redact_secrets(out.stderr[-3000:]),
            },
        )
        if out.exit_code:
            raise RuntimeError(
                "Command exit "
                + str(out.exit_code)
                + ": "
                + self.redact_secrets(out.stderr[-1600:])
                + " "
                + self.redact_secrets(out.stdout[-1600:])
            )
        return out.stdout

    # Prove a command runs in the expected image. Disk children must contain
    # the saved marker and source-installed dependency, not just return a new ID.
    async def wait_for_terminal(self, sandbox, trial, disk_marker=None):
        hashes = {
            name: hashlib.sha256((ROOT / "context" / name).read_bytes()).hexdigest()
            for name in [
                "run_sandbox_workload.py",
                "sandbox_workload.py",
                "manifest.json",
                "shared-app.bundle",
                "record_resource_usage.py",
            ]
        }
        code = (
            "import json,time,hashlib,os;"
            "from pathlib import Path;"
            "d=json.loads(__import__('sys').argv[1]);"
            "assert all(hashlib.sha256((Path('/opt/bench')/n).read_bytes()).hexdigest()==v for n,v in d.items());"
            "print(json.dumps({'guest_monotonic':time.monotonic(),"
            "'utc':time.time(),'cpu_count':os.cpu_count(),"
            "'memory':Path('/proc/meminfo').read_text(),"
            "'source_deps':Path('/workspace/source-app/node_modules/escape-html/package.json').exists(),"
            "'disk_marker':Path('/workspace/source-disk-marker').read_text() "
            "if Path('/workspace/source-disk-marker').exists() else None}))"
        )
        delay = 0.5
        errors = []
        deadline = trial["t0"] + 180
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("180-second terminal deadline; " + str(errors))
            try:
                begin = time.monotonic()
                value = json.loads(
                    await self.run_command(
                        sandbox,
                        ["python3", "-c", code, json.dumps(hashes)],
                        min(20, remaining),
                        trial,
                    )
                )
                end = time.monotonic()
                if end > deadline:
                    raise TimeoutError("Terminal after 180 seconds")
                if disk_marker is not None:
                    assert value["disk_marker"] == disk_marker, (
                        "Saved disk marker missing"
                    )
                    assert value.get("source_deps"), (
                        "Source-installed dependency missing"
                    )
                # Pair guest and controller clocks at this response. Its round-trip
                # time is retained as uncertainty when translating later guest times.
                trial["terminal_ready_s"] = end - trial["t0"]
                trial["clock_anchor"] = {
                    "guest": value["guest_monotonic"],
                    "controller": end,
                    "uncertainty_s": end - begin,
                }
                trial["guest_allocation"] = value
                trial["terminal_read_errors"] = errors
                return
            except Exception as e:
                if getattr(e, "status_code", None) in [401, 403]:
                    raise
                errors.append(self.redact_secrets(e))
                await asyncio.sleep(min(delay, max(0, deadline - time.monotonic())))
                delay = min(delay * 2, 5)

    # Send JSON through the authenticated provider proxy to the guest server.
    # The provider API key stays in the controller, outside the guest.
    async def request_guest(self, sandbox, payload, trial, timeout=180):
        url = "https://8123-" + sandbox.sandbox_id + ".sandbox.tensorlake.ai/"
        deadline = time.monotonic() + timeout + 5
        client = self.control_clients[
            int(hashlib.sha256(sandbox.sandbox_id.encode()).hexdigest()[:8], 16)
            % len(self.control_clients)
        ]

        async def fetch():
            response = await client.post(
                url,
                json=payload,
                timeout=min(timeout, max(0.1, deadline - time.monotonic())),
            )
            self.append_log(
                "control-responses.jsonl",
                {
                    "at": utc_now(),
                    "case": trial["case"],
                    "index": trial["index"],
                    "operation": payload["op"],
                    "status": response.status_code,
                    "request_id": response.headers.get("x-request-id"),
                    "trace_id": response.headers.get("x-trace-id"),
                },
            )
            response.raise_for_status()
            return response.json()

        # Progress/state are read-only observations. Mutating requests are not
        # replayed after an ambiguous transport error.
        for attempt in range(4):
            try:
                data = await asyncio.wait_for(
                    self.call_provider("control_http_" + payload["op"], fetch, trial),
                    max(0.1, deadline - time.monotonic()) + 5,
                )
                break
            except httpx.TransportError as e:
                if (
                    payload["op"] not in ["progress", "state"]
                    or attempt == 3
                    or time.monotonic() >= deadline
                ):
                    raise
                trial.setdefault("control_read_retries", []).append(
                    {
                        "at": utc_now(),
                        "op": payload["op"],
                        "attempt": attempt + 1,
                        "error": self.redact_secrets(e),
                    }
                )
                await asyncio.sleep(
                    min(0.5 * 2**attempt, max(0, deadline - time.monotonic()))
                )
        if not data.get("ok"):
            trial["guest_failure"] = data
            trial["steps"] = data.get("steps", [])
            raise RuntimeError("Guest " + json.dumps(data))
        return data["result"]

    # Start the scripted guest server once with process restart disabled.
    # Memory forks and resumes must reconnect to the saved process.
    async def start_guest_server(self, sandbox, trial):
        p = await self.call_provider(
            "agent_start",
            lambda: sandbox.start_process(
                "/opt/bench-venv/bin/python",
                ["-u", "/opt/bench/run_sandbox_workload.py"],
                env=self.guest_env,
                user="root",
                name="n1000-session",
                restart={"policy": "never"},
            ),
            trial,
        )
        trial["agent_pid"] = p.pid
        until = time.monotonic() + 180
        delay = 0.2
        while True:
            try:
                return await self.request_guest(sandbox, {"op": "progress"}, trial, 20)
            except Exception:
                if time.monotonic() >= until:
                    raise
                await asyncio.sleep(delay)
                delay = min(2, delay * 2)

    # Workload verification
    # Read the object directly from S3 outside the VM and compare length/hash.
    # A successful guest read could still come from its filesystem cache.
    async def verify_s3_write(self, data, trial):
        queued = time.monotonic()
        async with self.s3_slots:
            begin = time.monotonic()
            self.active_s3_reads += 1

            def get():
                attempts = []
                deadline = begin + 15
                for attempt in range(1, 4):
                    started = time.monotonic()
                    retry = None
                    try:
                        response = self.s3_read.get_object(
                            Bucket=FIXTURE_CONFIG["bucket"], Key=data["key"]
                        )
                        body = response["Body"]
                        try:
                            b = body.read()
                        finally:
                            body.close()
                        metadata = response.get("ResponseMetadata") or {}
                        value = {
                            "passed": hashlib.sha256(b).hexdigest() == data["sha256"]
                            and len(b) == data["bytes"],
                            "bytes": len(b),
                            "request_id": metadata.get("RequestId"),
                            "http_status": metadata.get("HTTPStatusCode"),
                            "error_code": None,
                        }
                    except Exception as e:
                        response = getattr(e, "response", None) or {}
                        metadata = response.get("ResponseMetadata") or {}
                        code = (response.get("Error") or {}).get("Code")
                        value = {
                            "passed": False,
                            "error": self.redact_secrets(e),
                            "request_id": metadata.get("RequestId"),
                            "http_status": metadata.get("HTTPStatusCode"),
                            "error_code": code,
                            "exception_chain": [],
                        }
                        cause = e
                        seen = set()
                        while cause is not None and id(cause) not in seen:
                            seen.add(id(cause))
                            value["exception_chain"].append(
                                {
                                    "type": type(cause).__name__,
                                    "error": self.redact_secrets(cause),
                                    "errno": getattr(cause, "errno", None),
                                }
                            )
                            cause = cause.__cause__ or cause.__context__
                        if isinstance(e, ClientError) and code == "SlowDown":
                            retry = "s3_slowdown"
                        elif isinstance(
                            e,
                            (
                                ConnectionClosedError,
                                ConnectTimeoutError,
                                EndpointConnectionError,
                                ReadTimeoutError,
                                ResponseStreamingError,
                            ),
                        ):
                            retry = "transport"
                    attempts.append(
                        {
                            "attempt": attempt,
                            "at": utc_now(),
                            "duration_s": time.monotonic() - started,
                            "retry_reason": retry,
                            "retry_delay_s": 0,
                            **value,
                        }
                    )
                    if value["passed"] or not retry or attempt == 3:
                        break
                    delay = (
                        random.uniform(0, 0.5) if retry == "s3_slowdown" else 0.25
                    ) * 2 ** (attempt - 1)
                    # Reserve the client's 2s connect + 2s read budget before
                    # replaying; keep the original failure if time is spent.
                    if time.monotonic() + delay + 4 > deadline:
                        attempts[-1]["retry_budget_exhausted"] = True
                        break
                    sleeping = time.monotonic()
                    time.sleep(delay)
                    attempts[-1]["retry_delay_s"] = time.monotonic() - sleeping
                return {
                    **value,
                    "attempts": attempts,
                    "first_attempt_passed": attempts[0]["passed"],
                    "recovered": value["passed"] and len(attempts) > 1,
                    "exhausted": not value["passed"]
                    and bool(retry)
                    and (
                        attempt == 3
                        or attempts[-1].get("retry_budget_exhausted", False)
                    ),
                }

            try:
                value = await asyncio.get_running_loop().run_in_executor(
                    self.s3_executor, get
                )
            finally:
                self.active_s3_reads -= 1
        value.update(
            at=utc_now(),
            epoch=time.time(),
            duration_s=time.monotonic() - begin,
            queue_s=begin - queued,
        )
        self.append_log(
            "s3-external.jsonl",
            {
                "case": trial["case"],
                "index": trial["index"],
                "key": data["key"],
                **value,
            },
        )
        return value

    # Complete the guest workload, translate its timestamps, then independently
    # verify its S3 write. Step durations and cumulative readiness stay separate.
    async def run_workload(self, sandbox, trial, mode):
        # 1. Ask the guest to run the ordered app/browser/S3 workload.
        result = await self.request_guest(
            sandbox,
            {
                "op": "setup",
                "case": trial["case"],
                "index": trial["index"],
                "run": self.state["run_id"],
                "marker": trial["marker"],
                "mode": mode,
            },
            trial,
            2400,
        )
        # 2. Translate guest completion timestamps using the terminal clock
        # anchor; duration_s still measures each individual operation.
        trial["steps"] = result["steps"]
        anchor = trial["clock_anchor"]
        for e in trial["steps"]:
            e["controller_elapsed_upper_s"] = (
                e["guest_monotonic"]
                - anchor["guest"]
                + anchor["controller"]
                - trial["t0"]
            )
        # 3. Finish after outside S3 verification. S3-ready and full-workflow
        # therefore share a timestamp in this workload.
        written = next(e["result"] for e in trial["steps"] if e["step"] == "s3_write")
        trial["external_s3_write"] = await self.verify_s3_write(written, trial)
        trial["s3_write_ready_s"] = (
            time.monotonic() - trial["t0"]
            if trial["external_s3_write"]["passed"]
            else None
        )
        trial["setup_passed"] = trial["external_s3_write"]["passed"]
        trial["full_workflow_s"] = trial["s3_write_ready_s"]
        trial["state_after_setup"] = result["state"]
        for stage, key in [
            ("local_write", "local_write_ready_s"),
            ("s3_read", "s3_read_ready_s"),
            ("app", "app_ready_s"),
            ("browser", "browser_ready_s"),
        ]:
            trial[key] = next(
                e["controller_elapsed_upper_s"]
                for e in trial["steps"]
                if e["step"] == stage
            )

    # Compare live process/browser/file state before edits, navigation or app
    # restarts. Then write through the existing mount without remounting it.
    async def verify_preserved_state(self, sandbox, trial, expected):
        observed = await self.request_guest(sandbox, {"op": "state"}, trial)
        trial["preservation"] = {
            "expected": expected,
            "actual": observed["state"],
            "passed": observed["state"] == expected,
        }
        trial["preservation_s"] = time.monotonic() - trial["t0"]
        if not trial["preservation"]["passed"]:
            raise RuntimeError("Untouched saved state differs")
        proof = await self.request_guest(
            sandbox,
            {
                "op": "proof_write",
                "run": self.state["run_id"],
                "case": trial["case"],
                "index": trial["index"],
            },
            trial,
        )
        trial["preserved_mount_write"] = proof["write"]
        trial["preserved_mount_verification"] = await self.verify_s3_write(
            proof["write"], trial
        )
        trial["preserved_s3_ready_s"] = time.monotonic() - trial["t0"]
        if not trial["preserved_mount_verification"]["passed"]:
            raise RuntimeError("Preserved mount write verification failed")
        return observed["state"]

    # Create a fresh VM or snapshot child and retain its result independently.
    # An API response containing a VM ID does not complete the workload.
    async def create_trial(self, case, index, snapshot=None, expected=None, mode=None):
        trial = {
            "case": case,
            "index": index,
            "marker": f"TL-{self.state['run_id']}-{case}-{index:04d}",
            "queued_at": utc_now(),
        }
        self.state.setdefault("cases", {}).setdefault(case, []).append(trial)
        sandbox = None
        try:
            queued = time.monotonic()
            # 1. Bound launch concurrency. A fork uses snapshot_id; a fresh VM
            # uses the prepared image and explicit resource configuration.
            # The create waits on the server (SDK 0.5.158 and later) and
            # returns as soon as the sandbox runs; the SDK no longer polls
            # the sandbox status on a timer after sending the request.
            async with self.launch_slots:
                trial["launch_queue_s"] = time.monotonic() - queued
                trial["t0"] = time.monotonic()
                trial["dispatch_at"] = utc_now()
                trial["name"] = (
                    f"tl1000-{self.state['run_id']}-{case.replace('_', '-')}-{index}"
                )
                create_options = dict(
                    name=trial["name"],
                    timeout_secs=IDLE_TIMEOUT_SECONDS,
                    startup_timeout=180,
                    request_timeout=180,
                    cancel_on_timeout=True,
                )
                if snapshot:
                    create_options["snapshot_id"] = snapshot
                else:
                    create_options.update(
                        image=IMAGE, cpus=4, memory_mb=8192, disk_mb=10240
                    )
                sandbox = await self.call_provider(
                    "create",
                    lambda: self.client.create_and_connect(**create_options),
                    trial,
                )
                self.owned_ids.add(sandbox.sandbox_id)
                trial["sandbox_id"] = sandbox.sandbox_id
                self.sandboxes[sandbox.sandbox_id] = sandbox
            sandbox = await asyncio.wait_for(
                self.call_provider(
                    "connect",
                    lambda: self.client.connect(
                        sandbox.sandbox_id, request_timeout=2500
                    ),
                    trial,
                ),
                max(1, 180 - (time.monotonic() - trial["t0"])),
            )
            self.sandboxes[sandbox.sandbox_id] = sandbox
            # 2. Check real terminal access and any expected saved disk marker.
            await self.wait_for_terminal(
                sandbox,
                trial,
                self.state.get("disk_marker") if case.startswith("disk") else None,
            )
            await self.call_provider(
                "expose_control_port",
                lambda: self.client.expose_ports(
                    sandbox.sandbox_id, [8123], allow_unauthenticated_access=False
                ),
                trial,
            )
            # 3. A memory child must have a new VM ID but preserved guest state.
            # Fresh/disk children start a guest server; all paths then run setup.
            if mode == "memory":
                assert sandbox.sandbox_id != self.state["source_id"]
                await self.verify_preserved_state(sandbox, trial, expected)
            else:
                await self.start_guest_server(sandbox, trial)
            await self.run_workload(sandbox, trial, mode or case)
        except Exception as e:
            trial["error"] = self.redact_secrets(e)
            trial["setup_passed"] = False
        self.save_trial(trial)
        self.append_log(
            "progress.jsonl",
            {
                "at": utc_now(),
                "case": case,
                "index": index,
                "setup_passed": trial["setup_passed"],
                "error": trial.get("error"),
            },
        )
        return sandbox, trial

    # Activity and same-sandbox resume
    # Exercise prepared VMs during one shared window. VMs whose initial setup
    # failed do not participate and remain unrun for this separate activity test.
    async def run_activity_window(self, trials, label, cycles=12, duration=60):
        good = [p for p in trials if p[0] and p[1].get("setup_passed")]
        start = time.time() + (20 if duration == 60 else 3)
        end = start + duration

        async def schedule(pair):
            sandbox, trial = pair
            try:
                await self.request_guest(
                    sandbox,
                    {
                        "op": "activity",
                        "epoch": start,
                        "duration": duration,
                        "cycles": cycles,
                    },
                    trial,
                    30,
                )
                trial["activity_schedule_ack_epoch"] = time.time()
            except Exception as e:
                trial["activity_error"] = self.redact_secrets(e)

        # 1. Give guests a common window: twelve checks, five seconds apart.
        await asyncio.gather(*(schedule(p) for p in good))

        # 2. Collect guest events and verify writes outside the VM. Outside
        # reads can finish after the window; their lateness is recorded.
        async def poll(pair):
            sandbox, trial = pair
            trial["activity"] = []
            seen = 0
            previous = time.monotonic()
            try:
                while time.time() < end + 180:
                    result = await self.request_guest(
                        sandbox, {"op": "progress"}, trial, 30
                    )
                    observed = time.monotonic()
                    self.append_log(
                        "polling.jsonl",
                        {
                            "at": utc_now(),
                            "case": trial["case"],
                            "index": trial["index"],
                            "interval_s": observed - previous,
                        },
                    )
                    previous = observed
                    a = result.get("activity") or {}
                    for event in a.get("events", [])[seen:]:
                        e = dict(event)
                        if e.get("s3_write"):
                            e["external"] = await self.verify_s3_write(
                                e["s3_write"], trial
                            )
                        trial["activity"].append(e)
                    seen = len(a.get("events", []))
                    if a.get("done"):
                        trial["activity_done"] = True
                        break
                    await asyncio.sleep(2)
                # 3. Require every expected check and outside read. Missing evidence
                # is not proof that the VM itself crashed.
                events = trial["activity"]
                trial["activity_passed"] = (
                    trial.get("activity_done", False)
                    and len(events) == cycles
                    and all(
                        e.get("passed")
                        and e.get("external", {}).get("passed")
                        and start <= datetime.fromisoformat(e["at"]).timestamp() < end
                        for e in events
                    )
                )
                trial["activity_external_late"] = sum(
                    e.get("external", {}).get("epoch", 0) > end for e in events
                )
                trial["full_with_activity_s"] = time.monotonic() - trial["t0"]
                trial["activity_window"] = {"start": start, "end": end, "label": label}
            except Exception as e:
                trial["activity_error"] = self.redact_secrets(e)
                trial["activity_passed"] = False
            self.save_trial(trial)

        await asyncio.gather(*(poll(p) for p in good))
        return {
            "start_epoch": start,
            "end_epoch": end,
            "participants": len(good),
            "passed": sum(r.get("activity_passed", False) for _, r in trials),
            "unrun": len(trials) - len(good),
        }

    # Test same-ID suspend/resume only for VMs with a valid prepared workload.
    # Snapshot forks into different IDs are separate cases handled by create_trial().
    async def suspend_and_resume(self, trials):
        # 1. Capture each VM baseline before asking the provider to suspend it.
        async def stop(pair):
            sandbox, trial = pair
            if not sandbox or not trial.get("setup_passed"):
                return
            try:
                trial["baseline_before_stop"] = (
                    await self.request_guest(sandbox, {"op": "state"}, trial)
                )["state"]
                async with self.launch_slots:
                    begin = time.monotonic()
                    result = await asyncio.wait_for(
                        self.call_provider(
                            "suspend",
                            lambda: self.client.suspend(
                                sandbox.sandbox_id, timeout=180, poll_interval=1
                            ),
                            trial,
                        ),
                        180,
                    )
                trial["stop"] = {
                    "passed": True,
                    "duration_s": time.monotonic()
                    - trial["_suspend_dispatch_monotonic"],
                    "trace_id": result.trace_id,
                    "confirmed_state": "SUSPENDED",
                    "sandbox_id": sandbox.sandbox_id,
                }
            except Exception as e:
                trial["stop"] = {"passed": False, "error": self.redact_secrets(e)}
            self.save_trial(trial)

        await asyncio.gather(*(stop(p) for p in trials))
        # Start fresh controller connections after suspension; no guest process is restarted.
        await asyncio.gather(*(client.aclose() for client in self.control_clients))
        self.control_clients = create_control_clients()
        resumed = []

        # 2. Resume the same ID and compare untouched state before the second
        # edit. The later app reload is deliberate work, not resume repair.
        async def resume(pair):
            sandbox, original = pair
            if not original.get("stop", {}).get("passed"):
                return
            trial = {
                "case": (
                    "resume_proof"
                    if original["case"].endswith("_proof")
                    else "resume_corrected"
                    if original["case"] == "fresh_corrected"
                    else "resume"
                ),
                "index": original["index"],
                "marker": original["marker"] + "-R",
                "sandbox_id": sandbox.sandbox_id,
                "original_marker": original["marker"],
            }
            self.state["cases"].setdefault(trial["case"], []).append(trial)
            try:
                queued = time.monotonic()
                async with self.launch_slots:
                    trial["launch_queue_s"] = time.monotonic() - queued
                    trial["t0"] = time.monotonic()
                    trial["dispatch_at"] = utc_now()
                    await asyncio.wait_for(
                        self.call_provider(
                            "resume",
                            lambda: self.client.resume(
                                sandbox.sandbox_id, timeout=180, poll_interval=1
                            ),
                            trial,
                        ),
                        180,
                    )
                sandbox = await asyncio.wait_for(
                    self.call_provider(
                        "connect",
                        lambda: self.client.connect(
                            sandbox.sandbox_id, request_timeout=2500
                        ),
                        trial,
                    ),
                    max(1, 180 - (time.monotonic() - trial["t0"])),
                )
                await self.wait_for_terminal(sandbox, trial)
                await self.verify_preserved_state(
                    sandbox, trial, original["baseline_before_stop"]
                )
                await self.run_workload(sandbox, trial, "resume")
            except Exception as e:
                trial["error"] = self.redact_secrets(e)
                trial["setup_passed"] = False
            resumed.append((sandbox, trial))
            self.save_trial(trial)

        await asyncio.gather(*(resume(p) for p in trials))
        return resumed

    # Evidence collection, inventory and cleanup
    # Download screenshots, logs and resource samples after workload timing.
    # Artifact-transfer errors stay distinct from workload failures.
    async def collect_evidence(self, trials):
        sem = asyncio.Semaphore(32)

        async def one(pair):
            sandbox, trial = pair
            if not sandbox:
                return
            async with sem:
                try:
                    code = (
                        "from pathlib import Path;import zipfile\n"
                        "with zipfile.ZipFile('/workspace/bench-artifact.zip','w',zipfile.ZIP_DEFLATED) as z:\n"
                        " for base,prefix in [(Path('/workspace/bench-evidence'),'guest'),(Path('/var/log/bench'),'system')]:\n"
                        "  for p in base.rglob('*'):\n"
                        "   if p.is_file():z.write(p,str(Path(prefix)/p.relative_to(base)))"
                    )
                    await self.run_command(sandbox, ["python3", "-c", code], 60, trial)
                    data = (
                        await self.call_provider(
                            "artifact_read",
                            lambda: sandbox.read_file("/workspace/bench-artifact.zip"),
                            trial,
                        )
                    ).value
                    self.write_queue.put(
                        (
                            "bytes",
                            f"trials/{trial['case']}/{trial['index']}/evidence.zip",
                            data,
                        )
                    )
                except Exception as e:
                    trial["collection_error"] = self.redact_secrets(e)
                self.save_trial(trial)

        await asyncio.gather(*(one(p) for p in trials))

    async def delete_sandboxes(self, ids):
        async def one(sandbox_id):
            try:
                await self.call_provider(
                    "delete", lambda: self.client.delete(sandbox_id)
                )
                return {"id": sandbox_id, "deleted": True, "at": utc_now()}
            except Exception as e:
                return {"id": sandbox_id, "error": self.redact_secrets(e)}

        records = await asyncio.gather(*(one(sandbox_id) for sandbox_id in set(ids)))
        self.state.setdefault("cleanup", []).extend(records)
        self.save_state()

    async def list_sandboxes(self):
        def get():
            client = SandboxClient(
                api_key=os.environ["TENSORLAKE_API_KEY"], max_retries=0, _internal=True
            )
            return [
                {
                    "id": x.sandbox_id,
                    "name": x.name,
                    "status": str(x.status),
                    "resources": x.resources.model_dump(mode="json"),
                }
                for entry in client.list()
                for x in [entry.value if hasattr(entry, "value") else entry]
            ]

        return await asyncio.to_thread(get)

    # Refuse another cohort while the observed inventory has active VMs.
    # Unrelated workloads are not deleted to make room for the benchmark.
    async def check_available_capacity(self):
        inventory = await self.list_sandboxes()
        known = {
            x["id"]: x
            for check in self.state.get("capacity_checks", [])
            for x in check["resources"]
            if x["id"] not in self.owned_ids
        }
        current = {x["id"]: x for x in inventory}
        for sandbox_id in known:
            try:
                info = (await self.client.get(sandbox_id)).value
            except SandboxNotFoundError:
                # Historical capacity checks may include VMs deleted since then.
                # Preserve those checks, but do not reserve capacity for a missing VM.
                current.pop(sandbox_id, None)
                continue
            current[sandbox_id] = {
                "id": sandbox_id,
                "name": info.name,
                "status": str(info.status),
                "resources": info.resources.model_dump(mode="json"),
            }
        inventory = list(current.values())
        active = [
            x
            for x in inventory
            if x["status"]
            not in [
                "SandboxStatus.SUSPENDED",
                "SandboxStatus.TERMINATED",
                "SandboxStatus.FAILED",
            ]
        ]
        self.state.setdefault("capacity_checks", []).append(
            {"at": utc_now(), "resources": inventory, "active": len(active)}
        )
        self.save_state()
        if active:
            raise RuntimeError(
                f"Need all {SANDBOX_COUNT} slots free; active resources remain: "
                + str(active)
            )

    # Measure controller CPU, memory and event-loop delay separately from guest
    # resources, so a slow observer can be distinguished from slow VMs.
    async def record_controller_usage(self):
        start = time.monotonic()
        cpu0 = time.process_time()
        tick = start
        while not self.finished:
            await asyncio.sleep(2)
            t = time.monotonic()
            u = resource.getrusage(resource.RUSAGE_SELF)
            self.append_log(
                "controller-resources.jsonl",
                {
                    "at": utc_now(),
                    "elapsed": t - start,
                    "cpu_s": time.process_time() - cpu0,
                    "rss_bytes": u.ru_maxrss,
                    "loop_delay_s": max(0, t - tick - 2),
                    "io_active": self.active_api_requests,
                    "s3_active": self.active_s3_reads,
                    "queued_writes": self.write_queue.qsize(),
                    "completed_calls": self.completed_calls,
                },
            )
            tick = t

    async def finish(self):
        self.save_state()
        await asyncio.to_thread(self.write_queue.join)
        self.finished = True
        await asyncio.gather(*(client.aclose() for client in self.control_clients))

    # A create response can fail after the provider makes the VM. Recover IDs
    # by this run-specific name prefix so cleanup can find those resources.
    async def reconcile_owned_sandboxes(self):
        inventory = await self.list_sandboxes()
        by_name = {x["name"]: x for x in inventory if x["name"]}
        for rows in self.state.get("cases", {}).values():
            for trial in rows:
                if not trial.get("sandbox_id") and trial.get("name") in by_name:
                    trial["sandbox_id"] = by_name[trial["name"]]["id"]
                    trial["reconciled_after_error"] = True
                    self.save_trial(trial)
        for x in inventory:
            if x["name"] and x["name"].startswith(
                "tl1000-" + self.state["run_id"] + "-"
            ):
                self.owned_ids.add(x["id"])
        self.state["last_inventory"] = inventory
        self.save_state()
        return inventory

    # Delete run-owned IDs, then GET each until it is terminal or gone.
    # Accepting a delete request alone does not prove the VM has stopped.
    async def cleanup_owned_sandboxes(self):
        removed = {x["id"] for x in self.state.get("cleanup", []) if x.get("deleted")}
        await self.delete_sandboxes(self.owned_ids - removed)
        for attempt in range(36):
            inventory = await self.reconcile_owned_sandboxes()

            async def verify(sandbox_id):
                try:
                    info = (
                        await self.call_provider(
                            "cleanup_verify", lambda: self.client.get(sandbox_id)
                        )
                    ).value
                    return {
                        "id": sandbox_id,
                        "status": str(info.status),
                        "terminal": str(info.status)
                        in ["SandboxStatus.TERMINATED", "SandboxStatus.FAILED"],
                    }
                except Exception as e:
                    if type(e).__name__ == "SandboxNotFoundError":
                        return {
                            "id": sandbox_id,
                            "status": "NOT_FOUND",
                            "terminal": True,
                        }
                    raise

            verified = await asyncio.gather(
                *(verify(sandbox_id) for sandbox_id in self.owned_ids)
            )
            self.state["cleanup_verified"] = verified
            active = [x["id"] for x in verified if not x["terminal"]]
            if not active:
                self.save_state()
                return
            if attempt % 6 == 0:
                await self.delete_sandboxes(active)
            await asyncio.sleep(5)
        raise RuntimeError("Owned sandboxes remain after bounded cleanup")

    # GET every known ID. The list response observed in this run was capped
    # at 100 entries and could not establish a full 1,000-VM cohort.
    async def inspect_cohort(self, trials):
        async def get(pair):
            sandbox, trial = pair
            if not sandbox:
                return None
            info = (
                await self.call_provider(
                    "inventory_get", lambda: self.client.get(sandbox.sandbox_id), trial
                )
            ).value
            return {
                "id": info.sandbox_id,
                "name": info.name,
                "status": str(info.status),
                "resources": info.resources.model_dump(mode="json"),
            }

        values = await asyncio.gather(*(get(pair) for pair in trials))
        return [v for v in values if v]

    # Delete keys only beneath this run/provider prefix. The shared seed and
    # objects belonging to other runs must remain available.
    async def delete_run_s3_objects(self):
        prefix = (
            FIXTURE_CONFIG["s3_prefixes"]["tensorlake"]
            + "n100-"
            + self.state["run_id"]
            + "/"
        )

        def clean():
            keys = [
                o["Key"]
                for page in self.s3.get_paginator("list_objects_v2").paginate(
                    Bucket=FIXTURE_CONFIG["bucket"], Prefix=prefix
                )
                for o in page.get("Contents", [])
            ]
            errors = []
            for i in range(0, len(keys), 1000):
                errors.extend(
                    self.s3.delete_objects(
                        Bucket=FIXTURE_CONFIG["bucket"],
                        Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]},
                    ).get("Errors", [])
                )
            return {
                "prefix": prefix,
                "deleted": len(keys),
                "errors": errors,
                "remaining": self.s3.list_objects_v2(
                    Bucket=FIXTURE_CONFIG["bucket"], Prefix=prefix
                ).get("KeyCount", 0),
            }

        self.state["s3_cleanup"] = await asyncio.to_thread(clean)

    # Benchmark phases
    # Create saved user state and prove restore paths before the large cohort.
    # These short preparation checks are excluded from N=1,000 results.
    async def prepare_snapshots(self):
        self.state["snapshot_credentials"] = self.state["credential_observations"][-1]
        # 1. Run the source workload, capture its live state and add a disk marker.
        source = await self.create_trial("source", 0, mode="source")
        sandbox, trial = source
        if not trial.get("setup_passed"):
            raise RuntimeError("Source preparation failed: " + str(trial.get("error")))
        self.state["source_id"] = sandbox.sandbox_id
        info = (await self.client.get(sandbox.sandbox_id)).value
        self.state["source_allocation"] = info.resources.model_dump(mode="json")
        assert info.resources.cpus == 4 and info.resources.memory_mb == 8192
        telemetry = json.loads(
            await self.run_command(
                sandbox,
                [
                    "python3",
                    "-c",
                    "import json;from pathlib import Path;p=Path('/var/log/bench/resources.jsonl');print(p.read_text().splitlines()[-1])",
                ],
                30,
                trial,
            )
        )
        assert telemetry["cpu_count"] == 4
        self.state["source_telemetry_sample"] = telemetry
        marker = self.state["run_id"] + "-disk-source"
        await self.run_command(
            sandbox,
            [
                "python3",
                "-c",
                "import os,sys;f=open('/workspace/source-disk-marker','w');f.write(sys.argv[1]);f.flush();os.fsync(f.fileno());f.close()",
                marker,
            ],
            30,
            trial,
        )
        self.state["disk_marker"] = marker
        before = (await self.request_guest(sandbox, {"op": "state"}, trial))["state"]
        self.state["source_state"] = before
        assert before["node_processes"], (
            "App process identity probe found no Node server"
        )
        # 2. Save both filesystem and memory versions of the prepared source.
        disk = await self.call_provider(
            "disk_snapshot",
            lambda: self.client.snapshot_and_wait(
                sandbox.sandbox_id,
                snapshot_type=SnapshotType.FILESYSTEM,
                timeout=180,
                wait_until="completed",
            ),
            trial,
        )
        assert disk.value.snapshot_type.value == "filesystem"
        self.state["disk_snapshot_id"] = disk.value.snapshot_id
        self.state["disk_snapshot"] = disk.value.model_dump(mode="json")
        self.save_state()
        memory = await self.call_provider(
            "memory_snapshot",
            lambda: self.client.snapshot_and_wait(
                sandbox.sandbox_id,
                snapshot_type=SnapshotType.MEMORY,
                timeout=180,
                wait_until="completed",
            ),
            trial,
        )
        assert memory.value.snapshot_type.value == "memory"
        self.state["memory_snapshot_id"] = memory.value.snapshot_id
        self.state["memory_snapshot"] = memory.value.model_dump(mode="json")
        self.save_state()
        # 3. Check fresh, disk-fork, memory-fork and same-ID resume paths using
        # short activity windows before launching the measured cohort.
        proofs = []
        for case, snapshot, mode, expected in [
            ("fresh_proof", None, "fresh", None),
            ("disk_proof", self.state["disk_snapshot_id"], "disk", None),
            ("memory_proof", self.state["memory_snapshot_id"], "memory", before),
        ]:
            pair = await self.create_trial(case, 0, snapshot, expected, mode)
            proofs.append(pair)
            if not pair[1].get("setup_passed"):
                raise RuntimeError(case + " failed: " + str(pair[1].get("error")))
            result = await self.run_activity_window([pair], case, cycles=2, duration=10)
            if result["passed"] != 1:
                raise RuntimeError(case + " activity failed")
            if case == "fresh_proof":
                resumed = await self.suspend_and_resume([pair])
                proofs += resumed
                if len(resumed) != 1 or not resumed[0][1].get("setup_passed"):
                    raise RuntimeError("resume proof failed")
                active = await self.run_activity_window(
                    resumed, "resume_proof", cycles=2, duration=10
                )
                if active["passed"] != 1:
                    raise RuntimeError("resumed proof activity failed")
                await self.collect_evidence(resumed)
            else:
                await self.collect_evidence([pair])
            await self.delete_sandboxes([pair[0].sandbox_id])
        await self.collect_evidence([source])
        await self.delete_sandboxes([sandbox.sandbox_id])
        self.state["proofs"] = [r for _, r in proofs]
        self.state["status"] = "prepared"
        self.save_state()

    # Run one N-VM case without pooling it with other cases or repeat runs.
    # A fresh case also supplies the VMs for the same-ID resume measurement.
    async def run_case(self, case):
        # 1. Check observed capacity and choose the starting state. Saved memory
        # also needs unexpired credentials from source preparation.
        await self.check_available_capacity()
        self.state.setdefault("case_configs", {})[case] = {
            "http_pool_count": 32,
            "http_connections_per_pool": 64,
            "io_limit": API_CONCURRENCY,
            "start_limit": LAUNCH_CONCURRENCY,
            "s3_limit": S3_CONCURRENCY,
            "read_only_transport_attempts": 4,
            "mutation_attempts": 1,
            "controller_sha256": hashlib.sha256(
                Path(__file__).read_bytes()
            ).hexdigest(),
        }
        if case == "memory":
            for key in ["expiry", "probe_expiry"]:
                remaining = (
                    datetime.fromisoformat(
                        self.state["snapshot_credentials"][key].replace("Z", "+00:00")
                    ).timestamp()
                    - time.time()
                )
                if remaining < 1200:
                    raise RuntimeError(
                        "Saved memory snapshot credentials need fresh preparation: "
                        + key
                    )
        self.state["status"] = "running_" + case
        self.save_state()
        fresh = case.startswith("fresh")
        resume_case = "resume_corrected" if case == "fresh_corrected" else "resume"
        if fresh:
            snapshot = None
            expected = None
            mode = "fresh"
        elif case.startswith("disk"):
            snapshot = self.state["disk_snapshot_id"]
            expected = None
            mode = "disk"
        else:
            snapshot = self.state["memory_snapshot_id"]
            expected = self.state["source_state"]
            mode = "memory"
        self.state["cases"][case] = []
        if fresh:
            self.state["cases"][resume_case] = []
        # 2. Dispatch N trials with the launch semaphore and run their activity.
        started = time.monotonic()
        trials = await asyncio.gather(
            *(
                self.create_trial(case, i, snapshot, expected, mode)
                for i in range(1, SANDBOX_COUNT + 1)
            )
        )
        self.state.setdefault("cohort_inventories", {})[case] = {
            "at": utc_now(),
            "resources": await self.inspect_cohort(trials),
            "method": "Explicit GET for every created ID; provider list response was limited to 100 rows.",
        }
        self.state.setdefault("activity", {})[case] = await self.run_activity_window(
            trials, case
        )
        self.save_state()
        # 3. Fresh VMs continue into suspend/resume. Collect artifacts and clean
        # up afterward; those costs do not extend startup checkpoints.
        if fresh:
            resumed = await self.suspend_and_resume(trials)
            self.state["activity"][resume_case] = await self.run_activity_window(
                resumed, resume_case
            )
            await self.collect_evidence(resumed)
            resumed_ids = {sandbox.sandbox_id for sandbox, r in resumed}
            await self.collect_evidence(
                [
                    (sandbox, r)
                    for sandbox, r in trials
                    if sandbox and sandbox.sandbox_id not in resumed_ids
                ]
            )
        else:
            await self.collect_evidence(trials)
        await self.cleanup_owned_sandboxes()
        await self.reconcile_owned_sandboxes()
        self.state.setdefault("case_wall_s", {})[case] = time.monotonic() - started
        self.state["status"] = "ready"
        self.save_state()


# Run one explicit CLI phase. Preparation saves snapshot IDs and ownership;
# later invocations reload that state rather than starting a different run.
async def main():
    import sys

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 8192:
        resource.setrlimit(
            resource.RLIMIT_NOFILE, (min(8192, hard) if hard >= 0 else 8192, hard)
        )
    mode = sys.argv[1]
    if mode == "prepare":
        state = {
            "run_id": "n1000-"
            + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            + "-"
            + uuid.uuid4().hex[:6],
            "status": "preparing",
            "created_at": utc_now(),
            "requested_n": SANDBOX_COUNT,
            "shape": {"cpus": 4, "memory_mb": 8192, "disk_mb": 10240},
            "config": {
                "sdk_request_retries": 0,
                "process_restart": "never",
                "create_concurrency": LAUNCH_CONCURRENCY,
                "command_concurrency": API_CONCURRENCY,
                "s3_concurrency": S3_CONCURRENCY,
                "idle_timeout_s": IDLE_TIMEOUT_SECONDS,
                "payload_bytes": 87,
                "control_transport": "authenticated provider port proxy via async HTTP; no exec polling",
                "file_limit": resource.getrlimit(resource.RLIMIT_NOFILE)[0],
                "primary_checkpoints": [
                    "local_write",
                    "mount",
                    "s3_read",
                    "clone",
                    "edit",
                    "install",
                    "app",
                    "browser",
                    "s3_write",
                    "external_verification",
                ],
                "cost_full_cpu_per_vm_hour": 0.241,
                "workflow_deadline_s": 2400,
                "readiness_deadline_s": 180,
                "clone_install_deadline_s": 600,
                "app_browser_s3_deadline_s": 180,
                "no_legacy_dollar_guard": True,
            },
            "cases": {},
            "owned_ids": [],
        }
    else:
        current = json.loads((OUT / "current.json").read_text())
        state = json.loads((OUT / current["run_id"] / "state.json").read_text())
    runner = Runner(state, refresh_probe=mode not in ["cleanup", "cleanup_vms"])
    monitor = asyncio.create_task(runner.record_controller_usage())
    runner.save_state()
    try:
        if mode == "prepare":
            await asyncio.wait_for(runner.prepare_snapshots(), 1800)
        elif mode in ["fresh", "disk", "memory", "fresh_corrected", "disk_corrected"]:
            await asyncio.wait_for(runner.run_case(mode), 6000)
        elif mode == "cleanup_vms":
            await runner.cleanup_owned_sandboxes()
            runner.state["status"] = "ready"
        elif mode == "cleanup":
            await runner.cleanup_owned_sandboxes()
            for key in ["disk_snapshot_id", "memory_snapshot_id"]:
                if runner.state.get(key):
                    try:
                        await runner.client.delete_snapshot(runner.state[key])
                        runner.state[key + "_deleted"] = True
                    except Exception as e:
                        runner.state[key + "_delete_error"] = runner.redact_secrets(e)
            await runner.delete_run_s3_objects()
            runner.state["status"] = "complete"
            runner.state["finished_at"] = utc_now()
        else:
            raise ValueError(mode)
    except Exception as e:
        runner.state["error"] = type(e).__name__ + ": " + runner.redact_secrets(e)
        runner.state["status"] = (
            "blocked"
            if "slots free" in str(e) or "credentials need fresh preparation" in str(e)
            else "failed"
        )
        runner.save_state()
        await runner.cleanup_owned_sandboxes()
        raise
    finally:
        await runner.finish()
        monitor.cancel()


if __name__ == "__main__":
    asyncio.run(main())
