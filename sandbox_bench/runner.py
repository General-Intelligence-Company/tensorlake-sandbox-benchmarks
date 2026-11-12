"""Run-owned resources, first-attempt evidence and a common guest workload."""

import asyncio
import base64
import fcntl
import hashlib
import io
import json
import math
import os
import random
import re
import shlex
import statistics
import tarfile
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import distribution
from pathlib import Path

from .providers import ProviderError


class GuestFailure(RuntimeError):
    def __init__(self, result):
        super().__init__("Guest workload: " + json.dumps(result))
        self.result = result


def now():
    return datetime.now(timezone.utc).isoformat()


def http_status(error):
    return (
        getattr(error, "status_code", None)
        or getattr(error, "status", None)
        or getattr(getattr(error, "response", None), "status_code", None)
    )


def assets():
    local = Path(__file__).resolve().parents[1] / "context"
    if local.is_dir():
        return local
    package = distribution("sandbox-bench")
    bundle = next(p for p in package.files if p.name == "shared-app.bundle")
    return Path(package.locate_file(bundle)).resolve().parent


def python(command):
    return "python3 -c " + shlex.quote(command)


class Evidence:
    def __init__(self, path, state=None):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=state is None, mode=0o700)
        self.lock = (self.path / ".lock").open("a")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.state = (
            state
            if state is not None
            else json.loads((self.path / "run.json").read_text())
        )
        self.secrets = [
            v
            for k, v in os.environ.items()
            if v and ("TOKEN" in k or "KEY" in k or "SECRET" in k)
        ]

    def clean(self, value):
        text = json.dumps(value, default=str)
        for secret in self.secrets:
            text = text.replace(secret, "[redacted]")
        return re.sub(r'(https?://[^\s"?]+)\?[^\s"]+', r"\1?[redacted]", text)

    def save(self):
        temp = self.path / "run.json.tmp"
        temp.write_text(self.clean(self.state) + "\n")
        temp.replace(self.path / "run.json")

    def log(self, kind, **value):
        with (self.path / "events.jsonl").open("a") as stream:
            stream.write(self.clean({"at": now(), "kind": kind, **value}) + "\n")

    async def mutation(self, kind, fn, **context):
        op = {
            "operation_id": uuid.uuid4().hex,
            "kind": kind,
            "at": now(),
            "status": "dispatched",
            **context,
        }
        self.state["operations"].append(op)
        self.save()  # An interrupted process leaves a visible, non-replayable intent.
        start = time.monotonic()
        try:
            value = await fn()
            op.update(status="returned", result=value)
            return value
        except BaseException as exc:
            op.update(status="unconfirmed", error=type(exc).__name__ + ": " + str(exc))
            raise
        finally:
            op["duration_s"] = time.monotonic() - start
            self.save()


class S3Fixture:
    def __init__(self, config, evidence):
        import boto3
        from botocore.config import Config

        session = boto3.Session(
            profile_name=config["profile"], region_name=config["region"]
        )
        credentials = session.get_credentials()
        frozen = credentials.get_frozen_credentials()
        self.expiry = getattr(credentials, "_expiry_time", None)
        if self.expiry is None:
            raise ValueError(
                "AWS profile must return temporary credentials with an expiration time"
            )
        self.ensure_valid()
        if not frozen.token:
            raise ValueError(
                "Workflow tests require temporary AWS role credentials with a session token"
            )
        account = session.client("sts").get_caller_identity()["Account"]
        if account != config["expected_account"]:
            raise ValueError("AWS account does not match s3.expected_account")
        self.config = config
        self.evidence = evidence
        self.client = session.client(
            "s3",
            config=Config(
                connect_timeout=2, read_timeout=2, retries={"total_max_attempts": 1}
            ),
        )
        self.env = {
            "AWS_ACCESS_KEY_ID": frozen.access_key,
            "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
            "AWS_SESSION_TOKEN": frozen.token,
            "AWS_DEFAULT_REGION": config["region"],
        }
        evidence.secrets.extend((frozen.access_key, frozen.secret_key, frozen.token))
        # Read the user's existing seed. Never overwrite it or create a shared fixture.
        seed = self.client.get_object(
            Bucket=config["bucket"], Key=config["prefix"] + config["seed"]
        )["Body"]
        try:
            data = seed.read()
        finally:
            seed.close()
        self.seed_hash = hashlib.sha256(data).hexdigest()
        self.env["N100_PROBE_URL"] = self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": config["bucket"],
                "Key": config["prefix"] + config["seed"],
            },
            ExpiresIn=3600,
        )
        evidence.secrets.append(self.env["N100_PROBE_URL"])
        evidence.state.setdefault("credential_checks", []).append(
            {"at": now(), "expires_at": self.expiry.isoformat(), "account": account}
        )

    def ensure_valid(self):
        if self.expiry.timestamp() - time.time() < 1500:
            raise RuntimeError(
                "AWS session has less than 25 minutes remaining; refresh it before a new run"
            )

    async def verify(self, write):
        def read():
            from botocore.exceptions import (
                ClientError,
                ConnectionClosedError,
                ConnectTimeoutError,
                EndpointConnectionError,
                ReadTimeoutError,
                ResponseStreamingError,
            )

            attempts = []
            deadline = time.monotonic() + 15
            for attempt in range(1, 4):
                start, retry = time.monotonic(), None
                try:
                    r = self.client.get_object(
                        Bucket=self.config["bucket"], Key=write["key"]
                    )
                    body = r["Body"]
                    try:
                        data = body.read()
                    finally:
                        body.close()
                    result = {
                        "passed": len(data) == write["bytes"]
                        and hashlib.sha256(data).hexdigest() == write["sha256"],
                        "request_id": (r.get("ResponseMetadata") or {}).get(
                            "RequestId"
                        ),
                    }
                except Exception as exc:
                    result = {
                        "passed": False,
                        "error": str(exc),
                        "type": type(exc).__name__,
                    }
                    if (
                        isinstance(exc, ClientError)
                        and (exc.response.get("Error") or {}).get("Code") == "SlowDown"
                    ):
                        retry = "s3_slowdown"
                    elif isinstance(
                        exc,
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
                        **result,
                        "attempt": attempt,
                        "duration_s": time.monotonic() - start,
                        "retry_reason": retry,
                    }
                )
                if result["passed"] or not retry or attempt == 3:
                    break
                delay = (
                    random.uniform(0, 0.5) if retry == "s3_slowdown" else 0.25
                ) * 2 ** (attempt - 1)
                if time.monotonic() + delay + 4 > deadline:
                    break
                time.sleep(delay)
            return {
                **result,
                "attempts": attempts,
                "first_attempt_passed": attempts[0]["passed"],
            }

        result = await asyncio.to_thread(read)
        self.evidence.log("external_s3_read", key=write["key"], **result)
        return result

    async def cleanup(self, run_id):
        prefix = self.config["prefix"] + "n100-" + run_id + "/"

        def remove():
            keys = [
                o["Key"]
                for page in self.client.get_paginator("list_objects_v2").paginate(
                    Bucket=self.config["bucket"], Prefix=prefix
                )
                for o in page.get("Contents", [])
            ]
            errors = []
            for start in range(0, len(keys), 1000):
                errors += self.client.delete_objects(
                    Bucket=self.config["bucket"],
                    Delete={
                        "Objects": [{"Key": k} for k in keys[start : start + 1000]]
                    },
                ).get("Errors", [])
            remaining = self.client.list_objects_v2(
                Bucket=self.config["bucket"], Prefix=prefix
            ).get("KeyCount", 0)
            return {
                "prefix": prefix,
                "deleted": len(keys),
                "remaining": remaining,
                "errors": errors,
            }

        return await asyncio.to_thread(remove)


class Runner:
    def __init__(self, config, provider, evidence, fixture=None):
        self.c, self.p, self.e, self.s3 = config, provider, evidence, fixture
        self.slots = asyncio.Semaphore(config["concurrency"])
        self.run_id = self.e.state["run_id"]

    async def exec(self, sid, command, timeout=180):
        start = time.monotonic()
        try:
            r = await self.p.exec(sid, command, timeout)
            self.e.log(
                "command",
                id=sid,
                command_sha256=hashlib.sha256(command.encode()).hexdigest(),
                duration_s=time.monotonic() - start,
                exit_code=r.exit_code,
                stdout=r.stdout[-2000:],
                stderr=r.stderr[-2000:],
            )
            if r.exit_code != 0:
                raise ProviderError(
                    f"Guest command exited {r.exit_code}: {r.stdout[-1000:]} {r.stderr[-1000:]}"
                )
            return r.stdout
        except Exception as exc:
            self.e.log(
                "command_error",
                id=sid,
                duration_s=time.monotonic() - start,
                error=str(exc),
            )
            raise

    async def terminal(self, sid, row):
        deadline, delay = row["t0"] + 180, 0.5
        while time.monotonic() < deadline:
            try:
                data = json.loads(
                    await self.exec(
                        sid,
                        python(
                            "import time,json;print(json.dumps({'monotonic':time.monotonic()}))"
                        ),
                        min(20, deadline - time.monotonic()),
                    )
                )
                row["terminal_ready_s"] = time.monotonic() - row["t0"]
                row["anchor"] = {
                    "guest": data["monotonic"],
                    "controller": time.monotonic(),
                }
                return
            except Exception as exc:
                row.setdefault("readiness_errors", []).append(
                    {"at": now(), "error": str(exc), "delay_s": delay}
                )
                if http_status(exc) in (401, 403):
                    raise
                await asyncio.sleep(min(delay, max(0, deadline - time.monotonic())))
                delay = min(delay * 2, 30)
        raise TimeoutError("Terminal readiness exceeded 180 seconds")

    async def bootstrap(self, sid, row):
        start = time.monotonic()
        # Package installation is part of cold workflow time, reported separately.
        # Users can supply a tools image; this check never silently treats it as warm.
        script = """set -eu
if command -v sudo >/dev/null; then SUDO=sudo; else SUDO=; fi
$SUDO mkdir -p /opt/bench /workspace /mnt/library /var/log/bench
if ! test -x /opt/bench-venv/bin/python || ! command -v s3fs >/dev/null; then
  $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update -qq
  $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3 git s3fs curl
  if ! command -v node >/dev/null || ! command -v npm >/dev/null; then
    $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs npm
  fi
  $SUDO python3 -m venv /opt/bench-venv
  $SUDO /opt/bench-venv/bin/pip -q install playwright==1.58.0
  $SUDO /opt/bench-venv/bin/python -m playwright install --with-deps chromium
fi
$SUDO /opt/bench-venv/bin/python -c 'from playwright.sync_api import sync_playwright; from pathlib import Path; p=sync_playwright().start(); t=Path("/root/.agent-browser/browsers/portable"); t.mkdir(parents=True,exist_ok=True); c=t/"chrome"; c.symlink_to(p.chromium.executable_path) if not c.exists() else None; p.stop()'
$SUDO chmod 1777 /workspace
"""
        await self.exec(sid, script, 1200)
        cfg = json.loads((assets() / "manifest.example.json").read_text())
        cfg.update(
            provider=self.c["provider"],
            bucket=self.s3.config["bucket"],
            region=self.s3.config["region"],
            seed_relative_path=self.s3.config["seed"],
            seed_sha256=self.s3.seed_hash,
            s3_prefixes={self.c["provider"]: self.s3.config["prefix"]},
        )
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as archive:
            for name in (
                "sandbox_workload.py",
                "run_sandbox_workload.py",
                "record_resource_usage.py",
                "shared-app.bundle",
            ):
                archive.add(assets() / name, arcname=name)
            data = json.dumps(cfg).encode()
            info = tarfile.TarInfo("manifest.json")
            info.size, info.mode = len(data), 0o600
            archive.addfile(info, io.BytesIO(data))
        encoded = base64.b64encode(buf.getvalue()).decode()
        await self.exec(
            sid,
            "printf %s " + shlex.quote(encoded) + " | base64 -d | "
            "$(command -v sudo || true) tar xzf - -C /opt/bench",
            120,
        )
        row["bootstrap_s"] = time.monotonic() - start

    async def start_guest(self, sid):
        env = {**self.s3.env, "BENCH_BIND_HOST": "127.0.0.1"}
        command = "env " + " ".join(shlex.quote(k + "=" + v) for k, v in env.items())
        command += " /opt/bench-venv/bin/python -u /opt/bench/run_sandbox_workload.py"
        for service, service_command, log_path in [
            ("guest", command, "/var/log/bench/guest.log"),
            (
                "recorder",
                "python3 /opt/bench/record_resource_usage.py",
                "/var/log/bench/recorder.log",
            ),
        ]:
            pid = await self.p.start_service(sid, service_command, log_path)
            self.e.log("service_started", id=sid, service=service, pid=pid)
        for attempt in range(30):
            try:
                await self.guest(sid, {"op": "progress"}, 10)
                return
            except Exception:
                await asyncio.sleep(0.5)
        raise TimeoutError("Guest service did not start")

    async def guest(self, sid, payload, timeout=600):
        # A wedged FUSE read can delay Python's signal handler in the guest.
        # Bound the observing client as well, without restarting the saved mount.
        if payload.get("op") == "state":
            timeout = min(timeout, 45)
        # A native exec reaches a loopback HTTP service. No public guest endpoint,
        # provider key, or provider admin credentials are installed in the VM.
        code = (
            "import urllib.request,json,base64;req=urllib.request.Request('http://127.0.0.1:8123/',data="
            + repr(json.dumps(payload).encode())
            + ",headers={'Content-Type':'application/json'});print(base64.b64encode(urllib.request.urlopen(req,timeout="
            + str(timeout)
            + ").read()).decode())"
        )
        # Some native exec APIs insert newlines between output chunks. Base64
        # tolerates that framing without changing JSON strings or their bytes.
        output = await self.exec(sid, python(code), timeout + 10)
        data = json.loads(base64.b64decode(output))
        if not data["ok"]:
            raise GuestFailure(data)
        return data["result"]

    async def workload(self, sid, row, mode, expected=None, disk_expected=None):
        if expected is not None:
            observed = (await self.guest(sid, {"op": "state"}))["state"]
            row["preservation"] = {
                "passed": observed == expected,
                "expected": expected,
                "observed": observed,
            }
            if observed != expected:
                raise RuntimeError(
                    "Saved RAM/browser/app/disk state differs; refusing repair"
                )
        elif mode in ("disk", "disk_resume"):
            check = json.loads(
                await self.exec(
                    sid,
                    python(
                        "import json,hashlib;from pathlib import Path;root=Path("
                        + repr(disk_expected["repo_path"])
                        + ");"
                        "print(json.dumps({'hash':hashlib.sha256((root/'api/index.mjs').read_bytes()).hexdigest(),"
                        "'dependency':(root/'node_modules/escape-html/package.json').exists()}))"
                    ),
                )
            )
            row["disk_preservation"] = {
                "passed": check["hash"] == disk_expected["edit_sha256"]
                and check["dependency"],
                "observed": check,
            }
            if not row["disk_preservation"]["passed"]:
                raise RuntimeError("Saved disk edit or installed dependency missing")
            # RAM is deliberately not expected in a disk fork.
            await self.start_guest(sid)
        else:
            await self.bootstrap(sid, row)
            await self.start_guest(sid)
        payload = {
            "op": "setup",
            "case": row["case"],
            "index": row["index"],
            "run": self.run_id,
            "marker": self.run_id + "-" + row["case"] + "-" + str(row["index"]),
            "mode": mode,
        }
        if mode == "disk_resume":
            payload["previous_marker"] = disk_expected["marker"]
            payload["repo_path"] = disk_expected["repo_path"]
        result = await self.guest(sid, payload, 1200)
        row["steps"] = result["steps"]
        row["external_s3"] = await self.s3.verify(
            next(s["result"] for s in result["steps"] if s["step"] == "s3_write")
        )
        if not row["external_s3"]["passed"]:
            row["failure_domain"] = "external_s3_verification"
            raise RuntimeError("Independent S3 readback did not match")
        row["full_workflow_s"] = time.monotonic() - row["t0"]
        for step in result["steps"]:
            row[step["step"] + "_ready_s"] = (
                step["guest_monotonic"]
                - row["anchor"]["guest"]
                + row["anchor"]["controller"]
                - row["t0"]
            )
        row["s3_write_ready_s"] = row["full_workflow_s"]
        return result["state"]

    async def trial(
        self, case, index, ref=None, expected=None, existing=None, disk_expected=None
    ):
        row = {"case": case, "index": index, "status": "queued", "queued_at": now()}
        self.e.state["trials"].append(row)
        self.e.save()
        async with self.slots:
            name = f"sb-{self.run_id}-{case}-{index}"
            row.update(
                name=name, status="dispatched", t0=time.monotonic(), dispatched_at=now()
            )
            self.e.save()
            try:
                if existing:
                    sid = await self.e.mutation(
                        "resume",
                        lambda: self.p.resume(existing),
                        id=existing,
                        name=name,
                    )
                    if sid != existing:
                        raise RuntimeError("Same-ID resume returned another sandbox")
                else:
                    sid = await self.e.mutation(
                        "create", lambda: self.p.create(name, ref), name=name
                    )
                    self.e.state["owned"][sid] = {"name": name, "deleted": False}
                    if ref and sid == ref["source_id"]:
                        raise RuntimeError("Fork reused its source ID")
                row["sandbox_id"] = sid
                self.e.save()
                await self.terminal(sid, row)
                if self.c["suite"] == "commands":
                    # Deterministic shell/filesystem checks, separate from the
                    # full browser/S3 workload and historical command benchmark.
                    code = 'set -eu; d=$(mktemp -d); trap \'rm -rf "$d"\' EXIT; printf \'%s\' sandbox-bench > "$d/a"; cp "$d/a" "$d/b"; cmp "$d/a" "$d/b"; mv "$d/b" "$d/c"; test -s "$d/c"; uname -s; printf \'\\nCOMMANDS_OK\\n\''
                    out = await self.exec(sid, code)
                    if not out.endswith("COMMANDS_OK\n"):
                        raise RuntimeError("Command verification marker missing")
                    row["commands_s"] = time.monotonic() - row["t0"]
                else:
                    mode = (
                        ("resume" if expected else "disk_resume")
                        if existing
                        else (
                            ref["kind"]
                            if ref
                            else "source"
                            if case.startswith("source-")
                            else "fresh"
                        )
                    )
                    row["saved_state"] = await self.workload(
                        sid,
                        row,
                        mode,
                        expected,
                        disk_expected or (ref or {}).get("disk_expected"),
                    )
                row["status"] = "passed"
            except asyncio.CancelledError:
                row.update(
                    status="interrupted", error="Controller interrupted this trial"
                )
                raise
            except Exception as exc:
                row.update(status="failed", error=type(exc).__name__ + ": " + str(exc))
                if isinstance(exc, GuestFailure):
                    row["guest_failure"] = exc.result
                    row["steps"] = exc.result.get("steps", [])
                    row["failure_domain"] = "guest_workload"
                row.setdefault("failure_domain", "unknown")
            finally:
                row["duration_s"] = time.monotonic() - row["t0"]
                self.e.save()
            return row

    async def activity(self, rows):
        if self.c["suite"] != "workflow":
            return
        ready = [r for r in rows if r["status"] == "passed"]
        epoch = time.time() + max(5, len(ready) * 0.25)

        async def schedule(row):
            try:
                await self.guest(
                    row["sandbox_id"],
                    {"op": "activity", "epoch": epoch, "cycles": 12, "duration": 60},
                )
            except Exception as exc:
                row["activity"] = {
                    "passed": False,
                    "error": str(exc),
                    "failure_domain": "harness_control",
                }

        await asyncio.gather(*(schedule(r) for r in ready))
        if not ready:
            return
        await asyncio.sleep(max(0, epoch + 60 - time.time()))

        async def collect(row):
            if "activity" in row:
                return
            try:
                drain_started = time.monotonic()
                deadline = drain_started + 180
                value = (await self.guest(row["sandbox_id"], {"op": "progress"}))[
                    "activity"
                ]
                while not value["done"] and time.monotonic() < deadline:
                    await asyncio.sleep(0.5)
                    value = (
                        await self.guest(
                            row["sandbox_id"],
                            {"op": "progress"},
                            min(30, max(1, deadline - time.monotonic())),
                        )
                    )["activity"]
                events = value["events"]
                for event in events:
                    if event.get("s3_write"):
                        event["external_s3"] = await self.s3.verify(event["s3_write"])
                row["activity"] = {
                    "passed": bool(value["done"])
                    and len(events) == 12
                    and all(
                        e.get("passed")
                        and e.get("external_s3", {}).get("passed")
                        and e["start_lateness_s"] <= 1
                        and datetime.fromisoformat(e["at"]).timestamp() <= epoch + 61
                        for e in events
                    ),
                    "window_start": epoch,
                    "window_end": epoch + 60,
                    "drain_wait_s": time.monotonic() - drain_started,
                    "events": events,
                    "done": value["done"],
                }
            except Exception as exc:
                row["activity"] = {
                    "passed": False,
                    "error": str(exc),
                    "failure_domain": "unknown",
                }

        await asyncio.gather(*(collect(r) for r in ready))
        self.e.save()

    async def artifact(self, row):
        if self.c["suite"] != "workflow" or not row.get("sandbox_id"):
            return
        try:
            code = (
                "import io,zipfile,base64;from pathlib import Path;b=io.BytesIO();z=zipfile.ZipFile(b,'w',zipfile.ZIP_DEFLATED);"
                "[(z.write(p,str(Path(base.name)/p.relative_to(base)))) for base in [Path('/workspace/bench-evidence'),Path('/var/log/bench')] for p in base.rglob('*') if p.is_file()];z.close();print(base64.b64encode(b.getvalue()).decode())"
            )
            data = await self.exec(
                row["sandbox_id"], "$(command -v sudo || true) " + python(code), 120
            )
            path = self.e.path / "artifacts" / f"{row['case']}-{row['index']}.zip"
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(base64.b64decode(data))
            row["artifact"] = str(path.relative_to(self.e.path))
        except Exception as exc:
            row["collection_error"] = str(exc)
        self.e.save()

    async def cleanup_ids(self, ids):
        for sid in set(ids):
            record = self.e.state["owned"][sid]
            if record.get("deleted"):
                continue
            try:
                if not await self.p.absent(sid):
                    if any(
                        o["kind"] == "delete"
                        and o.get("id") == sid
                        and o["status"] == "unconfirmed"
                        for o in self.e.state["operations"]
                    ):
                        raise RuntimeError(
                            "Earlier deletion outcome is unconfirmed; not replaying it"
                        )
                    await self.e.mutation("delete", lambda: self.p.delete(sid), id=sid)
                for _ in range(60):
                    if await self.p.absent(sid):
                        record.update(deleted=True, verified_at=now())
                        break
                    await asyncio.sleep(1)
                if not record.get("deleted"):
                    raise RuntimeError("Deletion was not independently confirmed")
            except Exception as exc:
                record["cleanup_error"] = str(exc)
            self.e.save()

    async def cleanup(self):
        # Reconcile names after an ambiguous create before deleting known IDs.
        try:
            inventory = await self.p.inventory()
            intents = {
                o.get("name")
                for o in self.e.state["operations"]
                if o["kind"] == "create"
            }
            for vm in inventory:
                if vm.get("name") in intents or vm.get("owned_run") == self.run_id:
                    self.e.state["owned"].setdefault(
                        vm["id"], {"name": vm["name"], "deleted": False}
                    )
        except Exception as exc:
            self.e.state["reconciliation_error"] = str(exc)
        await self.cleanup_ids(list(self.e.state["owned"]))
        for ref in self.e.state["snapshots"]:
            try:
                if not await self.p.snapshot_absent(ref) and not ref.get(
                    "delete_submitted"
                ):
                    if any(
                        o["kind"] == "delete_snapshot"
                        and o.get("snapshot", {}).get("id") == ref["id"]
                        and o["status"] == "unconfirmed"
                        for o in self.e.state["operations"]
                    ):
                        raise RuntimeError(
                            "Earlier snapshot deletion is unconfirmed; not replaying it"
                        )
                    await self.e.mutation(
                        "delete_snapshot",
                        lambda: self.p.delete_snapshot(ref),
                        snapshot=ref,
                    )
                    ref["delete_submitted"] = True
                ref["deleted"] = await self.p.snapshot_absent(ref)
            except Exception as exc:
                ref["cleanup_error"] = str(exc)
            self.e.save()
        if self.s3:
            try:
                self.e.state["s3_cleanup"] = await self.s3.cleanup(self.run_id)
            except Exception as exc:
                self.e.state["s3_cleanup"] = {"error": str(exc)}
        self.e.save()

    async def phase(self, case):
        if self.s3:
            self.s3.ensure_valid()
        ref, expected = None, None
        source_ids = []
        if case in ("disk", "memory"):
            source = await self.trial("source-" + case, 0)
            if source["status"] != "passed":
                raise RuntimeError("Snapshot source workload failed")
            source["preparation"] = True
            sid = source["sandbox_id"]
            source_ids.append(sid)
            await self.exec(
                sid,
                python(
                    "from pathlib import Path;Path('/workspace/source-disk-marker').write_text("
                    + repr(self.run_id)
                    + ")"
                ),
            )
            expected = (
                (await self.guest(sid, {"op": "state"}))["state"]
                if case == "memory"
                else None
            )
            ref = await self.e.mutation(
                "snapshot",
                lambda: self.p.snapshot(sid, case, self.run_id + "-" + case),
                id=sid,
            )
            ref.update(source_id=sid, disk_expected=source["saved_state"])
            self.e.state["snapshots"].append(ref)
            self.e.save()
        for start in range(1, self.c["count"] + 1, self.c["batch_size"]):
            if self.s3:
                self.s3.ensure_valid()
            if any(o["status"] == "unconfirmed" for o in self.e.state["operations"]):
                raise RuntimeError(
                    "Unconfirmed lifecycle operation; no further dispatch"
                )
            indices = range(
                start, min(start + self.c["batch_size"], self.c["count"] + 1)
            )
            rows = await asyncio.gather(
                *(self.trial(case, i, ref, expected) for i in indices)
            )
            await self.activity(rows)
            await asyncio.gather(*(self.artifact(r) for r in rows))
            if case == "fresh" and "resume" in self.c["lifecycles"]:
                resumed = []
                for row in rows:
                    if row["status"] != "passed":
                        self.e.state["trials"].append(
                            {
                                "case": "resume",
                                "index": row["index"],
                                "status": "ineligible",
                                "error": "Fresh workload did not pass",
                            }
                        )
                        continue
                    sid = row["sandbox_id"]
                    try:
                        preserved = (
                            (await self.guest(sid, {"op": "state"}))["state"]
                            if self.p.resume_memory
                            else None
                        )
                        await self.e.mutation(
                            "pause", lambda: self.p.pause(sid), id=sid
                        )
                        resumed.append(
                            await self.trial(
                                "resume",
                                row["index"],
                                expected=preserved,
                                existing=sid,
                                disk_expected=row["saved_state"],
                            )
                        )
                    except Exception as exc:
                        self.e.state["trials"].append(
                            {
                                "case": "resume",
                                "index": row["index"],
                                "status": "ineligible",
                                "error": str(exc),
                            }
                        )
                await self.activity(resumed)
                await asyncio.gather(*(self.artifact(r) for r in resumed))
            await self.cleanup_ids(
                [r["sandbox_id"] for r in rows if r.get("sandbox_id")]
            )
            if any(
                not v.get("deleted")
                for k, v in self.e.state["owned"].items()
                if k not in source_ids
            ):
                raise RuntimeError("Cleanup not confirmed; next batch held")
        await self.cleanup_ids(source_ids)

    async def run(self):
        self.e.state["status"] = "running"
        self.e.save()
        try:
            for case in self.c["lifecycles"]:
                if case != "resume":
                    await self.phase(case)
            self.e.state["status"] = "finished"
        except BaseException as exc:
            self.e.state.update(
                status="interrupted", error=type(exc).__name__ + ": " + str(exc)
            )
            raise
        finally:
            await self.cleanup()
            self.e.state["finished_at"] = now()
            self.e.save()


def summarize(state):
    result = {
        "run_id": state["run_id"],
        "provider": state["config"]["provider"],
        "status": state["status"],
        "cases": {},
    }
    for case in state["config"]["lifecycles"]:
        rows = [r for r in state["trials"] if r["case"] == case]
        values = sorted(
            r["full_workflow_s"]
            for r in rows
            if r.get("full_workflow_s") is not None and r["status"] == "passed"
        )
        result["cases"][case] = {
            "requested": state["config"]["count"],
            "attempted": sum("t0" in r for r in rows),
            "passed": sum(r["status"] == "passed" for r in rows),
            "ineligible": sum(r["status"] == "ineligible" for r in rows),
            "unrun": state["config"]["count"] - len(rows),
            "clean_activity": sum(
                r.get("activity", {}).get("passed", False) for r in rows
            ),
            "failed": sum(r["status"] == "failed" for r in rows),
            "interrupted": sum(r["status"] == "interrupted" for r in rows),
            "successful_workflow_s": {
                "n": len(values),
                "mean": statistics.mean(values) if values else None,
                "p95": values[max(0, math.ceil(len(values) * 0.95) - 1)]
                if values
                else None,
            },
            "errors": [
                {
                    "index": r["index"],
                    "error": r.get("error"),
                    "domain": r.get("failure_domain"),
                }
                for r in rows
                if r["status"] != "passed"
            ],
        }
        result["cases"][case]["timings_s"] = {}
        for field in (
            "terminal_ready_s",
            "local_write_ready_s",
            "s3_write_ready_s",
            "full_workflow_s",
            "commands_s",
            "bootstrap_s",
        ):
            measured = sorted(r[field] for r in rows if r.get(field) is not None)
            result["cases"][case]["timings_s"][field] = {
                "n": len(measured),
                "mean": statistics.mean(measured) if measured else None,
                "p95": measured[max(0, math.ceil(len(measured) * 0.95) - 1)]
                if measured
                else None,
            }
    result["cleanup"] = {
        "owned": len(state["owned"]),
        "deleted": sum(x.get("deleted", False) for x in state["owned"].values()),
        "snapshots": state["snapshots"],
        "s3": state.get("s3_cleanup"),
        "unconfirmed_operations": [
            o for o in state["operations"] if o["status"] == "unconfirmed"
        ],
    }
    return result


def complete(report, config):
    clean = report["cleanup"]
    return (
        report["status"] == "finished"
        and clean["owned"] == clean["deleted"]
        and not clean["unconfirmed_operations"]
        and all(x.get("deleted") for x in clean["snapshots"])
        and (
            not clean["s3"]
            or clean["s3"].get("remaining") == 0
            and not clean["s3"].get("errors")
        )
        and all(
            v["passed"] == v["requested"]
            and (config["suite"] != "workflow" or v["clean_activity"] == v["passed"])
            for v in report["cases"].values()
        )
    )
