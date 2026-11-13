import asyncio
import hashlib
import io
import json
from types import SimpleNamespace

import httpx
import pytest

from sandbox_bench.cli import configuration, doctor, example
from sandbox_bench.providers import SPECS, Boat, Command, Exe, ProviderError, Tensorlake
from sandbox_bench.runner import Evidence, Runner, S3Fixture, complete, summarize


def config(tmp_path, provider="boat", **overrides):
    path = tmp_path / "config.toml"
    path.write_text(example(provider))
    return {**configuration(path), "run_id": "test-run", **overrides}


def evidence(tmp_path, c):
    return Evidence(
        tmp_path / "run",
        {
            "run_id": c["run_id"],
            "config": c,
            "status": "prepared",
            "operations": [],
            "owned": {},
            "snapshots": [],
            "trials": [],
        },
    )


class FakeProvider:
    def __init__(self, ambiguous=False, failed_command=False, acknowledge_only=False):
        self.active = {}
        self.creates = []
        self.deletes = []
        self.peak = 0
        self.ambiguous, self.failed_command, self.acknowledge_only = (
            ambiguous,
            failed_command,
            acknowledge_only,
        )

    async def create(self, name, ref=None):
        self.active[name] = {"id": name, "name": name}
        self.creates.append(name)
        self.peak = max(self.peak, len(self.active))
        if self.ambiguous:
            raise TimeoutError("response lost after allocation")
        await asyncio.sleep(0)
        return name

    async def exec(self, sid, cmd, timeout=180):
        if "monotonic" in cmd:
            return Command('{"monotonic": 1}')
        return Command(
            "COMMANDS_OK\n" if not self.failed_command else "bad", exit_code=0
        )

    async def inventory(self):
        return list(self.active.values())

    async def absent(self, sid):
        return sid not in self.active

    async def delete(self, sid):
        self.deletes.append(sid)
        if not self.acknowledge_only:
            self.active.pop(sid)


@pytest.mark.parametrize("provider", SPECS)
def test_generated_configs_work_without_credentials(tmp_path, provider, monkeypatch):
    for key in SPECS[provider].env:
        monkeypatch.delenv(key, raising=False)
    c = config(tmp_path, provider)
    result = doctor(c)
    assert set(result["missing_environment_variables"]) == set(SPECS[provider].env)
    assert result["maximum_owned_vms"] == 5


def test_no_silent_lifecycle_downgrade(tmp_path):
    path = tmp_path / "test.toml"
    path.write_text(example("boat").replace('["fresh"]', '["memory"]'))
    with pytest.raises(ValueError, match="Unsupported"):
        configuration(path)


async def test_batches_preserve_all_attempts_and_clean_every_id(tmp_path):
    c = config(tmp_path, count=7, batch_size=3, concurrency=2)
    e, p = evidence(tmp_path, c), FakeProvider()
    await Runner(c, p, e).run()
    report = summarize(e.state)
    assert len(p.creates) == len(set(p.creates)) == 7
    assert p.peak <= 3
    assert len(p.deletes) == 7 and not p.active
    assert report["cases"]["fresh"]["attempted"] == 7
    assert complete(report, c)


async def test_ambiguous_create_is_not_replayed_and_is_reconciled_for_cleanup(tmp_path):
    c = config(tmp_path, count=5, batch_size=1, concurrency=1)
    e, p = evidence(tmp_path, c), FakeProvider(ambiguous=True)
    with pytest.raises(RuntimeError, match="Unconfirmed"):
        await Runner(c, p, e).run()
    report = summarize(e.state)
    assert len(p.creates) == 1
    assert p.deletes == p.creates and not p.active
    assert report["cases"]["fresh"]["attempted"] == 1
    assert report["cases"]["fresh"]["unrun"] == 4
    assert not complete(report, c)


async def test_workload_failure_is_not_a_pass_or_replaced_case(tmp_path):
    c = config(tmp_path, count=2)
    e, p = evidence(tmp_path, c), FakeProvider(failed_command=True)
    await Runner(c, p, e).run()
    report = summarize(e.state)
    assert report["cases"]["fresh"]["failed"] == 2
    assert len(p.creates) == 2 and not complete(report, c)


def test_lock_refuses_second_controller(tmp_path):
    e = evidence(tmp_path, config(tmp_path))
    e.save()
    with pytest.raises(BlockingIOError):
        Evidence(e.path)


def test_dispatch_journal_recovers_ownership_and_preserves_newer_checkpoint(tmp_path):
    e = evidence(tmp_path, config(tmp_path))
    e.save()
    checkpoint = (e.path / "run.json").read_bytes()
    operation = {
        "operation_id": "create-1",
        "kind": "create",
        "status": "dispatched",
        "name": "owned-vm",
    }
    e.save_update("operation", operation["operation_id"], operation)
    assert Evidence.read_state(e.path)["operations"][0]["status"] == "dispatched"
    operation.update(status="returned", result="owned-vm")
    e.save_update("operation", operation["operation_id"], operation)
    trial = {
        "case": "fresh", "index": 1, "status": "passed", "sandbox_id": "owned-vm"
    }
    e.save_trial(trial)
    snapshot = {
        "operation_id": "snapshot-1",
        "kind": "snapshot",
        "status": "returned",
        "id": "owned-vm",
        "result": {"kind": "disk", "id": "saved-disk"},
    }
    e.save_update("operation", snapshot["operation_id"], snapshot)
    assert (e.path / "run.json").read_bytes() == checkpoint
    e.lock.close()

    recovered = Evidence(e.path)
    assert recovered.state["owned"]["owned-vm"]["deleted"] is False
    assert recovered.state["trials"] == [trial]
    assert recovered.state["snapshots"] == [
        {"kind": "disk", "id": "saved-disk", "source_id": "owned-vm"}
    ]
    recovered.state["trials"][0]["activity"] = {"passed": True}
    recovered.state["owned"]["owned-vm"]["deleted"] = True
    recovered.save()
    final = Evidence.read_state(e.path)
    assert final["trials"][0]["activity"]["passed"] is True
    assert final["owned"]["owned-vm"]["deleted"] is True


def test_redaction_covers_nested_errors_and_signed_urls(tmp_path, monkeypatch):
    monkeypatch.setenv("BOAT_API_KEY", "test-secret-token")
    e = evidence(tmp_path, config(tmp_path))
    e.log(
        "error",
        nested={
            "error": "Bearer test-secret-token",
            "url": "https://example.com/a?sig=SECRET&key=SECRET",
        },
    )
    text = (e.path / "events.jsonl").read_text()
    assert "test-secret-token" not in text and "SECRET" not in text
    assert "[redacted]" in text


async def test_boat_request_is_single_attempt_and_has_org_header(tmp_path, monkeypatch):
    monkeypatch.setenv("BOAT_API_KEY", "test")
    monkeypatch.setenv("BOAT_ORG", "team-test")
    p = Boat(config(tmp_path))
    await p.http.aclose()
    requests = []

    async def fail(request):
        requests.append(request)
        raise httpx.ReadTimeout("after dispatch")

    p.http = httpx.AsyncClient(
        base_url="https://boat.dev/api/v1",
        transport=httpx.MockTransport(fail),
        headers={"X-Boat-Org": "team-test"},
    )
    with pytest.raises(httpx.ReadTimeout):
        await p.create("my-test")
    assert len(requests) == 1
    assert requests[0].headers["X-Boat-Org"] == "team-test"
    assert json.loads(requests[0].content)["noEnv"] is True
    await p.close()


async def test_exe_requires_exit_frame(tmp_path, monkeypatch):
    monkeypatch.setenv("EXE_API_KEY", "test")
    p = Exe(config(tmp_path, "exe"))
    await p.http.aclose()
    p.http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, text='{"stdout":"eA=="}\n')
        )
    )
    with pytest.raises(ProviderError, match="no exit code"):
        await p.exec("vm", "echo hi")
    await p.close()


async def test_tensorlake_inventory_uses_actual_sdk_iterator():
    from tensorlake._tracing import TracedIterator

    p = object.__new__(Tensorlake)

    async def listing():
        return TracedIterator(
            "trace", [SimpleNamespace(sandbox_id="id", name="name", status="RUNNING")]
        )

    p.client = SimpleNamespace(list=listing)
    assert await p.inventory() == [{"id": "id", "name": "name", "status": "RUNNING"}]


@pytest.mark.parametrize(
    "error_code,attempts,passed", [("SlowDown", 2, True), ("AccessDenied", 1, False)]
)
async def test_portable_s3_read_retries_only_eligible_failures(
    tmp_path, monkeypatch, error_code, attempts, passed
):
    from botocore.exceptions import ClientError

    from sandbox_bench import runner

    calls, bodies = [], []

    def get(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            error = ClientError(
                {"Error": {"Code": error_code}, "ResponseMetadata": {}}, "GetObject"
            )
            error.response["ResponseMetadata"] = None
            raise error
        body = io.BytesIO(b"content")
        bodies.append(body)
        return {"Body": body, "ResponseMetadata": None}

    monkeypatch.setattr(runner.time, "sleep", lambda _: None)
    fixture = object.__new__(S3Fixture)
    fixture.config = {"bucket": "bucket"}
    fixture.evidence = evidence(tmp_path, config(tmp_path))
    fixture.client = SimpleNamespace(get_object=get)
    result = await fixture.verify(
        {"key": "a", "bytes": 7, "sha256": hashlib.sha256(b"content").hexdigest()}
    )
    assert len(calls) == attempts and result["passed"] == passed
    assert not result["first_attempt_passed"]
    assert all(body.closed for body in bodies)


async def test_deletion_acknowledgement_is_not_cleanup_proof(tmp_path, monkeypatch):
    from sandbox_bench import runner

    c = config(tmp_path)
    e, p = evidence(tmp_path, c), FakeProvider(acknowledge_only=True)
    p.active["owned"] = {"id": "owned", "name": "owned"}
    e.state["owned"]["owned"] = {"deleted": False}

    async def no_sleep(_):
        pass

    monkeypatch.setattr(runner.asyncio, "sleep", no_sleep)
    await Runner(c, p, e).cleanup_ids(["owned"])
    assert not e.state["owned"]["owned"]["deleted"]
    assert "independently" in e.state["owned"]["owned"]["cleanup_error"]


async def test_boat_stop_polls_state_without_replaying_stop(tmp_path, monkeypatch):
    monkeypatch.setenv("BOAT_API_KEY", "test")
    monkeypatch.setenv("BOAT_ORG", "team-test")
    p = Boat(config(tmp_path))
    await p.http.aclose()
    seen = []

    def request(req):
        seen.append((req.method, req.url.path))
        return httpx.Response(200, json={"sandbox": {"state": "archived"}})

    p.http = httpx.AsyncClient(
        base_url="https://boat.dev/api/v1", transport=httpx.MockTransport(request)
    )
    await p.pause("vm")
    assert [x[0] for x in seen] == ["POST", "GET"]
    await p.close()


async def test_cleanup_does_not_replay_unconfirmed_delete(tmp_path):
    c = config(tmp_path)
    e, p = evidence(tmp_path, c), FakeProvider()
    p.active["owned"] = {"id": "owned", "name": "owned"}
    e.state["owned"]["owned"] = {"deleted": False}
    e.state["operations"] = [{"kind": "delete", "id": "owned", "status": "unconfirmed"}]
    await Runner(c, p, e).cleanup_ids(["owned"])
    assert p.deletes == []
    assert not e.state["owned"]["owned"]["deleted"]


async def test_modal_disk_cleanup_uses_existing_rpc_messages(monkeypatch):
    from modal.exception import NotFoundError
    from modal.client import _Client

    from sandbox_bench.providers import Modal

    calls = []

    async def lookup(req):
        calls.append(("lookup", req.image_id))
        raise NotFoundError("gone")

    async def delete(req):
        calls.append(("delete", req.image_id))

    async def client():
        return SimpleNamespace(
            _stub=SimpleNamespace(ImageFromId=lookup, ImageDelete=delete)
        )

    monkeypatch.setattr(_Client, "from_env", client)
    p = object.__new__(Modal)
    ref = {"id": "im-test", "kind": "disk"}
    await p.delete_snapshot(ref)
    assert await p.snapshot_absent(ref)
    assert calls == [("delete", "im-test"), ("lookup", "im-test")]


async def test_readiness_stops_on_blaxel_style_forbidden(tmp_path):
    c = config(tmp_path)
    e, p = evidence(tmp_path, c), FakeProvider()
    calls = []

    class Forbidden(Exception):
        status = 403

    async def forbidden(*args):
        calls.append(args)
        raise Forbidden("denied")

    p.exec = forbidden
    import time

    with pytest.raises(Forbidden):
        await Runner(c, p, e).terminal("vm", {"t0": time.monotonic()})
    assert len(calls) == 1


def test_shared_bundle_clones_its_pinned_commit_without_private_branch(tmp_path):
    import subprocess
    import importlib.util
    from sandbox_bench.runner import assets

    spec = importlib.util.spec_from_file_location(
        "guest_workload", assets() / "sandbox_workload.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    SandboxWorkload = module.SandboxWorkload
    worker = object.__new__(SandboxWorkload)
    worker.config = json.loads((assets() / "manifest.example.json").read_text())
    worker.app_dir = tmp_path / "app"

    def command(args, cwd=None, timeout=60):
        args = [
            str(assets() / "shared-app.bundle")
            if a == "/opt/bench/shared-app.bundle"
            else a
            for a in args
        ]
        return subprocess.run(
            args, cwd=cwd, timeout=timeout, capture_output=True, text=True, check=True
        ).stdout.strip()

    worker.run_command = command
    result = worker.clone_test_repository()
    assert result["commit"] == worker.config["commit"]
    assert (worker.app_dir / "api/index.mjs").exists()


async def test_cancelled_trial_is_visible_and_its_vm_is_cleaned(tmp_path):
    c = config(tmp_path, count=1, batch_size=1, concurrency=1)
    e, p = evidence(tmp_path, c), FakeProvider()
    started = asyncio.Event()

    async def paused(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    p.exec = paused
    task = asyncio.create_task(Runner(c, p, e).run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert e.state["trials"][0]["status"] == "interrupted"
    assert not p.active
    assert summarize(e.state)["cases"]["fresh"]["interrupted"] == 1


async def test_activity_drains_late_final_check_without_turning_it_into_a_pass(
    tmp_path, monkeypatch
):
    from sandbox_bench import runner
    from datetime import datetime, timezone

    c = config(tmp_path, suite="workflow")
    e = evidence(tmp_path, c)
    r = Runner(c, FakeProvider(), e, SimpleNamespace(verify=None))
    calls = []
    events = [
        {
            "passed": True,
            "s3_write": {},
            "external_s3": {"passed": True},
            "start_lateness_s": 0,
            "at": datetime.fromtimestamp(1006 + i * 5, timezone.utc).isoformat(),
        }
        for i in range(12)
    ]
    events[-1]["at"] = datetime.fromtimestamp(1070, timezone.utc).isoformat()

    async def guest(sid, payload, timeout=600):
        calls.append(payload["op"])
        if payload["op"] == "activity":
            return {"scheduled_at": 1000}
        done = calls.count("progress") > 1
        return {"activity": {"events": events if done else events[:-1], "done": done}}

    async def sleep(_):
        return

    monkeypatch.setattr(runner.time, "time", lambda: 1000)
    monkeypatch.setattr(runner.asyncio, "sleep", sleep)
    r.guest = guest
    row = {"status": "passed", "sandbox_id": "vm"}
    await r.activity([row])
    assert calls == ["activity", "progress", "progress"]
    assert len(row["activity"]["events"]) == 12
    assert row["activity"]["done"] and not row["activity"]["passed"]


@pytest.mark.parametrize(
    "help_text,compat",
    [
        ("   compat_dir (enable support)\n", True),
        ("   notsup_compat_dir (disable support)\n", False),
    ],
)
def test_disk_restore_recreates_mount_directory(
    tmp_path, monkeypatch, help_text, compat
):
    import importlib.util
    from sandbox_bench.runner import assets

    spec = importlib.util.spec_from_file_location(
        "guest_mount_test", assets() / "sandbox_workload.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    worker = object.__new__(module.SandboxWorkload)
    worker.config = {
        "bucket": "bucket",
        "provider": "boat",
        "s3_prefixes": {"boat": "prefix/"},
        "region": "us-east-1",
    }
    worker.evidence_dir = tmp_path
    mount = tmp_path / "mount"
    real_path = module.Path
    monkeypatch.setattr(
        module, "Path", lambda p: mount if str(p) == "/mnt/library" else real_path(p)
    )
    observations = iter([False, True])
    worker.is_s3_mounted = lambda: next(observations)
    monkeypatch.setattr(
        module.subprocess, "run", lambda *a, **kw: SimpleNamespace(stdout=help_text)
    )

    def start(*args, **kwargs):
        assert mount.is_dir()
        assert ("compat_dir" in args[0]) is compat
        return SimpleNamespace(pid=10, poll=lambda: None)

    monkeypatch.setattr(module.subprocess, "Popen", start)
    assert worker.ensure_s3_mount()["pid"] == 10


async def test_tensorlake_service_uses_native_process_lifetime():
    from unittest.mock import AsyncMock

    p = object.__new__(Tensorlake)
    start = AsyncMock(return_value=SimpleNamespace(pid=246))
    command_run = AsyncMock(side_effect=AssertionError("short-lived exec used"))
    p.handles = {"sandbox": SimpleNamespace(start_process=start, run=command_run)}
    assert (
        await p.start_service("sandbox", "python worker.py", "/var/log/worker.log")
        == 246
    )
    args, kwargs = start.call_args
    assert args == ("bash", ["-lc", "python worker.py >> /var/log/worker.log 2>&1"])
    assert "timeout" not in kwargs and kwargs["restart"] == {"policy": "never"}
    command_run.assert_not_called()


def test_aws_redaction_preserves_region_for_later_cleanup(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    import boto3

    c = config(tmp_path, suite="workflow")
    c["s3"] = {
        "profile": "temporary-role",
        "region": "us-east-1",
        "expected_account": "123456789012",
        "bucket": "test-bucket",
        "prefix": "test/",
        "seed": "seed.txt",
    }
    e = evidence(tmp_path, c)
    frozen = SimpleNamespace(
        access_key="test-access-key",
        secret_key="test-secret-key",
        token="test-session-token",
    )
    credentials = SimpleNamespace(
        _expiry_time=datetime.now(timezone.utc) + timedelta(hours=1),
        get_frozen_credentials=lambda: frozen,
    )
    s3 = SimpleNamespace(
        get_object=lambda **kw: {"Body": io.BytesIO(b"seed")},
        generate_presigned_url=lambda *a, **kw: (
            "https://example.com/seed?signature=test"
        ),
    )
    session = SimpleNamespace(
        get_credentials=lambda: credentials,
        client=lambda name, **kw: (
            SimpleNamespace(get_caller_identity=lambda: {"Account": "123456789012"})
            if name == "sts"
            else s3
        ),
    )
    monkeypatch.setattr(boto3, "Session", lambda **kw: session)
    fixture = S3Fixture(c["s3"], e)
    e.state["guest_environment"] = fixture.env
    e.save()
    saved = json.loads((e.path / "run.json").read_text())
    assert saved["config"]["s3"]["region"] == "us-east-1"
    assert saved["guest_environment"]["AWS_DEFAULT_REGION"] == "us-east-1"
    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "N100_PROBE_URL",
    ]:
        assert saved["guest_environment"][key] == "[redacted]"


def test_saved_state_check_deadline_clears_after_failure(monkeypatch):
    import importlib.util
    from sandbox_bench.runner import assets

    spec = importlib.util.spec_from_file_location(
        "guest_deadline_test", assets() / "sandbox_workload.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    worker = object.__new__(module.SandboxWorkload)
    worker.set_current_step = lambda name: None
    alarms = []
    monkeypatch.setattr(module.signal, "alarm", alarms.append)

    def blocked_mount():
        assert alarms[-1] == 30
        raise TimeoutError("preserved FUSE read stalled")

    worker._capture_session_state = blocked_mount
    with pytest.raises(TimeoutError, match="preserved FUSE"):
        worker.capture_session_state()
    assert alarms == [30, 0]


async def test_guest_rpc_survives_native_exec_chunk_boundaries(tmp_path, monkeypatch):
    import base64

    c = config(tmp_path)
    r = Runner(c, FakeProvider(), evidence(tmp_path, c))
    result = {"activity": [{"detail": "x" * 20000}]}
    payload = base64.b64encode(
        json.dumps({"ok": True, "result": result}).encode()
    ).decode()

    async def chunked_exec(sid, command, timeout):
        assert "b64encode" in command
        return "\n".join(payload[i : i + 16384] for i in range(0, len(payload), 16384))

    monkeypatch.setattr(r, "exec", chunked_exec)
    assert await r.guest("vm", {"op": "progress"}) == result


async def test_saved_state_rpc_has_client_deadline_when_guest_cannot_interrupt(
    tmp_path, monkeypatch
):
    import base64

    c = config(tmp_path)
    r = Runner(c, FakeProvider(), evidence(tmp_path, c))

    async def observe(sid, command, timeout):
        assert "timeout=45" in command
        assert timeout == 55
        return base64.b64encode(b'{"ok":true,"result":{"state":{}}}').decode()

    monkeypatch.setattr(r, "exec", observe)
    assert await r.guest("vm", {"op": "state"}) == {"state": {}}


async def test_modal_snapshot_requests_use_sdk_event_loop(monkeypatch):
    from modal.client import _Client
    from sandbox_bench.providers import Modal

    main_loop = asyncio.get_running_loop()
    calls = []

    async def rpc(request):
        assert asyncio.get_running_loop() is not main_loop
        calls.append(request.image_id)

    async def client():
        return SimpleNamespace(_stub=SimpleNamespace(ImageFromId=rpc, ImageDelete=rpc))

    monkeypatch.setattr(_Client, "from_env", client)
    p = object.__new__(Modal)
    assert not await p.snapshot_absent({"id": "im-test", "kind": "disk"})
    await p.delete_snapshot({"id": "im-test", "kind": "disk"})
    assert calls == ["im-test", "im-test"]
    from modal.exception import NotFoundError

    async def missing(*args):
        raise NotFoundError("deleted")

    monkeypatch.setattr(p, "_image_request", missing)
    assert await p.snapshot_absent({"id": "im-test", "kind": "disk"})


async def test_generated_names_fit_provider_limits_and_distinguish_sources(tmp_path):
    c = config(tmp_path, run_id="123456789abc")
    e, p = evidence(tmp_path, c), FakeProvider()
    r = Runner(c, p, e)
    for case, index in [("fresh", 1000), ("source-disk", 0), ("source-memory", 0)]:
        await r.trial(case, index)
    assert len(set(p.creates)) == 3
    assert all(len(name) <= 24 for name in p.creates)
    await r.cleanup()
