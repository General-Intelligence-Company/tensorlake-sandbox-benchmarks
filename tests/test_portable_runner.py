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
    from grpclib.const import Status
    from grpclib.exceptions import GRPCError
    from modal.client import _Client

    from sandbox_bench.providers import Modal

    calls = []

    async def lookup(req):
        calls.append(("lookup", req.image_id))
        raise GRPCError(Status.NOT_FOUND, "gone")

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
