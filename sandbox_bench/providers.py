"""Provider boundaries. Lifecycle mutations are submitted once, never retried here."""

import asyncio
import base64
import os
import shlex
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx


@dataclass(frozen=True)
class ProviderSpec:
    env: tuple[str, ...]
    extra: str | None
    lifecycles: tuple[str, ...]
    notes: str


SPECS = {
    "tensorlake": ProviderSpec(
        ("TENSORLAKE_API_KEY",),
        "tensorlake",
        ("fresh", "disk", "memory", "resume"),
        "Memory snapshots require account access; resume keeps the sandbox ID.",
    ),
    "modal": ProviderSpec(
        ("MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET"),
        "modal",
        ("fresh", "disk", "memory"),
        "Memory snapshots require access and stop the source; restoring creates a new ID. No same-ID resume in this adapter.",
    ),
    "boat": ProviderSpec(
        ("BOAT_API_KEY", "BOAT_ORG"),
        None,
        ("fresh", "disk", "resume"),
        "Disk forks use an archived source. Stop/resume reboots from disk; no RAM preservation claim.",
    ),
    "blaxel": ProviderSpec(
        ("BL_API_KEY", "BL_WORKSPACE"),
        "blaxel",
        ("fresh", "memory"),
        "Snapshots capture RAM and filesystem and require preview access. No disk-only fork or explicit pause test in this adapter.",
    ),
    "beam": ProviderSpec(
        ("BEAM_API_KEY",),
        "beam",
        ("fresh", "disk", "resume"),
        "Uses Beam VMs, not Beam serverless Sandboxes. Disk snapshots and same-ID memory-preserving pause/resume.",
    ),
    "exe": ProviderSpec(
        ("EXE_API_KEY",),
        None,
        ("fresh", "disk", "resume"),
        "Uses HTTPS API. The token must permit new, ls, ssh, cp, pause, resume and rm. Disk copies and same-ID resume.",
    ),
}


class ProviderError(RuntimeError):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status_code = status


@dataclass
class Command:
    stdout: str
    stderr: str = ""
    exit_code: int = 0


class Provider:
    """IDs and snapshot references are JSON values persisted in the ownership ledger."""

    resume_memory = True

    def __init__(self, config):
        self.config = config
        self.handles: dict[str, Any] = {}

    async def close(self):
        pass

    async def prepare(self):
        """Optional image build before measured VM creation."""
        return None

    async def start_service(self, sid, command, log_path):
        launcher = (
            "import subprocess;f=open(" + repr(log_path) + ", 'ab');"
            "p=subprocess.Popen("
            + repr(["bash", "-lc", command])
            + ",stdout=f,stderr=f,stdin=subprocess.DEVNULL,start_new_session=True);print(p.pid)"
        )
        result = await self.exec(
            sid, "$(command -v sudo || true) python3 -c " + shlex.quote(launcher), 30
        )
        if result.exit_code != 0:
            raise ProviderError("Service launch failed: " + result.stderr[-1000:])
        return int(result.stdout.strip())

    async def pause(self, sid):
        raise NotImplementedError

    async def resume(self, sid):
        raise NotImplementedError

    async def snapshot(self, sid, kind, name):
        raise NotImplementedError

    async def delete_snapshot(self, ref):
        raise NotImplementedError

    async def snapshot_absent(self, ref):
        raise NotImplementedError


class Tensorlake(Provider):
    def __init__(self, config):
        super().__init__(config)
        from tensorlake.sandbox import AsyncSandboxClient

        self.client = AsyncSandboxClient(
            api_key=os.environ["TENSORLAKE_API_KEY"], max_retries=0, request_timeout=180
        )

    async def create(self, name, ref=None):
        options = dict(
            name=name,
            timeout_secs=self.config["ttl_seconds"],
            startup_timeout=180,
            request_timeout=180,
            cancel_on_timeout=True,
        )
        if ref:
            options["snapshot_id"] = ref["id"]
        else:
            options.update(
                image=self.config.get("image") or "tensorlake/ubuntu-minimal",
                cpus=self.config["cpus"],
                memory_mb=self.config["memory_mb"],
                disk_mb=self.config["disk_mb"],
            )
        sb = await self.client.create_and_connect(**options)
        self.handles[sb.sandbox_id] = sb
        return sb.sandbox_id

    async def exec(self, sid, command, timeout=180):
        if sid not in self.handles:
            self.handles[sid] = await self.client.connect(sid, request_timeout=2500)
        r = await self.handles[sid].run(
            "bash", ["-lc", command], timeout=timeout, user="root"
        )
        return Command(r.stdout, r.stderr, r.exit_code)

    async def start_service(self, sid, command, log_path):
        if sid not in self.handles:
            self.handles[sid] = await self.client.connect(sid, request_timeout=2500)
        # Native process ownership keeps this service beyond a short exec deadline.
        process = await self.handles[sid].start_process(
            "bash",
            ["-lc", command + " >> " + shlex.quote(log_path) + " 2>&1"],
            user="root",
            restart={"policy": "never"},
        )
        return process.pid

    async def inventory(self):
        return [
            {"id": entry.sandbox_id, "name": entry.name, "status": str(entry.status)}
            for entry in await self.client.list()
        ]

    async def absent(self, sid):
        try:
            info = (await self.client.get(sid)).value
            return str(info.status) in (
                "SandboxStatus.TERMINATED",
                "SandboxStatus.FAILED",
            )
        except Exception as exc:
            if type(exc).__name__ == "SandboxNotFoundError":
                return True
            raise

    async def delete(self, sid):
        await self.client.delete(sid)

    async def pause(self, sid):
        await self.client.suspend(sid, timeout=180, poll_interval=1)

    async def resume(self, sid):
        await self.client.resume(sid, timeout=180, poll_interval=1)
        self.handles.pop(sid, None)
        return sid

    async def snapshot(self, sid, kind, name):
        from tensorlake.sandbox import SnapshotType

        value = (
            await self.client.snapshot_and_wait(
                sid,
                snapshot_type=SnapshotType.MEMORY
                if kind == "memory"
                else SnapshotType.FILESYSTEM,
                timeout=180,
                wait_until="completed",
            )
        ).value
        return {"id": value.snapshot_id, "kind": kind}

    async def delete_snapshot(self, ref):
        await self.client.delete_snapshot(ref["id"])

    async def snapshot_absent(self, ref):
        try:
            await self.client.get_snapshot(ref["id"])
            return False
        except Exception as exc:
            if (
                getattr(exc, "status_code", None) == 404
                or "NotFound" in type(exc).__name__
            ):
                return True
            raise


class Modal(Provider):
    def __init__(self, config):
        super().__init__(config)
        import modal

        self.modal = modal
        self.app = None

    async def create(self, name, ref=None):
        m = self.modal
        if ref and ref["kind"] == "memory":
            sb = await m.Sandbox._experimental_from_snapshot.aio(
                m.SandboxSnapshot.from_id(ref["id"]), name=name
            )
        else:
            if self.app is None:
                self.app = await m.App.lookup.aio(
                    self.config["modal_app"], create_if_missing=True
                )
            image = (
                m.Image.from_id(ref["id"])
                if ref
                else (
                    m.Image.from_id(self.config["image"])
                    if self.config.get("image")
                    else m.Image.debian_slim(python_version="3.11")
                )
            )
            sb = await m.Sandbox.create.aio(
                "sleep",
                "infinity",
                app=self.app,
                image=image,
                runtime="vm",
                cpu=self.config["cpus"],
                memory=self.config["memory_mb"],
                timeout=self.config["ttl_seconds"],
                _experimental_enable_snapshot=True,
                name=name,
                tags={"sandbox_bench": self.config["run_id"]},
            )
        self.handles[sb.object_id] = sb
        return sb.object_id

    async def handle(self, sid):
        if sid not in self.handles:
            self.handles[sid] = await self.modal.Sandbox.from_id.aio(sid)
        return self.handles[sid]

    async def exec(self, sid, command, timeout=180):
        p = await (await self.handle(sid)).exec.aio(
            "bash", "-lc", command, timeout=timeout
        )
        stdout, stderr = await asyncio.gather(p.stdout.read.aio(), p.stderr.read.aio())
        await p.wait.aio()
        return Command(stdout, stderr, p.returncode)

    async def inventory(self):
        if self.app is None:
            self.app = await self.modal.App.lookup.aio(
                self.config["modal_app"], create_if_missing=False
            )
        return [
            {
                "id": s.object_id,
                "name": None,
                "status": "active",
                "owned_run": self.config["run_id"],
            }
            async for s in self.modal.Sandbox.list.aio(
                app_id=self.app.app_id, tags={"sandbox_bench": self.config["run_id"]}
            )
        ]

    async def absent(self, sid):
        return await (await self.handle(sid)).poll.aio() is not None

    async def delete(self, sid):
        await (await self.handle(sid)).terminate.aio()

    async def snapshot(self, sid, kind, name):
        sb = await self.handle(sid)
        if kind == "memory":
            snap = await sb._experimental_snapshot.aio()
        else:
            snap = await sb.snapshot_filesystem.aio(
                timeout=180, ttl=self.config["ttl_seconds"]
            )
        return {"id": snap.object_id, "kind": kind}

    async def _image_request(self, method, image_id):
        from modal._utils.async_utils import synchronizer
        from modal.client import _Client
        from modal_proto import api_pb2

        # The shared Modal client belongs to its SDK event loop, not ours.
        @synchronizer.create_blocking
        async def request():
            client = await _Client.from_env()
            return await getattr(client._stub, method)(
                getattr(api_pb2, method + "Request")(image_id=image_id)
            )

        return await request.aio()

    async def delete_snapshot(self, ref):
        if ref["kind"] == "memory":
            raise ProviderError(
                "Modal SDK exposes no memory snapshot deletion API; snapshot expires after seven days"
            )
        await self._image_request("ImageDelete", ref["id"])

    async def snapshot_absent(self, ref):
        if ref["kind"] == "memory":
            raise ProviderError(
                "Modal memory snapshot expiry requires manual verification"
            )
        from modal.exception import NotFoundError

        try:
            await self._image_request("ImageFromId", ref["id"])
            return False
        except NotFoundError:
            return True


class Blaxel(Provider):
    def __init__(self, config):
        super().__init__(config)
        os.environ["BL_SANDBOX_READ_RETRIES"] = "0"
        os.environ["BL_FS_PART_RETRIES"] = "0"
        from blaxel.core import SandboxInstance, Snapshot
        from blaxel.core.client.client import client

        self.Sandbox, self.Snapshot, self.client = SandboxInstance, Snapshot, client

    async def create(self, name, ref=None):
        if ref:
            snapshot = await self.Snapshot.get(ref["id"])
            await snapshot.fork(name)
        else:
            await self.Sandbox.create(
                {
                    "name": name,
                    "image": self.config.get("image") or "blaxel/base-image:latest",
                    "memory": self.config["memory_mb"],
                    "region": self.config["region"],
                    "ttl": str(self.config["ttl_seconds"]) + "s",
                    "labels": {"sandbox_bench": self.config["run_id"]},
                },
                timeout=180,
            )
        return name

    async def exec(self, sid, command, timeout=180):
        sb = await self.Sandbox.get(sid)
        # Explicitly forward workspace authentication using the supported SDK.
        # Do not replace its client with a custom unauthenticated HTTP session.
        p = await sb.process.exec(
            {
                "name": "bench-" + str(time.time_ns()),
                "command": command,
                "wait_for_completion": True,
                "timeout": timeout,
                "keep_alive": False,
            }
        )
        return Command(p.stdout or "", p.stderr or "", p.exit_code)

    async def inventory(self):
        page = await self.Sandbox.list()
        return [
            {"id": s.metadata.name, "name": s.metadata.name, "status": str(s.status)}
            async for s in page.auto_paging_iter()
        ]

    async def absent(self, sid):
        r = await self.client.get_async_httpx_client().get(
            "/sandboxes/" + quote(sid, safe=""), headers={"Cache-Control": "no-cache"}
        )
        if r.status_code == 404:
            return True
        r.raise_for_status()
        return r.json().get("status") == "TERMINATED"

    async def delete(self, sid):
        await self.Sandbox.delete(sid)

    async def snapshot(self, sid, kind, name):
        sb = await self.Sandbox.get(sid)
        snap = await sb.snapshots.create(name)
        deadline = time.monotonic() + 180
        while str(snap.status).lower() != "ready":
            if (
                str(snap.status).lower() in ("failed", "error")
                or time.monotonic() > deadline
            ):
                raise ProviderError("Snapshot did not become ready")
            await asyncio.sleep(1)
            snap = await self.Snapshot.get(snap.id)
        return {"id": snap.id, "kind": "memory"}

    async def delete_snapshot(self, ref):
        await self.Snapshot.delete(ref["id"])

    async def snapshot_absent(self, ref):
        r = await self.client.get_async_httpx_client().get(
            "/snapshots/" + quote(ref["id"], safe="")
        )
        if r.status_code != 404:
            r.raise_for_status()
        return r.status_code == 404


class Boat(Provider):
    resume_memory = False

    def __init__(self, config):
        super().__init__(config)
        self.http = httpx.AsyncClient(
            base_url="https://boat.dev/api/v1",
            timeout=180,
            headers={
                "Authorization": "Bearer " + os.environ["BOAT_API_KEY"],
                "X-Boat-Org": os.environ["BOAT_ORG"],
            },
        )

    async def request(self, method, path, body=None):
        headers = (
            {"X-Ascii-Confirm-Delete": path.rsplit("/", 1)[-1]}
            if method == "DELETE"
            else {}
        )
        r = await self.http.request(method, path, json=body, headers=headers)
        if r.is_error:
            raise ProviderError(r.text[:2000], r.status_code)
        return r.json() if r.content else {}

    async def create(self, name, ref=None):
        body = {
            "type": self.config["machine_type"],
            "ttlSeconds": self.config["ttl_seconds"],
            "noEnv": True,
            "monitoring": True,
            "snapshots": True,
            "name": name,
            "org": os.environ["BOAT_ORG"],
        }
        if self.config.get("image"):
            body["from"] = self.config["image"]
        path = (
            "/sandboxes/" + quote(ref["id"], safe="") + "/fork" if ref else "/sandboxes"
        )
        value = await self.request("POST", path, body)
        return value["sandbox"]["id"]

    async def exec(self, sid, command, timeout=180):
        r = await self.request(
            "POST",
            "/sandboxes/" + sid + "/commands",
            {
                "command": command,
                "timeoutSeconds": min(600, int(timeout)),
                "detached": False,
            },
        )
        return Command(r.get("stdout", ""), r.get("stderr", ""), r.get("exitCode", 1))

    async def inventory(self):
        r = await self.request("GET", "/sandboxes")
        return [
            {"id": s["id"], "name": s.get("name"), "status": s.get("state")}
            for s in r["sandboxes"]
        ]

    async def absent(self, sid):
        try:
            await self.request("GET", "/sandboxes/" + sid)
            return False
        except ProviderError as exc:
            if exc.status_code in (404, 410):
                return True
            raise

    async def delete(self, sid):
        await self.request("DELETE", "/sandboxes/" + sid)

    async def pause(self, sid):
        await self.request("POST", "/sandboxes/" + sid + "/stop", {})
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            value = await self.request("GET", "/sandboxes/" + sid)
            if value["sandbox"]["state"] == "archived":
                return
            await asyncio.sleep(1)
        raise TimeoutError("Boat stop did not reach archived")

    async def resume(self, sid):
        value = await self.request(
            "POST",
            "/sandboxes/" + sid + "/resume",
            {
                "type": self.config["machine_type"],
                "ttlSeconds": self.config["ttl_seconds"],
                "noEnv": True,
            },
        )
        return value["sandbox"]["id"]

    async def snapshot(self, sid, kind, name):
        await self.pause(sid)
        return {"id": sid, "kind": "disk", "retains_source": True}

    async def delete_snapshot(self, ref):
        # Boat's unnamed disk fork source is itself an owned VM.
        pass

    async def snapshot_absent(self, ref):
        return await self.absent(ref["id"])

    async def close(self):
        await self.http.aclose()


class Exe(Provider):
    def __init__(self, config):
        super().__init__(config)
        self.http = httpx.AsyncClient(
            timeout=660,
            headers={"Authorization": "Bearer " + os.environ["EXE_API_KEY"]},
        )

    async def api(self, command, *, ndjson=False):
        import json

        r = await self.http.post(
            "https://exe.dev/exec",
            content=command,
            headers={"Accept": "application/x-ndjson"} if ndjson else None,
        )
        if r.is_error:
            raise ProviderError(r.text[:2000], r.status_code)
        try:
            result = r.json()
        except json.JSONDecodeError:
            result = None  # Guest command output is a stream of JSON objects.
        if isinstance(result, dict) and result.get("error"):
            raise ProviderError(str(result["error"]))
        return r.text

    async def create(self, name, ref=None):
        shape = f" --cpu={self.config['cpus']} --memory={self.config['memory_mb']}MB"
        command = (
            f"cp {shlex.quote(ref['id'])} {name} --standalone"
            if ref
            else f"new --standalone --name={name} --no-email"
        )
        await self.api(command + shape)
        return name

    async def exec(self, sid, command, timeout=180):
        import json

        text = await self.api(
            "ssh " + shlex.quote(sid) + " " + shlex.quote(command), ndjson=True
        )
        stdout, stderr, code = "", "", None
        for line in text.splitlines():
            row = json.loads(line)
            stdout += base64.b64decode(row.get("stdout", "")).decode(errors="replace")
            stderr += base64.b64decode(row.get("stderr", "")).decode(errors="replace")
            if "exit_code" in row:
                code = row["exit_code"]
            if "error" in row:
                raise ProviderError(str(row["error"]))
        if code is None:
            raise ProviderError("Command response has no exit code; outcome unknown")
        return Command(stdout, stderr, code)

    async def inventory(self):
        import json

        return [
            {"id": s["vm_name"], "name": s["vm_name"], "status": s["status"]}
            for s in json.loads(await self.api("ls"))["vms"]
        ]

    async def absent(self, sid):
        return not any(x["id"] == sid for x in await self.inventory())

    async def delete(self, sid):
        await self.api("rm " + shlex.quote(sid))

    async def pause(self, sid):
        await self.api("pause " + shlex.quote(sid))

    async def resume(self, sid):
        await self.api("resume " + shlex.quote(sid))
        return sid

    async def snapshot(self, sid, kind, name):
        await self.pause(sid)
        return {"id": sid, "kind": "disk", "retains_source": True}

    async def delete_snapshot(self, ref):
        pass

    async def snapshot_absent(self, ref):
        return await self.absent(ref["id"])

    async def close(self):
        await self.http.aclose()


class Beam(Provider):
    def __init__(self, config):
        super().__init__(config)
        from beam import VM
        from beta9.config import ConfigContext

        self.VM = VM
        self.context = ConfigContext(
            token=os.environ["BEAM_API_KEY"],
            gateway_host="gateway.beam.cloud",
            gateway_port=443,
            api_url="https://app.beam.cloud",
        )

    async def prepare(self):
        if not self.config.get("image"):

            def build():
                from beam import Image
                from beta9.abstractions.vm import DEFAULT_VM_BASE_IMAGE, prepare_image

                vm = self.vm("image-builder")
                image = Image(base_image=DEFAULT_VM_BASE_IMAGE)
                image.ignore_python = True
                result = prepare_image(image, vm._service, False).build()
                if not result.success:
                    raise ProviderError(result.error or "Beam VM image build failed")
                return result.image_id

            self.config["image"] = await asyncio.to_thread(build)
        return {
            "image": self.config["image"],
            "ownership": "cached provider image; not a measured sandbox",
        }

    def vm(self, sid):
        if sid not in self.handles:
            self.handles[sid] = self.VM(sid, context=self.context)
        return self.handles[sid]

    def request(self, vm, method, path="", body=None):
        http = vm._vm_http or vm._service.http
        r = http.request(
            method,
            "/api/v1/vm/{ws}" + path,
            timeout=180,
            **({"json": body} if body is not None else {}),
        )
        if not r.ok:
            raise ProviderError(r.text[:2000], r.status_code)
        return r.json() if r.content else {}

    async def create(self, name, ref=None):
        import uuid

        vm = self.vm(name)
        body = {
            "name": name,
            "request_id": str(uuid.uuid4()),
            "spec": {
                "cpu": self.config["cpus"] * 1000,
                "memory": self.config["memory_mb"],
                "disk_size": str(self.config["disk_mb"] * 1024 * 1024),
                "desktop": True,
                "ssh": False,
                "idle_timeout": self.config["ttl_seconds"],
                "auto_resume": False,
            },
            "metadata": {"sandbox_bench": self.config["run_id"]},
        }
        if ref:
            body["snapshot"] = ref["id"]
        elif self.config.get("image"):
            body["spec"]["image_id"] = self.config["image"]
        else:
            raise ValueError("Beam image preparation must finish before VM dispatch")
        data = await asyncio.to_thread(self.request, vm, "POST", "", body)
        vm._set(data)
        self.handles[vm.info["id"]] = vm
        return vm.info["id"]

    async def exec(self, sid, command, timeout=180):
        def run():
            import uuid

            from beta9.channel import request_metadata, rpc_timeout
            from beta9.clients.pod import (
                PodSandboxConnectRequest,
                PodSandboxExecRequest,
                PodSandboxStatusRequest,
                PodSandboxStderrRequest,
                PodSandboxStdoutRequest,
                PodServiceStub,
            )

            vm = self.vm(sid)
            vm._set(self.request(vm, "GET", vm._path()))
            cid = vm.info["container_id"]
            stub = PodServiceStub(vm._service.channel)

            def rpc(method, arg):
                token = request_metadata.set(
                    (("x-beta9-request-id", str(uuid.uuid4())),)
                )
                try:
                    with rpc_timeout(min(30, timeout)):
                        result = getattr(stub, method)(arg)
                    if not result.ok:
                        raise ProviderError(result.error_msg)
                    return result
                finally:
                    request_metadata.reset(token)

            rpc("sandbox_connect", PodSandboxConnectRequest(container_id=cid))
            r = rpc(
                "sandbox_exec",
                PodSandboxExecRequest(
                    container_id=cid,
                    command="bash -lc " + shlex.quote(command),
                    wait=False,
                ),
            )
            deadline = time.monotonic() + timeout
            while not r.done:
                status = rpc(
                    "sandbox_status",
                    PodSandboxStatusRequest(container_id=cid, pid=r.pid),
                )
                if status.exit_code >= 0:
                    out = rpc(
                        "sandbox_stdout",
                        PodSandboxStdoutRequest(container_id=cid, pid=r.pid),
                    )
                    err = rpc(
                        "sandbox_stderr",
                        PodSandboxStderrRequest(container_id=cid, pid=r.pid),
                    )
                    return Command(
                        r.stdout + out.stdout, r.stderr + err.stderr, status.exit_code
                    )
                if time.monotonic() > deadline:
                    raise TimeoutError("Beam process did not finish")
                time.sleep(0.25)
            return Command(r.stdout, r.stderr, r.exit_code)

        return await asyncio.to_thread(run)

    async def inventory(self):
        data = await asyncio.to_thread(self.VM.list, context=self.context, all=True)
        return [
            {"id": s["id"], "name": s.get("name"), "status": s.get("status")}
            for s in data
        ]

    async def absent(self, sid):
        try:
            await asyncio.to_thread(
                self.request, self.vm(sid), "GET", "/" + quote(sid, safe="")
            )
            return False
        except ProviderError as exc:
            if exc.status_code == 404:
                return True
            raise

    async def delete(self, sid):
        await asyncio.to_thread(
            self.request, self.vm(sid), "DELETE", "/" + quote(sid, safe="")
        )

    async def pause(self, sid):
        vm = self.vm(sid)
        await asyncio.to_thread(
            self.request, vm, "POST", "/" + quote(sid, safe="") + "/pause", {}
        )
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            value = await asyncio.to_thread(
                self.request, vm, "GET", "/" + quote(sid, safe="")
            )
            if value["status"] == "paused":
                return
            await asyncio.sleep(1)
        raise TimeoutError("Beam pause did not become ready")

    async def resume(self, sid):
        await asyncio.to_thread(
            self.request, self.vm(sid), "POST", "/" + quote(sid, safe="") + "/start", {}
        )
        return sid

    async def snapshot(self, sid, kind, name):
        value = await asyncio.to_thread(
            self.request,
            self.vm(sid),
            "POST",
            "/" + quote(sid, safe="") + "/snapshot",
            {"name": name},
        )
        return {"id": value["id"], "kind": "disk"}

    async def delete_snapshot(self, ref):
        await asyncio.to_thread(
            self.request,
            self.vm(ref["id"]),
            "DELETE",
            "/artifacts/snapshot/" + quote(ref["id"], safe=""),
        )

    async def snapshot_absent(self, ref):
        values = await asyncio.to_thread(
            self.request, self.vm(ref["id"]), "GET", "/artifacts/snapshot"
        )
        return not any(x["id"] == ref["id"] for x in values)

    async def close(self):
        for vm in {id(v): v for v in self.handles.values()}.values():
            vm.close()


PROVIDERS = {
    "tensorlake": Tensorlake,
    "modal": Modal,
    "boat": Boat,
    "blaxel": Blaxel,
    "beam": Beam,
    "exe": Exe,
}
