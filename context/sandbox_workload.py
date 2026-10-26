# One session owns its app, browser, S3 mount and saved-state checks.
# The entrypoint calls this object only from the main worker thread.
from datetime import datetime, timezone
import glob
import hashlib
import json
import os
import secrets
import signal
import socket
import subprocess
import time
import urllib.request
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(b):
    return hashlib.sha256(b).hexdigest()


def redact_secrets(s):
    for k in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "N100_PROBE_URL",
    ]:
        if os.environ.get(k):
            s = s.replace(os.environ[k], "[redacted]")
    return s


class SandboxWorkload:
    def __init__(self, config):
        self.config = config
        self.case_name = "unconfigured"
        self.trial_index = "0"
        self.marker = "unconfigured"
        self.run_id = "unconfigured"
        self.mode = "unconfigured"
        self.evidence_dir = Path("/workspace/bench-evidence")
        self.evidence_dir.mkdir(exist_ok=True)
        self.app_dir = Path("/workspace/source-app")
        self.s3_prefix = f"n100-{self.run_id}/{self.case_name}/{self.trial_index}/"
        self.current_step = "initial"
        self.app_process = None
        self.step_results = []
        self.activity = None
        self.page = None
        self.playwright = None
        self.browser = None
        self.last_s3_write = None
        self.mount_process = None
        self.process_nonce = secrets.token_hex(32)
        self.previous_marker = None

    def raise_step_timeout(self, *_):
        raise TimeoutError(self.current_step + " exceeded its workload deadline")

    def record_event(self, kind, **kw):
        line = json.dumps(
            {"type": kind, "at": utc_now(), "guest_monotonic": time.monotonic(), **kw}
        )
        with (self.evidence_dir / "events.jsonl").open("a") as f:
            f.write(line + "\n")
        print(line, flush=True)

    def record_network_request(self, row):
        row["at"] = utc_now()
        with (self.evidence_dir / "network.jsonl").open("a") as f:
            f.write(json.dumps(row) + "\n")

    def run_command(self, args, timeout=600, cwd=None):
        t = time.monotonic()
        p = subprocess.run(
            args, cwd=cwd, capture_output=True, text=True, timeout=timeout
        )
        row = {
            "argv": args,
            "duration_s": time.monotonic() - t,
            "exit_code": p.returncode,
            "stdout": redact_secrets(p.stdout[-4000:]),
            "stderr": redact_secrets(p.stderr[-4000:]),
        }
        with (self.evidence_dir / "commands.jsonl").open("a") as f:
            f.write(json.dumps(row) + "\n")
        if p.returncode:
            raise RuntimeError(json.dumps(row))
        return p.stdout.strip()

    def is_s3_mounted(self):
        return any(
            (
                " /mnt/library " in x and "fuse.s3fs" in x
                for x in Path("/proc/self/mountinfo").read_text().splitlines()
            )
        )

    def verify_s3_seed(self):
        t = time.monotonic()
        assert self.is_s3_mounted(), "s3fs mount missing"
        b = (Path("/mnt/library") / self.config["seed_relative_path"]).read_bytes()
        assert sha256(b) == self.config["seed_sha256"], "seed hash mismatch"
        return {
            "duration_s": time.monotonic() - t,
            "sha256": sha256(b),
            "bytes": len(b),
            "mount_present": True,
        }

    # Flush through the guest mount; the controller independently verifies these bytes in S3.
    def write_s3_file(self, name):
        t = time.monotonic()
        assert self.is_s3_mounted(), "s3fs mount missing"
        rel = self.s3_prefix + name
        path = Path("/mnt/library") / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        body = (
            self.run_id + ":" + self.trial_index + ":" + os.urandom(16).hex()
        ).encode()
        size = self.config["write_payload_bytes"]
        assert len(body) <= size
        payload = body.ljust(size, b"#")
        write_started = time.monotonic()
        with path.open("wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        write_fsync_s = time.monotonic() - write_started
        read_started = time.monotonic()
        assert path.read_bytes() == payload
        local_readback_s = time.monotonic() - read_started
        return {
            "key": self.config["s3_prefixes"]["tensorlake"] + rel,
            "sha256": sha256(payload),
            "bytes": len(payload),
            "payload_size_bytes": self.config["write_payload_bytes"],
            "write_fsync_s": write_fsync_s,
            "local_readback_s": local_readback_s,
            "duration_s": time.monotonic() - t,
        }

    def read_app_response(self, path="/"):
        t = time.monotonic()
        try:
            with urllib.request.urlopen("http://127.0.0.1:8000" + path, timeout=5) as r:
                b = r.read()
                status = r.status
            row = {
                "target": "app",
                "stage": self.current_step,
                "status": status,
                "duration_s": time.monotonic() - t,
                "bytes": len(b),
            }
            self.record_network_request(row)
            return b.decode()
        except Exception as e:
            self.record_network_request(
                {
                    "target": "app",
                    "stage": self.current_step,
                    "error": redact_secrets(str(e)),
                    "duration_s": time.monotonic() - t,
                }
            )
            raise

    def check_outbound_https(self, ipv6=False):
        t = time.monotonic()
        args = [
            "curl",
            "--noproxy",
            "*",
            "--silent",
            "--show-error",
            "--connect-timeout",
            "3",
            "--max-time",
            "5",
            "-o",
            "/tmp/n100-probe-body",
            "-w",
            "%{json}",
        ]
        if ipv6:
            args.append("--ipv6")
        args.append(os.environ["N100_PROBE_URL"])
        p = subprocess.run(args, capture_output=True, text=True, timeout=7)
        try:
            d = json.loads(p.stdout)
        except Exception:
            d = {}
        allowed = [
            "remote_ip",
            "remote_port",
            "http_code",
            "time_namelookup",
            "time_connect",
            "time_appconnect",
            "time_starttransfer",
            "time_total",
            "size_download",
        ]
        row = {
            "target": "controlled_https_ipv6" if ipv6 else "controlled_https",
            "stage": self.current_step,
            "proxy_route": "direct (--noproxy *)",
            "exit_code": p.returncode,
            "duration_s": time.monotonic() - t,
            **{k: d.get(k) for k in allowed},
        }
        row["sha256"] = (
            sha256(Path("/tmp/n100-probe-body").read_bytes())
            if p.returncode == 0
            else None
        )
        row["passed"] = (
            p.returncode == 0
            and d.get("http_code") == 200
            and (row["sha256"] == self.config["seed_sha256"])
            and (not ipv6 or ":" in d.get("remote_ip", ""))
        )
        if p.returncode:
            row["error"] = redact_secrets(p.stderr)
        self.record_network_request(row)
        return row

    def verify_local_disk_write(self):
        p = Path("/workspace/local-write-marker")
        with p.open("w") as f:
            f.write(self.marker)
            f.flush()
            os.fsync(f.fileno())
        assert p.read_text() == self.marker
        return {"readback_verified": True, "fsync": True}

    def set_current_step(self, value):
        self.current_step = value
        Path("/var/log/bench/phase").write_text(value)

    # Keep each timer around the operation itself. The controller uses its completion time for cumulative readiness.
    def measure_step(self, name, fn):
        self.set_current_step(name)
        t = time.monotonic()
        signal_timeout = 600 if name in ["clone", "install"] else 180
        signal.alarm(signal_timeout)
        try:
            result = fn()
        finally:
            signal.alarm(0)
        event = {
            "type": "step",
            "step": name,
            "duration_s": time.monotonic() - t,
            "at": utc_now(),
            "guest_monotonic": time.monotonic(),
            "result": result,
        }
        self.step_results.append(event)
        self.record_event("step_record", record=event)
        return result

    # Fresh/disk sandboxes may mount S3. Memory forks and resumes must use the preserved mount instead.
    def ensure_s3_mount(self):
        t = time.monotonic()
        if self.is_s3_mounted():
            return {"already_mounted": True, "duration_s": time.monotonic() - t}
        args = [
            "s3fs",
            self.config["bucket"]
            + ":/"
            + self.config["s3_prefixes"]["tensorlake"].rstrip("/"),
            "/mnt/library",
            "-f",
            "-o",
            "use_session_token",
            "-o",
            "url=https://s3.us-east-1.amazonaws.com",
            "-o",
            "endpoint=us-east-1",
            "-o",
            "allow_other",
            "-o",
            "uid=1000",
            "-o",
            "gid=1000",
            "-o",
            "compat_dir",
            "-o",
            "stat_cache_expire=1",
        ]
        log = (self.evidence_dir / "s3fs.log").open("a")
        self.mount_process = subprocess.Popen(args, stdout=log, stderr=log)
        while time.monotonic() - t < 180:
            if self.is_s3_mounted():
                return {
                    "pid": self.mount_process.pid,
                    "duration_s": time.monotonic() - t,
                }
            if self.mount_process.poll() is not None:
                raise RuntimeError("s3fs exited " + str(self.mount_process.returncode))
            time.sleep(0.1)
        raise TimeoutError("mount not ready in 180 seconds")

    # Release the old app port before loading the next deliberate code edit.
    def stop_app(self):
        if self.app_process and self.app_process.poll() is None:
            os.killpg(self.app_process.pid, 15)
            self.app_process.wait(timeout=10)
        end = time.monotonic() + 10
        while time.monotonic() < end:
            try:
                with socket.create_connection(("127.0.0.1", 8000), timeout=0.2):
                    pass
            except OSError:
                return {"port_released": True}
            time.sleep(0.05)
        raise TimeoutError("Old app did not release port 8000")

    # Require this trial's marker so an old process cannot satisfy app readiness.
    def start_app(self):
        log = (self.evidence_dir / "app.log").open("a")
        self.app_process = subprocess.Popen(
            ["npm", "run", "dev"],
            cwd=self.app_dir,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        until = time.monotonic() + 180
        while time.monotonic() < until:
            try:
                assert self.marker in self.read_app_response()
                return {"pid": self.app_process.pid, "marker": self.marker}
            except Exception:
                if self.app_process.poll() is not None:
                    raise RuntimeError("app exited " + str(self.app_process.returncode))
                time.sleep(0.1)
        raise TimeoutError("app not ready")

    # Keep the browser alive after interaction so later snapshot checks can inspect its in-memory state.
    def verify_app_in_browser(self):
        if self.page is None:
            from playwright.sync_api import sync_playwright

            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                executable_path=glob.glob("/root/.agent-browser/browsers/*/chrome")[0],
                headless=True,
                args=["--no-sandbox"],
            )
            self.page = self.browser.new_page()
            self.page.set_default_timeout(180000)

            def record_browser_response(q):
                try:
                    self.record_network_request(
                        {
                            "target": "browser_app",
                            "stage": self.current_step,
                            "url": q.url,
                            "method": q.method,
                            "status": q.response().status if q.response() else None,
                            "duration_s": max(0, q.timing["responseEnd"]) / 1000,
                            "timing_ms": q.timing,
                            "sizes": q.sizes(),
                        }
                    )
                except Exception as e:
                    self.record_network_request(
                        {
                            "target": "browser_instrumentation",
                            "error": redact_secrets(str(e)),
                            "stage": self.current_step,
                        }
                    )

            self.page.on("requestfinished", record_browser_response)
            self.page.on(
                "requestfailed",
                lambda q: self.record_network_request(
                    {
                        "target": "browser_app",
                        "stage": self.current_step,
                        "url": q.url,
                        "error": q.failure,
                    }
                ),
            )
        self.page.goto("http://127.0.0.1:8000", wait_until="networkidle")
        assert (
            self.marker in self.page.title()
            and self.marker in self.page.locator("h1").inner_text()
        )
        self.page.locator("input[name=name]").fill(self.marker)
        self.page.locator("form button").click()
        self.page.wait_for_load_state("networkidle")
        assert (
            self.page.locator("#greeting").inner_text() == "Hello, " + self.marker + "!"
        )
        self.page.locator("#counter").click()
        assert self.page.locator("#counter").inner_text() == "Clicks: 1"
        self.page.evaluate(
            "window.benchNonce=crypto.randomUUID();window.benchObject={value:23}"
        )
        self.page.screenshot(
            path=str(self.evidence_dir / (self.case_name + "-result.png"))
        )
        return {
            "title": self.page.title(),
            "counter": self.page.locator("#counter").inner_text(),
            "verified": True,
        }

    # Pair process IDs with start ticks so reusing a PID cannot look like preserved process state.
    def capture_app_process_ids(self):
        result = {}
        for p in Path("/proc").glob("[0-9]*/cmdline"):
            try:
                args = p.read_bytes()
                if b"node" in args.split(b"\x00")[0] and b"server.mjs" in args:
                    result[p.parent.name] = (
                        p.with_name("stat").read_text().rsplit(")", 1)[1].split()[19]
                    )
            except OSError:
                pass
        return result

    # Observe saved state without navigation, repairs or restarts; the controller compares this before editing.
    def capture_session_state(self):
        assert self.page is not None and self.app_process is not None
        assert self.app_process.poll() is None
        assert self.marker in self.read_app_response()
        saved = (
            Path("/mnt/library")
            / self.last_s3_write["key"].removeprefix(
                self.config["s3_prefixes"]["tensorlake"]
            )
        ).read_bytes()
        return {
            "process_pid": os.getpid(),
            "process_nonce": self.process_nonce,
            "app_pid": self.app_process.pid,
            "app_start": Path("/proc/" + str(self.app_process.pid) + "/stat")
            .read_text()
            .rsplit(")", 1)[1]
            .split()[19],
            "node_processes": self.capture_app_process_ids(),
            "browser": self.page.evaluate(
                '({url:location.href,title:document.title,nonce:window.benchNonce,object:window.benchObject,counter:document.querySelector("#counter").textContent,greeting:document.querySelector("#greeting").textContent})'
            ),
            "marker": self.marker,
            "repo_path": str(self.app_dir),
            "edit_sha256": sha256(
                (self.app_dir / self.config["edit_file"]).read_bytes()
            ),
            "local_marker": Path("/workspace/local-write-marker").read_text(),
            "disk_marker": Path("/workspace/source-disk-marker").read_text()
            if Path("/workspace/source-disk-marker").exists()
            else None,
            "mount_present": self.is_s3_mounted(),
            "seed_sha256": self.verify_s3_seed()["sha256"],
            "saved_s3_sha256": sha256(saved),
            "saved_s3_key": self.last_s3_write["key"],
        }

    def apply_code_change(self):
        p = self.app_dir / self.config["edit_file"]
        text = p.read_text()
        needle = (
            "Sandbox workload: " + self.previous_marker
            if self.mode == "resume"
            else self.config["edit_replace"]
        )
        assert text.count(needle) == 2
        p.write_text(text.replace(needle, "Sandbox workload: " + self.marker))
        return {"replacements": 2}

    def clone_test_repository(self):
        self.run_command(
            [
                "git",
                "clone",
                "--branch",
                "shared-pinned",
                "/opt/bench/shared-app.bundle",
                str(self.app_dir),
            ],
            timeout=600,
        )
        head = self.run_command(["git", "rev-parse", "HEAD"], cwd=self.app_dir)
        assert head == self.config["commit"]
        return {"commit": head, "source": "approved local Git bundle"}

    # The controller verifies preserved state before this method makes any new changes.
    def run_workload(self, config):
        self.mode = config["mode"]
        self.previous_marker = self.marker
        self.case_name = config["case"]
        self.trial_index = str(config["index"])
        self.run_id = config["run"]
        self.marker = config["marker"]
        self.s3_prefix = f"n100-{self.run_id}/{self.case_name}/{self.trial_index}/"
        self.activity = None
        self.evidence_dir = Path("/workspace/bench-evidence") / self.case_name
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.step_results = []
        if self.mode != "resume":
            self.app_dir = Path("/workspace") / (
                "source-app" if self.mode == "source" else "workload-app"
            )
        # 1. Verify writable local disk and the mounted S3 seed.
        self.measure_step("local_write", self.verify_local_disk_write)
        if self.mode in ["memory", "resume"]:
            assert self.is_s3_mounted(), "Preserved mount missing; refusing repair"
            self.measure_step("mount", lambda: {"preserved": True})
        else:
            self.measure_step("mount", self.ensure_s3_mount)
        self.measure_step("s3_read", self.verify_s3_seed)
        # 2. Prepare the repo and dependencies; resumes reuse what was saved.
        if self.mode != "resume":
            self.measure_step("clone", self.clone_test_repository)
        self.measure_step("edit", self.apply_code_change)
        if self.mode != "resume":
            self.measure_step(
                "install",
                lambda: self.run_command(
                    ["npm", "ci", "--no-audit", "--no-fund"],
                    cwd=self.app_dir,
                    timeout=600,
                ),
            )
        else:
            self.measure_step(
                "installed_deps_reused",
                lambda: {
                    "present": (self.app_dir / "node_modules/escape-html").exists()
                },
            )
        # 3. Load the deliberate edit, exercise the browser, then write through S3.
        if self.app_process is not None:
            self.measure_step("app_stop_after_preservation", self.stop_app)
        self.measure_step("app", self.start_app)
        self.measure_step("browser", self.verify_app_in_browser)
        self.last_s3_write = self.measure_step(
            "s3_write", lambda: self.write_s3_file("initial.txt")
        )
        return {
            "steps": self.step_results,
            "state": self.capture_session_state(),
            "guest_monotonic": time.monotonic(),
        }

    # These operation names and response fields are the existing controller protocol.
    def dispatch_workload_request(self, job):
        op = job["op"]
        if op == "setup":
            return self.run_workload(job)
        if op == "state":
            return {
                "state": self.capture_session_state(),
                "guest_monotonic": time.monotonic(),
            }
        if op == "progress":
            return {
                "steps": self.step_results,
                "activity": self.activity,
                "guest_monotonic": time.monotonic(),
            }
        if op == "activity":
            self.activity = {**job, "events": [], "done": False}
            return {"scheduled": True, "guest_monotonic": time.monotonic()}
        if op == "proof_write":
            prior = self.s3_prefix
            self.s3_prefix = (
                f"n100-{job['run']}/preserved-{job['case']}/{job['index']}/"
            )
            try:
                return {
                    "write": self.write_s3_file("proof.txt"),
                    "guest_monotonic": time.monotonic(),
                }
            finally:
                self.s3_prefix = prior
        raise ValueError(op)

    # Run a due activity check on the same thread as Playwright; preserve the shared five-second schedule.
    def run_due_activity_check(self):
        if (
            self.activity
            and (not self.activity["done"])
            and (time.time() >= self.activity["epoch"])
        ):
            i = len(self.activity["events"])
            if (
                i < self.activity["cycles"]
                and time.time() >= self.activity["epoch"] + 5 * i
            ):
                self.set_current_step("activity")
                t = time.monotonic()
                signal.alarm(180)
                try:
                    assert self.marker in self.read_app_response()
                    self.page.locator("#counter").click()
                    expected = "Clicks: " + str(i + 2)
                    assert self.page.locator("#counter").inner_text() == expected
                    read = self.verify_s3_seed()
                    write = self.write_s3_file("activity-" + str(i) + ".txt")
                    probe_result = self.check_outbound_https()
                    event = {
                        "index": i,
                        "passed": probe_result["passed"],
                        "s3_read": read,
                        "s3_write": write,
                        "network_probe": probe_result,
                    }
                except Exception as e:
                    event = {
                        "index": i,
                        "passed": False,
                        "error": redact_secrets(str(e)),
                    }
                finally:
                    signal.alarm(0)
                event.update(
                    at=utc_now(),
                    duration_s=time.monotonic() - t,
                    guest_monotonic=time.monotonic(),
                    scheduled_epoch=self.activity["epoch"] + 5 * i,
                    start_lateness_s=max(
                        0,
                        time.time()
                        - (time.monotonic() - t)
                        - (self.activity["epoch"] + 5 * i),
                    ),
                )
                self.activity["events"].append(event)
                self.record_event("activity", record=event)
            if (
                time.time() >= self.activity["epoch"] + self.activity["duration"]
                and len(self.activity["events"]) == self.activity["cycles"]
            ):
                self.activity["done"] = True
