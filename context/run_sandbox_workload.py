"""Serve controller requests while one main thread owns the sandbox workload."""

import json
import queue
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from sandbox_workload import SandboxWorkload, redact_secrets, utc_now


def process_workload_requests(workload, jobs):
    # Playwright and saved session state stay on this thread. HTTP threads only
    # queue requests and wait for their own reply.
    while True:
        try:
            job, replies = jobs.get(timeout=0.05)
        except queue.Empty:
            job = None
        if job is not None:
            try:
                replies.put(
                    {"ok": True, "result": workload.dispatch_workload_request(job)}
                )
            except Exception as e:
                replies.put(
                    {
                        "ok": False,
                        "error": redact_secrets(repr(e)),
                        "steps": workload.step_results,
                        "at": utc_now(),
                        "stage": workload.current_step,
                    }
                )
        workload.run_due_activity_check()


def main():
    workload = SandboxWorkload(json.loads(Path("/opt/bench/manifest.json").read_text()))
    signal.signal(signal.SIGALRM, workload.raise_step_timeout)
    jobs = queue.Queue()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            job = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            reply = queue.Queue()
            jobs.put((job, reply))
            value = reply.get()
            body = json.dumps(value).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    # Tensorlake authenticates port 8123 upstream; no provider key enters the guest.
    web = ThreadingHTTPServer(("0.0.0.0", 8123), Handler)
    threading.Thread(target=web.serve_forever, daemon=True).start()
    process_workload_requests(workload, jobs)


if __name__ == "__main__":
    main()
