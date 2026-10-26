# Collect guest counters independently of controller polling. summarize_results.py
# uses actual sample timestamps to compute use and expose missing coverage.

import json
import os
import shutil
import time
from pathlib import Path
from datetime import datetime, timezone


def record_resource_usage():
    log_dir = Path("/var/log/bench")
    log_dir.mkdir(exist_ok=True)
    while True:
        sample = {
            "at": datetime.now(timezone.utc).isoformat(),
            "guest_monotonic": time.monotonic(),
            "cpu_count": os.cpu_count(),
            "raw": {},
            "disk": dict(
                zip(["total", "used", "free"], shutil.disk_usage("/workspace"))
            ),
        }
        try:
            sample["stage"] = (log_dir / "phase").read_text()
        except OSError:
            sample["stage"] = "boot"
        # Optional cgroup files differ by environment. Their absence is missing
        # evidence, not a report of zero resource use.
        for path in [
            "/proc/stat",
            "/proc/meminfo",
            "/proc/net/dev",
            "/sys/fs/cgroup/cpu.stat",
            "/sys/fs/cgroup/cpu.max",
            "/sys/fs/cgroup/memory.current",
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory.peak",
        ]:
            try:
                sample["raw"][path] = Path(path).read_text()
            except OSError:
                pass
        sample["mount_present"] = any(
            " /mnt/library " in entry and "fuse.s3fs" in entry
            for entry in Path("/proc/self/mountinfo").read_text().splitlines()
        )
        with (log_dir / "resources.jsonl").open("a") as output_file:
            output_file.write(json.dumps(sample) + "\n")
        # Two seconds is the intended interval. The report does not invent
        # samples during pauses, restores or collection delays.
        time.sleep(2)


if __name__ == "__main__":
    record_resource_usage()
