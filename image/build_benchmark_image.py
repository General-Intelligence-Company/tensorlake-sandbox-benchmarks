# Add the workload, its HTTP entrypoint and the resource recorder to the tools image.

import json
import time
from pathlib import Path
from tensorlake import Image


def build_benchmark_image():
    image_name = "tensorlake-benchmark-session"
    build_started = time.monotonic()
    image = (
        Image(name=image_name, base_image="tensorlake-benchmark-tools")
        .copy("run_sandbox_workload.py", "/opt/bench/run_sandbox_workload.py")
        .copy("sandbox_workload.py", "/opt/bench/sandbox_workload.py")
        .copy("record_resource_usage.py", "/opt/bench/record_resource_usage.py")
        .copy("manifest.json", "/opt/bench/manifest.json")
        .copy("shared-app.bundle", "/opt/bench/shared-app.bundle")
        .copy(
            "bench-resource-recorder.service",
            "/etc/systemd/system/bench-resource-recorder.service",
        )
        .run(
            "sudo mkdir -p /etc/systemd/system/multi-user.target.wants && "
            "sudo ln -s /etc/systemd/system/bench-resource-recorder.service "
            "/etc/systemd/system/multi-user.target.wants/bench-resource-recorder.service && "
            "sudo chmod 644 /opt/bench/*"
        )
    )
    result = image.build(
        context_dir="context",
        registered_name=image_name,
        cpus=4,
        memory_mb=8192,
        disk_mb=10240,
        is_public=False,
        docker_config_json="{}",
        verbose=True,
    )
    Path("outputs/benchmark/image.json").write_text(
        json.dumps(
            {
                "name": image_name,
                "result": result,
                "elapsed_s": time.monotonic() - build_started,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    build_benchmark_image()
