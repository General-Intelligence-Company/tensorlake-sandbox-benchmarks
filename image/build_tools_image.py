# Install Chromium, Playwright and s3fs before measured launches. This
# image-build duration is outside the sandbox readiness timer.

import json
import time
from pathlib import Path
from tensorlake import Image


def build_tools_image():
    image_name = "tensorlake-benchmark-tools"
    build_started = time.monotonic()
    image = (
        Image(name=image_name, base_image="tensorlake/ubuntu-minimal")
        .run(
            "sudo npm install -g agent-browser@0.39.1 && "
            "sudo chmod +x /usr/lib/node_modules/agent-browser/bin/agent-browser-linux-x64"
        )
        .run("agent-browser install --with-deps")
        .run(
            "sudo apt-get install -y s3fs python3-venv && "
            "sudo python3 -m venv /opt/bench-venv && "
            "sudo /opt/bench-venv/bin/pip install playwright==1.58.0 && "
            "sudo mkdir -p /mnt/library /workspace /opt/bench /var/log/bench && "
            "sudo chown 1000:1000 /workspace"
        )
    )
    result = image.build(
        registered_name=image_name,
        cpus=4,
        memory_mb=8192,
        disk_mb=10240,
        is_public=False,
        docker_config_json="{}",
        verbose=True,
    )
    Path("outputs/benchmark/base-image.json").write_text(
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
    build_tools_image()
