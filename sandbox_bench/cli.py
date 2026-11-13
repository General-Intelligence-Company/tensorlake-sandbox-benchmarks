"""Explicit configuration, offline checks and one-shot benchmark execution."""

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import sys
import tomllib
import uuid
from importlib.metadata import version
from pathlib import Path

from .providers import PROVIDERS, SPECS
from .runner import Evidence, Runner, S3Fixture, assets, complete, now, summarize


def example(provider):
    return f'''# Keep credentials in environment variables, never in this file.
provider = "{provider}"
suite = "commands" # "workflow" also tests Chromium, the app, and mounted S3.
prepared = true # Build tools once, before measured workflow cases.
count = 5
batch_size = 5
concurrency = 5
lifecycles = ["fresh"]
cpus = 4
memory_mb = 8192
disk_mb = 20480
ttl_seconds = 3600

[provider_options]
# Optional prepared tools image; cold bootstrap time is otherwise included.
image = ""
region = "us-was-1" # Blaxel region
machine_type = "default" # Boat machine type; confirm its shape for your org.
modal_app = "sandbox-bench"

# Needed only for suite = "workflow". The seed must already exist.
[s3]
profile = "YOUR_AWS_PROFILE"
expected_account = "YOUR_12_DIGIT_AWS_ACCOUNT"
bucket = "YOUR_TEST_BUCKET"
region = "us-east-1"
prefix = "sandbox-bench/{provider}/"
seed = "readme.txt"
'''


def configuration(path):
    raw = tomllib.loads(Path(path).read_text())
    allowed = {
        "provider",
        "suite",
        "prepared",
        "count",
        "batch_size",
        "concurrency",
        "lifecycles",
        "cpus",
        "memory_mb",
        "disk_mb",
        "ttl_seconds",
        "provider_options",
        "s3",
    }
    if set(raw) - allowed:
        raise ValueError(
            "Unknown config fields: " + ", ".join(sorted(set(raw) - allowed))
        )
    c = {
        "suite": "commands",
        "prepared": True,
        "count": 5,
        "batch_size": 5,
        "concurrency": 5,
        "lifecycles": ["fresh"],
        "cpus": 4,
        "memory_mb": 8192,
        "disk_mb": 20480,
        "ttl_seconds": 3600,
        **raw,
    }
    if c.get("provider") not in SPECS:
        raise ValueError("Choose a provider: " + ", ".join(SPECS))
    if c["suite"] not in ("commands", "workflow"):
        raise ValueError("suite must be commands or workflow")
    if type(c["prepared"]) is not bool:
        raise ValueError("prepared must be true or false")
    for key in (
        "count",
        "batch_size",
        "concurrency",
        "cpus",
        "memory_mb",
        "disk_mb",
        "ttl_seconds",
    ):
        if type(c[key]) is not int or c[key] < 1:
            raise ValueError(key + " must be a positive integer")
    if c["count"] > 1000 or c["batch_size"] > 500 or c["concurrency"] > c["batch_size"]:
        raise ValueError(
            "Maximum count is 1000, batch is 500, and concurrency cannot exceed batch_size"
        )
    if not c["lifecycles"] or len(set(c["lifecycles"])) != len(c["lifecycles"]):
        raise ValueError("Choose unique lifecycle names")
    unsupported = set(c["lifecycles"]) - set(SPECS[c["provider"]].lifecycles)
    if unsupported:
        raise ValueError(
            "Unsupported by this adapter: " + ", ".join(sorted(unsupported))
        )
    if "resume" in c["lifecycles"] and "fresh" not in c["lifecycles"]:
        raise ValueError(
            "resume requires fresh in the same run; failed fresh cases remain ineligible"
        )
    if c["suite"] == "commands" and c["lifecycles"] != ["fresh"]:
        raise ValueError(
            "The command smoke suite supports fresh only; use workflow for lifecycle preservation tests"
        )
    options = c.pop("provider_options", {})
    if set(options) - {"image", "region", "machine_type", "modal_app"}:
        raise ValueError("Unknown provider_options field")
    c.update(
        {
            "image": "",
            "region": "us-was-1",
            "machine_type": "default",
            "modal_app": "sandbox-bench",
            **options,
        }
    )
    if c["suite"] == "workflow":
        s3 = c.get("s3", {})
        if set(s3) != {
            "profile",
            "expected_account",
            "bucket",
            "region",
            "prefix",
            "seed",
        } or not all(s3.values()):
            raise ValueError(
                "Workflow needs s3.profile, expected_account, bucket, region, prefix and seed"
            )
        if not s3["expected_account"].isdigit() or len(s3["expected_account"]) != 12:
            raise ValueError("s3.expected_account must be a 12-digit account ID")
        if (
            not s3["prefix"]
            or not s3["prefix"].endswith("/")
            or s3["prefix"].startswith("/")
            or ".." in s3["prefix"].split("/")
        ):
            raise ValueError("Use a nonempty relative S3 prefix ending in /")
    return c


def doctor(c):
    spec = SPECS[c["provider"]]
    missing = [key for key in spec.env if not os.environ.get(key)]
    module = {"beam": "beam"}.get(c["provider"], c["provider"])
    installed = not spec.extra or importlib.util.find_spec(module) is not None
    return {
        "provider": c["provider"],
        "suite": c["suite"],
        "missing_environment_variables": missing,
        "sdk_installed": installed,
        "install": f"pip install -e '.[{spec.extra}]'" if spec.extra else None,
        "credentials_checked": "presence only; no API calls",
        "notes": spec.notes,
        "count_per_lifecycle": c["count"],
        "maximum_owned_vms": min(c["batch_size"], c["count"])
        + int(bool(set(c["lifecycles"]) & {"disk", "memory"}))
        + int(c["prepared"] and c["suite"] == "workflow"),
        "ok": not missing and installed,
    }


async def execute(args):
    if args.command == "cleanup":
        e = Evidence(args.run_dir)
        c = e.state["config"]
    else:
        c = configuration(args.config)
        run_id = uuid.uuid4().hex[:12]
        c["run_id"] = run_id
        e = Evidence(
            Path(args.output) / f"{c['provider']}-{run_id}",
            {
                "run_id": run_id,
                "created_at": now(),
                "status": "prepared",
                "timing_protocol": ("prepared-v1" if c["prepared"] else "cold-v1")
                if c["suite"] == "workflow"
                else "commands-v1",
                "config": c,
                "operations": [],
                "owned": {},
                "snapshots": [],
                "trials": [],
                "source_hashes": {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in assets().iterdir()
                    if p.is_file()
                },
                "controller_hashes": {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in Path(__file__).parent.glob("*.py")
                },
                "python_version": sys.version,
            },
        )
    checked = doctor(c)
    if not checked["ok"]:
        raise ValueError(json.dumps(checked))
    if args.command != "cleanup":
        distribution = {"beam": "beta9", "boat": "httpx", "exe": "httpx"}.get(
            c["provider"], c["provider"]
        )
        e.state["provider_sdk"] = {
            "distribution": distribution,
            "version": version(distribution),
        }
    p = PROVIDERS[c["provider"]](c)
    try:
        try:
            fixture = S3Fixture(c["s3"], e) if c["suite"] == "workflow" else None
        except Exception as exc:
            if args.command != "cleanup":
                raise
            fixture = None
            e.state["s3_cleanup"] = {"error": str(exc)}
        runner = Runner(c, p, e, fixture)
        if args.command == "cleanup":
            await runner.cleanup()
        else:
            print("Run evidence: " + str(e.path), flush=True)
            e.state["image_preparation"] = await p.prepare()
            e.save()
            await runner.run()
    finally:
        await p.close()
        e.save()
        report = summarize(e.state)
        (e.path / "summary.json").write_text(e.clean(report) + "\n")
        print(e.clean(report))
    return 0 if complete(report, c) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("providers", help="List adapters and their exact lifecycle support")
    init = sub.add_parser(
        "init", help="Write a config, without credentials or API calls"
    )
    init.add_argument("--provider", choices=SPECS, required=True)
    init.add_argument("--output", default="benchmark.toml")
    check = sub.add_parser(
        "doctor", help="Validate configuration locally; no provider calls"
    )
    check.add_argument("--config", default="benchmark.toml")
    run = sub.add_parser(
        "run", help="Start a new measured run; creates billable resources"
    )
    run.add_argument("--config", default="benchmark.toml")
    run.add_argument("--output", default="outputs")
    for command in ("report", "cleanup"):
        sub.add_parser(command).add_argument("run_dir")
    args = parser.parse_args()
    try:
        if args.command == "providers":
            print(
                json.dumps(
                    {
                        k: {
                            "credentials": v.env,
                            "lifecycles": v.lifecycles,
                            "notes": v.notes,
                        }
                        for k, v in SPECS.items()
                    },
                    indent=2,
                )
            )
        elif args.command == "init":
            with Path(args.output).open("x") as f:
                f.write(example(args.provider))
            print(args.output)
        elif args.command == "doctor":
            result = doctor(configuration(args.config))
            print(json.dumps(result, indent=2))
            return 0 if result["ok"] else 1
        elif args.command == "report":
            print(
                json.dumps(
                    summarize(Evidence.read_state(args.run_dir)),
                    indent=2,
                )
            )
        else:
            return asyncio.run(execute(args))
    except (ValueError, RuntimeError, OSError, ImportError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
