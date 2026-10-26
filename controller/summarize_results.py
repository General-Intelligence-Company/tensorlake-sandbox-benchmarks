"""Turn saved benchmark evidence into summaries, charts' data and a readable report."""

import csv
import json
import math
import statistics
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


def generate_report():
    # 1. Read one saved run. Original attempts and diagnostic repeats stay separate.
    RESULTS_ROOT = Path(__file__).resolve().parents[1] / "outputs/benchmark"
    RUN_DIR = (
        RESULTS_ROOT / json.loads((RESULTS_ROOT / "current.json").read_text())["run_id"]
    )
    RUN_STATE = json.loads((RUN_DIR / "state.json").read_text())
    requested_n = RUN_STATE["requested_n"]
    CASES = [
        "fresh",
        "disk",
        "memory",
        "resume",
        "fresh_corrected",
        "resume_corrected",
        "disk_corrected",
    ]
    CASE_ROWS = {case: RUN_STATE.get("cases", {}).get(case, []) for case in CASES}

    # Drop missing/nonfinite measurements and report the actual sample count.
    # P95 uses nearest rank; missing timing never becomes zero.
    def summarize_values(values):
        values = sorted(
            float(v) for v in values if v is not None and math.isfinite(float(v))
        )
        return {
            "n": len(values),
            "mean": statistics.mean(values) if values else None,
            "median": statistics.median(values) if values else None,
            "p95": values[math.ceil(0.95 * len(values)) - 1] if values else None,
            "maximum": max(values) if values else None,
        }

    def timestamp_seconds(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()

    def read_json_lines(path):
        if path.exists():
            for line in path.read_text().splitlines():
                if line.strip():
                    yield json.loads(line)

    def write_json_lines(name, rows):
        with (RUN_DIR / (name + ".jsonl")).open("w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    # Separate attempted failures from unrun/blocked work while keeping the
    # original requested cohort denominator.
    def outcome_counts(attempted, passed, blocked=False, requested=requested_n):
        remaining = max(0, requested - attempted)
        return {
            "requested": requested,
            "attempted": attempted,
            "passed": passed,
            "failed": attempted - passed,
            "blocked": remaining if blocked else 0,
            "unrun": 0 if blocked else remaining,
        }

    def stop_outcomes(rows):
        dispatched = [
            trial["stop"]
            for trial in rows
            if "stop" in trial and "_suspend_dispatch_monotonic" in trial
        ]
        preparation_failures = [
            trial
            for trial in rows
            if "stop" in trial
            and "_suspend_dispatch_monotonic" not in trial
            and not trial["stop"].get("passed")
        ]
        counts = outcome_counts(
            len(dispatched),
            sum(bool(stop.get("passed")) for stop in dispatched),
            blocked,
        )
        # A failed baseline read is a real preparation failure, but no suspend
        # request was sent. Keep it separate from provider stop failures.
        remaining_key = "blocked" if blocked else "unrun"
        counts[remaining_key] = max(
            0, counts[remaining_key] - len(preparation_failures)
        )
        return {
            **counts,
            "preparation_failed": len(preparation_failures),
            "preparation_errors": [
                {"index": trial["index"], "error": trial["stop"].get("error")}
                for trial in preparation_failures
            ],
            "duration": summarize_values(
                stop.get("duration_s") for stop in dispatched if stop.get("passed")
            ),
        }

    # 2. Summarize outcomes and timings, keeping missing observations out of averages.
    blocked = RUN_STATE.get("status") == "blocked"
    summary = {
        "provider": "Tensorlake",
        "dataset": f"N={requested_n}",
        "run_id": RUN_STATE["run_id"],
        "created_at": RUN_STATE["created_at"],
        "reported_at": datetime.now(timezone.utc).isoformat(),
        "status": RUN_STATE["status"],
        "error": RUN_STATE.get("error"),
        "cases": {},
        "steps": {},
        "metrics": [],
        "resources": {},
        "network": {},
        "s3": {},
        "preparation": {},
        "config": RUN_STATE["config"],
        "shape": RUN_STATE["shape"],
        "cleanup": RUN_STATE.get("cleanup", []),
        "s3_cleanup": RUN_STATE.get("s3_cleanup"),
        "capacity_checks": RUN_STATE.get("capacity_checks", []),
        "snapshots_deleted": {
            k: RUN_STATE.get(k + "_deleted", False)
            for k in ["disk_snapshot_id", "memory_snapshot_id"]
        },
    }
    trial_rows = []
    activity_checks = []
    resource_samples = []
    network_requests = []
    s3_operations = []
    parse_errors = []
    for case in CASES:
        rows = CASE_ROWS[case]
        case_counts = outcome_counts(
            len(rows), sum(bool(trial.get("setup_passed")) for trial in rows), blocked
        )
        activity_attempts = sum("activity_passed" in trial for trial in rows)
        case_counts.update(
            terminal_passed=sum("terminal_ready_s" in trial for trial in rows),
            preservation_passed=sum(
                bool(trial.get("preservation", {}).get("passed")) for trial in rows
            ),
            activity=RUN_STATE.get("activity", {}).get(case),
            activity_passed=sum(bool(trial.get("activity_passed")) for trial in rows),
            activity_counts=outcome_counts(
                activity_attempts,
                sum(bool(trial.get("activity_passed")) for trial in rows),
            ),
            external_verifications_after_window=sum(
                trial.get("activity_external_late", 0) for trial in rows
            ),
        )
        case_counts["activity_first_attempt_clean"] = sum(
            bool(trial.get("activity_passed"))
            and all(
                e.get("external", {}).get(
                    "first_attempt_passed", e.get("external", {}).get("passed", False)
                )
                for e in trial.get("activity", [])
            )
            for trial in rows
        )
        case_counts["activity_external_recoveries"] = sum(
            bool(e.get("external", {}).get("recovered"))
            for trial in rows
            for e in trial.get("activity", [])
        )
        summary["cases"][case] = case_counts
        # Workload timing includes completed workflows. Terminal and queue
        # measurements can still exist when a VM fails a later workload step.
        for key in [
            "terminal_ready_s",
            "local_write_ready_s",
            "s3_read_ready_s",
            "app_ready_s",
            "browser_ready_s",
            "s3_write_ready_s",
            "full_workflow_s",
            "preservation_s",
            "preserved_s3_ready_s",
            "full_with_activity_s",
            "launch_queue_s",
        ]:
            values = [
                trial.get(key)
                for trial in rows
                if trial.get("setup_passed")
                or key in ["terminal_ready_s", "launch_queue_s"]
            ]
            metric = {
                "scenario": case,
                "metric": key,
                "unit": "seconds",
                **summarize_values(values),
            }
            summary["metrics"].append(metric)
        steps = [
            "local_write",
            "mount",
            "s3_read",
            "edit",
            "app",
            "browser",
            "s3_write",
        ] + (
            ["installed_deps_reused", "app_stop_after_preservation"]
            if case.startswith("resume")
            else ["clone", "install"]
        )
        for step in steps:
            passed = [
                e for trial in rows for e in trial.get("steps", []) if e["step"] == step
            ]
            failures = sum(
                trial.get("guest_failure", {}).get("stage") == step for trial in rows
            )
            summary["steps"][case + "/" + step] = {
                **outcome_counts(len(passed) + failures, len(passed), blocked),
                "duration": summarize_values(e["duration_s"] for e in passed),
            }
            summary["metrics"].append(
                {
                    "scenario": case,
                    "metric": step + "_operation",
                    "unit": "seconds",
                    **summarize_values(e["duration_s"] for e in passed),
                }
            )
        for trial in rows:
            trial_rows.append(
                {
                    "case": case,
                    "index": trial["index"],
                    "sandbox_id": trial.get("sandbox_id"),
                    "dispatch_at": trial.get("dispatch_at"),
                    "terminal_ready_s": trial.get("terminal_ready_s"),
                    "full_workflow_s": trial.get("full_workflow_s"),
                    "setup_passed": trial.get("setup_passed", False),
                    "preservation_passed": trial.get("preservation", {}).get("passed"),
                    "activity_passed": trial.get("activity_passed", False),
                    "error": trial.get("error"),
                }
            )
            for e in trial.get("steps", []):
                if e["step"] in ["s3_read", "s3_write"]:
                    s3_operations.append(
                        {
                            "case": case,
                            "index": trial["index"],
                            "operation": e["step"],
                            "phase": "primary_workload",
                            "passed": True,
                            "duration_s": e["duration_s"],
                            **(e["result"] if isinstance(e["result"], dict) else {}),
                        }
                    )
            for field, op, phase in [
                ("external_s3_write", "external_readback", "primary_workload"),
                ("preserved_mount_write", "s3_write", "preservation"),
                ("preserved_mount_verification", "external_readback", "preservation"),
            ]:
                if trial.get(field):
                    s3_operations.append(
                        {
                            "case": case,
                            "index": trial["index"],
                            "operation": op,
                            "phase": phase,
                            "passed": True,
                            **trial[field],
                        }
                    )
            window = trial.get("activity_window", {})
            # Keep every expected check, including absent observations. A partial
            # set of events is not a clean activity window.
            for i in range(12):
                e = next(
                    (
                        record
                        for record in trial.get("activity", [])
                        if record["index"] == i
                    ),
                    None,
                )
                inside = bool(
                    e
                    and window.get("start", 0)
                    <= timestamp_seconds(e["at"])
                    < window.get("end", 0)
                )
                ok = bool(
                    e
                    and e.get("passed")
                    and e.get("external", {}).get("passed")
                    and inside
                )
                activity_checks.append(
                    {
                        "case": case,
                        "index": trial["index"],
                        "tick": i,
                        "status": "unrun"
                        if e is None
                        else "passed"
                        if ok
                        else "failed",
                        "within_window": inside,
                        "record": e,
                    }
                )
                if e:
                    for field, op in [
                        ("s3_read", "s3_read"),
                        ("s3_write", "s3_write"),
                        ("external", "external_readback"),
                    ]:
                        if e.get(field):
                            s3_operations.append(
                                {
                                    "case": case,
                                    "index": trial["index"],
                                    "operation": op,
                                    "phase": "activity",
                                    "passed": True,
                                    **e[field],
                                }
                            )
            archive_path = (
                RUN_DIR / "trials" / case / str(trial["index"]) / "evidence.zip"
            )
            if case.startswith("fresh") and not archive_path.exists():
                archive_path = (
                    RUN_DIR
                    / "trials"
                    / ("resume_corrected" if case == "fresh_corrected" else "resume")
                    / str(trial["index"])
                    / "evidence.zip"
                )
            if not archive_path.exists():
                continue
            begin = timestamp_seconds(trial["dispatch_at"])
            end = float("inf")
            if case.startswith("fresh"):
                resume = next(
                    (
                        record
                        for record in CASE_ROWS[
                            "resume_corrected"
                            if case == "fresh_corrected"
                            else "resume"
                        ]
                        if record["index"] == trial["index"]
                    ),
                    None,
                )
                if resume:
                    end = timestamp_seconds(resume["dispatch_at"])
            with zipfile.ZipFile(archive_path) as archive:
                for name in archive.namelist():
                    target = (
                        resource_samples
                        if name.endswith("resources.jsonl")
                        else network_requests
                        if name.endswith("network.jsonl")
                        else None
                    )
                    if target is None:
                        continue
                    file_lines = archive.read(name).decode().splitlines()
                    for line_number, line in enumerate(file_lines, 1):
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError as e:
                            parse_errors.append(
                                {
                                    "archive": str(archive_path),
                                    "entry": name,
                                    "line": line_number,
                                    "last_line": line_number == len(file_lines),
                                    "error": str(e),
                                }
                            )
                            continue
                        if begin <= timestamp_seconds(record["at"]) < end:
                            target.append(
                                {
                                    "case": case,
                                    "index": trial["index"],
                                    "sandbox_id": trial.get("sandbox_id"),
                                    **record,
                                }
                            )

    # 3. Keep stop timings and preparation checks separate from startup timings.
    summary["cases"]["stop"] = {
        **stop_outcomes(CASE_ROWS["fresh"]),
        "mechanism": "same-ID suspend, confirmed SUSPENDED",
    }
    summary["metrics"].append(
        {
            "scenario": "stop",
            "metric": "duration",
            "unit": "seconds",
            **summary["cases"]["stop"]["duration"],
        }
    )
    summary["cases"]["stop_corrected"] = {
        **stop_outcomes(CASE_ROWS["fresh_corrected"]),
        "classification": "separate corrected diagnostic cohort",
    }
    summary["metrics"].append(
        {
            "scenario": "stop_corrected",
            "metric": "duration",
            "unit": "seconds",
            **summary["cases"]["stop_corrected"]["duration"],
        }
    )
    summary["corrected_cohorts"] = (
        "Cases ending _corrected are separately identified diagnostic cohorts after controller repairs; they do not replace original first attempts."
    )
    summary["case_configs"] = RUN_STATE.get("case_configs", {})
    # Choose validated repeats for comparisons but preserve each original
    # case separately. Never combine different runs into one sample.
    summary["preferred_cases"] = {
        "fresh": "fresh_corrected",
        "stop": "stop_corrected",
        "resume": "resume_corrected",
        "disk": "disk_corrected",
        "memory": "memory",
    }
    if (RESULTS_ROOT / "image-cleanup.json").exists():
        summary["image_cleanup"] = json.loads(
            (RESULTS_ROOT / "image-cleanup.json").read_text()
        )
    for case, rows in RUN_STATE.get("cases", {}).items():
        if case not in CASES:
            summary["preparation"][case] = {
                "attempted": len(rows),
                "setup_passed": sum(bool(trial.get("setup_passed")) for trial in rows),
                "activity_passed": sum(
                    bool(trial.get("activity_passed")) for trial in rows
                ),
                "preservation_passed": sum(
                    bool(trial.get("preservation", {}).get("passed")) for trial in rows
                ),
                "errors": [trial.get("error") for trial in rows if trial.get("error")],
            }

    # 4. Compute actual guest use from raw counters, not allocated capacity.
    # Each aggregate retains its observed VM sample count.
    samples_by_trial = defaultdict(list)
    cohort_buckets = defaultdict(dict)
    per_trial_resources = []
    for record in resource_samples:
        samples_by_trial[(record["case"], record["index"])].append(record)
    for (case, index), samples in samples_by_trial.items():
        samples.sort(key=lambda record: timestamp_seconds(record["at"]))
        previous = None
        trial_samples = []
        gaps = []
        for record in samples:
            memory_values = {
                k: int(v.split()[0]) * 1024
                for k, v in (
                    line.split(":", 1)
                    for line in record["raw"].get("/proc/meminfo", "").splitlines()
                )
            }
            counters = [
                int(v)
                for v in record["raw"].get("/proc/stat", "").splitlines()[0].split()[1:]
            ]
            at = timestamp_seconds(record["at"])
            cpu = None
            # Only use increasing counters across short gaps. Resets or missing
            # intervals must not create fabricated CPU usage.
            if previous:
                gap = at - previous[0]
                gaps.append(gap)
                delta = [a - b for a, b in zip(counters, previous[1])]
                total = sum(delta[:8])
                idle = sum(delta[3:5])
                if 0 < gap < 10 and total > 0 and all(v >= 0 for v in delta):
                    cpu = (total - idle) / total * record["cpu_count"]
            previous = (at, counters)
            sample = {
                "epoch": at,
                "cpu_cores": cpu,
                "memory_bytes": memory_values.get("MemTotal", 0)
                - memory_values.get("MemAvailable", 0),
                "disk_free_bytes": record["disk"]["free"],
            }
            trial_samples.append(sample)
            cohort_buckets[(case, math.floor(at / 2) * 2)][index] = sample
        per_trial_resources.append(
            {
                "case": case,
                "index": index,
                "samples": len(samples),
                "max_gap_s": max(gaps, default=None),
                "gaps_over_3s": sum(case_counts > 3 for case_counts in gaps),
                "cpu_peak_cores": max(
                    (
                        record["cpu_cores"]
                        for record in trial_samples
                        if record["cpu_cores"] is not None
                    ),
                    default=None,
                ),
                "memory_peak_bytes": max(
                    (record["memory_bytes"] for record in trial_samples), default=None
                ),
            }
        )
    # Sum observed samples only. Do not interpolate or carry values
    # forward through suspension or missing observations.
    cohort = [
        {
            "case": case,
            "epoch": at,
            "sampled_vms": len(v),
            "cpu_samples": sum(
                record["cpu_cores"] is not None for record in v.values()
            ),
            "cpu_cores": sum(record["cpu_cores"] or 0 for record in v.values()),
            "memory_bytes": sum(record["memory_bytes"] for record in v.values()),
        }
        for (case, at), v in sorted(cohort_buckets.items())
    ]
    summary["resources"] = {
        "raw_samples": len(resource_samples),
        "case_vm_series": len(samples_by_trial),
        "unique_vms": len({record["sandbox_id"] for record in resource_samples}),
        "per_vm": per_trial_resources,
        "method": "Observed two-second UTC buckets, no interpolation or carry-forward. CPU = nonidle /proc/stat delta times guest CPU count. RAM = MemTotal minus MemAvailable. Guest metrics, not provider billing metrics. Counters reset or gaps >=10 seconds have no CPU delta.",
        "peaks": {
            case: {
                "cpu": max(
                    (record for record in cohort if record["case"] == case),
                    key=lambda record: record["cpu_cores"],
                    default=None,
                ),
                "memory": max(
                    (record for record in cohort if record["case"] == case),
                    key=lambda record: record["memory_bytes"],
                    default=None,
                ),
            }
            for case in CASES
        },
    }
    summary["parse_errors"] = parse_errors
    # 5. Summarize observed network/S3 operations and the controller's own load.
    for target in sorted({record["target"] for record in network_requests}):
        for case in CASES:
            group = [
                record
                for record in network_requests
                if record["target"] == target and record["case"] == case
            ]
            if group:
                summary["network"][case + "/" + target] = {
                    "requests": len(group),
                    "errors": sum(
                        bool(record.get("error"))
                        or ("passed" in record and not record["passed"])
                        or (record.get("status") or 0) >= 400
                        for record in group
                    ),
                    "duration": summarize_values(
                        record.get("duration_s") for record in group
                    ),
                }
    for case in CASES:
        for phase in ["preservation", "primary_workload", "activity"]:
            for op in ["s3_read", "s3_write", "external_readback"]:
                group = [
                    record
                    for record in s3_operations
                    if record["case"] == case
                    and record["operation"] == op
                    and record["phase"] == phase
                ]
                if group:
                    summary["s3"][case + "/" + phase + "/" + op] = {
                        "operations": len(group),
                        "failed": sum(not record["passed"] for record in group),
                        "first_attempt_failed": sum(
                            not record.get("first_attempt_passed", record["passed"])
                            for record in group
                        ),
                        "recovered": sum(
                            bool(record.get("recovered")) for record in group
                        ),
                        "duration": summarize_values(
                            record.get("duration_s") for record in group
                        ),
                    }
    calls = list(read_json_lines(RUN_DIR / "controller-calls.jsonl"))
    control = list(read_json_lines(RUN_DIR / "controller-resources.jsonl"))
    polls = list(read_json_lines(RUN_DIR / "polling.jsonl"))
    summary["controller"] = {
        "samples": len(control),
        "maximum_cpu_seconds_in_one_process": max(
            (record["cpu_s"] for record in control), default=None
        ),
        "rss_peak_bytes": max(
            (record["rss_bytes"] for record in control), default=None
        ),
        "event_loop_delay_s": summarize_values(
            record["loop_delay_s"] for record in control
        ),
        "call_queue_s": summarize_values(record["queue_s"] for record in calls),
        "poll_interval_s": summarize_values(record["interval_s"] for record in polls),
        "calls": len(calls),
        "call_errors": sum(record.get("error") is not None for record in calls),
        "max_io_active": max((record["io_active"] for record in control), default=0),
        "max_s3_active": max((record["s3_active"] for record in control), default=0),
    }
    summary["controller"]["by_case"] = {
        case: {
            "calls": len(group),
            "errors": sum(record.get("error") is not None for record in group),
            "queue_s": summarize_values(record["queue_s"] for record in group),
        }
        for case in CASES
        for group in [[record for record in calls if record.get("case") == case]]
    }
    summary["failures"] = {
        case: {
            "setup": dict(
                Counter(trial["error"] for trial in rows if "error" in trial)
            ),
            "activity_observation": dict(
                Counter(
                    trial.get("activity_error")
                    or "Exception message empty in original controller"
                    for trial in rows
                    if "activity_error" in trial
                )
            ),
            "external_s3": dict(
                Counter(
                    e["external"].get("error", "Unknown verification failure")
                    for trial in rows
                    for e in trial.get("activity", [])
                    if e.get("external") and not e["external"].get("passed")
                )
            ),
            "guest_probe": dict(
                Counter(
                    e.get("error") or str(e.get("network_probe", {}).get("http_code"))
                    for trial in rows
                    for e in trial.get("activity", [])
                    if not e.get("passed")
                )
            ),
        }
        for case, rows in CASE_ROWS.items()
    }
    if (RUN_DIR / "activity-observation-audit-summary.json").exists():
        summary["observation_audit"] = json.loads(
            (RUN_DIR / "activity-observation-audit-summary.json").read_text()
        )
    if (RUN_DIR / "controller-transport-change.json").exists():
        summary["controller_transport_change"] = json.loads(
            (RUN_DIR / "controller-transport-change.json").read_text()
        )
    # 6. Export the same evidence as raw rows, comparison data and readable tables.
    for name, values in [
        ("resources", resource_samples),
        ("network", network_requests),
        ("s3-operations", s3_operations),
        ("activity-ledger", activity_checks),
        ("cohort-resources", cohort),
        ("metric-distributions", summary["metrics"]),
    ]:
        write_json_lines(name, values)
    with (RUN_DIR / "attempts.csv").open("w") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=list(trial_rows[0])
            if trial_rows
            else ["case", "index", "sandbox_id"],
        )
        writer.writeheader()
        writer.writerows(trial_rows)
    (RUN_DIR / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    compact = {
        k: v
        for k, v in summary.items()
        if k not in ["steps", "capacity_checks", "cleanup"]
    }
    compact["resources"] = {
        k: v for k, v in summary["resources"].items() if k != "per_vm"
    }
    compact["artifact_path"] = str(RUN_DIR)
    (RUN_DIR / "site-metrics.json").write_text(json.dumps(compact, indent=2) + "\n")
    report_lines = [
        f"# Tensorlake N={requested_n}",
        f"Run `{RUN_STATE['run_id']}`. Status: **{RUN_STATE['status']}**.",
        RUN_STATE.get("error") or "",
        "",
        "| Case | Requested | Attempted | Passed workload | Failed | Preparation failed | Blocked | Unrun |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for case, case_counts in summary["cases"].items():
        report_lines.append(
            "| "
            + case
            + " | "
            + " | ".join(
                str(
                    case_counts.get(k, 0)
                    if k == "preparation_failed"
                    else case_counts[k]
                )
                for k in [
                    "requested",
                    "attempted",
                    "passed",
                    "failed",
                    "preparation_failed",
                    "blocked",
                    "unrun",
                ]
            )
            + " |"
        )
    report_lines += [
        "",
        "| Activity case | First-attempt clean windows | Passed with bounded external recovery | Recovered external GETs |",
        "|---|---:|---:|---:|",
    ]
    for case in CASES:
        case_counts = summary["cases"][case]
        report_lines.append(
            f"| {case} | {case_counts['activity_first_attempt_clean']} | {case_counts['activity_passed']} | {case_counts['activity_external_recoveries']} |"
        )
    report_lines += [
        "",
        "Preparation resources and short proof windows are separate from measured attempts. Stop preparation failures occur before a suspend request is sent; they are not provider stop failures.",
        "",
        "## Measurements",
        "",
        "| Case / metric | N | Mean s | Median s | P95 s | Max s |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for metric in summary["metrics"]:
        report_lines.append(
            "| "
            + metric["scenario"]
            + " / "
            + metric["metric"]
            + " | "
            + str(metric["n"])
            + " | "
            + " | ".join(
                "—" if metric[k] is None else f"{metric[k]:.3f}"
                for k in ["mean", "median", "p95", "maximum"]
            )
            + " |"
        )
    report_lines += [
        "",
        "P95 uses nearest rank. Cumulative guest checkpoints use the terminal clock anchor and retain its round-trip uncertainty. Primary S3-ready and full-workflow checkpoints end after host-side S3 verification. S3 operation duration is measured directly; preserved-state writes are separate.",
        "",
        "Every sandbox clones the pinned approved local Git bundle; remote GitHub clone latency is not measured. Process restart policy is never. The app restarts deliberately only after untouched state verification for the second edit.",
        "",
        "Guest CPU/RAM/network are exported separately from controller metrics. Resource aggregates sum observed samples only and retain the sample count at each peak. No interpolation or carry-forward.",
        "",
        "Live capacity checks and cleanup receipts are in summary.json and state.json. The controller enforces 180-second readiness, per-stage deadlines, and an overall 6,000-second case deadline. No legacy $16 guard applies.",
    ]
    report_lines += [
        "",
        "## Preparation checks",
        "",
        "| Path | Attempts | Workload passed | Short activity passed | Preservation passed |",
        "|---|---:|---:|---:|---:|",
    ]
    for case, preparation_counts in summary["preparation"].items():
        report_lines.append(
            "| "
            + case
            + " | "
            + " | ".join(
                str(preparation_counts[k])
                for k in [
                    "attempted",
                    "setup_passed",
                    "activity_passed",
                    "preservation_passed",
                ]
            )
            + " |"
        )
    report_lines += [
        "",
        "Preparation proof screenshots, raw two-second guest samples, network records, and complete before/after state comparisons are retained in the per-trial evidence ZIPs.",
    ]
    report_lines += [
        "",
        "Corrected cohorts are separately labeled diagnostics after controller repairs. Original first attempts remain in this report and the raw files. AWS SlowDown and controlled-probe failures remain failures; later object verification does not turn them into primary passes.",
    ]
    (RUN_DIR / "report.md").write_text("\n".join(report_lines) + "\n")
    print(
        json.dumps(
            {
                "run_id": RUN_STATE["run_id"],
                "status": RUN_STATE["status"],
                "cases": summary["cases"],
                "preparation": summary["preparation"],
                "output": str(RUN_DIR),
            }
        )
    )


if __name__ == "__main__":
    generate_report()
