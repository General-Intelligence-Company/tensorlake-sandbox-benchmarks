"""Recover observations from saved guest logs without rerunning any workload or changing primary results."""

import concurrent.futures
import hashlib
import json
import os
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config


def audit_saved_activity():
    # This is a separate audit: it can reread S3, but never changes primary results.
    RESULTS_ROOT = Path(__file__).resolve().parents[1] / "outputs/benchmark"
    RUN_DIR = (
        RESULTS_ROOT / json.loads((RESULTS_ROOT / "current.json").read_text())["run_id"]
    )
    RUN_STATE = json.loads((RUN_DIR / "state.json").read_text())
    FIXTURE_CONFIG = json.loads(
        (Path(__file__).resolve().parents[1] / "context/manifest.json").read_text()
    )
    credential_env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    aws_credentials = json.loads(
        subprocess.check_output(
            [
                "aws",
                "configure",
                "export-credentials",
                "--profile",
                FIXTURE_CONFIG["aws_profile_for_shared_fixture"],
                "--format",
                "process",
            ],
            env=credential_env,
        )
    )
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials["AccessKeyId"],
        aws_secret_access_key=aws_credentials["SecretAccessKey"],
        aws_session_token=aws_credentials["SessionToken"],
        region_name=FIXTURE_CONFIG["region"],
        config=Config(max_pool_connections=16, retries={"total_max_attempts": 1}),
    )

    def timestamp_seconds(value):
        return datetime.fromisoformat(value).timestamp()

    # Recover guest events independently of controller observations. Later S3
    # reads can confirm bytes but do not change original activity pass counts.
    def recover_trial_observations(item):
        case, trial = item
        archive_path = RUN_DIR / "trials" / case / str(trial["index"]) / "evidence.zip"
        if case.startswith("fresh") and not archive_path.exists():
            archive_path = (
                RUN_DIR
                / "trials"
                / ("resume_corrected" if case == "fresh_corrected" else "resume")
                / str(trial["index"])
                / "evidence.zip"
            )
        audit = {
            "case": case,
            "index": trial["index"],
            "primary_passed": trial.get("activity_passed", False),
            "archive_present": archive_path.exists(),
            "events": [],
        }
        if not archive_path.exists():
            return audit
        with zipfile.ZipFile(archive_path) as archive:
            name = "guest/" + case + "/events.jsonl"
            if name not in archive.namelist():
                return audit
            events = [
                json.loads(line)["record"]
                for line in archive.read(name).decode().splitlines()
                if json.loads(line).get("type") == "activity"
            ]
        window = trial.get("activity_window") or RUN_STATE.get("activity", {}).get(
            case, {}
        )
        begin = window.get("start", window.get("start_epoch", 0))
        end = window.get("end", window.get("end_epoch", begin + 60))
        for i in range(12):
            event = next(
                (
                    e
                    for e in events
                    if e["index"] == i and begin <= timestamp_seconds(e["at"]) < end
                ),
                None,
            )
            entry = {
                "tick": i,
                "guest_event": event,
                "guest_passed_in_window": bool(event and event.get("passed")),
            }
            primary = next(
                (x for x in trial.get("activity", []) if x["index"] == i), None
            )
            entry["primary_external"] = primary.get("external") if primary else None
            write = event.get("s3_write") if event else None
            # Reread only previously unconfirmed objects belonging to this run.
            # The new check is explicitly recorded as later verification.
            if write and not (primary or {}).get("external", {}).get("passed"):
                assert write["key"].startswith(
                    FIXTURE_CONFIG["s3_prefixes"]["tensorlake"]
                    + "n100-"
                    + RUN_STATE["run_id"]
                    + "/"
                )
                try:
                    body = s3.get_object(
                        Bucket=FIXTURE_CONFIG["bucket"], Key=write["key"]
                    )["Body"].read()
                    entry["later_external"] = {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "passed": len(body) == write["bytes"]
                        and hashlib.sha256(body).hexdigest() == write["sha256"],
                        "bytes": len(body),
                    }
                except Exception as e:
                    entry["later_external"] = {"passed": False, "error": str(e)}
            entry["bytes_confirmed"] = bool(
                (entry.get("primary_external") or {}).get("passed")
                or entry.get("later_external", {}).get("passed")
            )
            audit["events"].append(entry)
        audit["guest_window_passed"] = len(audit["events"]) == 12 and all(
            x["guest_passed_in_window"] for x in audit["events"]
        )
        audit["later_complete_evidence"] = audit["guest_window_passed"] and all(
            x["bytes_confirmed"] for x in audit["events"]
        )
        return audit

    CASES = [
        "fresh",
        "resume",
        "disk",
        "memory",
        "fresh_corrected",
        "resume_corrected",
        "disk_corrected",
    ]
    trial_items = [
        (case, trial)
        for case in CASES
        for trial in RUN_STATE.get("cases", {}).get(case, [])
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(recover_trial_observations, trial_items))
    with (RUN_DIR / "activity-observation-audit.jsonl").open("w") as output_file:
        for row in results:
            output_file.write(json.dumps(row) + "\n")
    summary = {}
    for case in CASES:
        audits = [trial for trial in results if trial["case"] == case]
        summary[case] = {
            "rows": len(audits),
            "primary_passed": sum(trial["primary_passed"] for trial in audits),
            "guest_window_passed": sum(
                trial.get("guest_window_passed", False) for trial in audits
            ),
            "later_complete_evidence": sum(
                trial.get("later_complete_evidence", False) for trial in audits
            ),
            "archive_missing": sum(not trial["archive_present"] for trial in audits),
            "later_external_reads": sum(
                "later_external" in e for trial in audits for e in trial["events"]
            ),
        }
    (RUN_DIR / "activity-observation-audit-summary.json").write_text(
        json.dumps(
            {
                "method": "Read saved guest logs and GET only previously unconfirmed S3 objects. No workload reruns; primary results remain unchanged. Later verification occurred after the activity window.",
                "cases": summary,
            },
            indent=2,
        )
        + "\n"
    )
    print(json.dumps(summary))


if __name__ == "__main__":
    audit_saved_activity()
