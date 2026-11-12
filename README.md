# Sandbox benchmarks

Pick a provider, supply your own credentials, and run the same scripted checks. The adapters cover **Tensorlake, Modal, Boat, Blaxel, Beam VMs and exe.dev**. Each run gets its own results and resource ledger.

This started as our Tensorlake benchmark. The [original controller and setup instructions](docs/tensorlake-reference.md) remain available for reproducing that protocol. The portable runner reuses its app, browser, S3 and saved-state workload. It does not rewrite historical results.

## Get started

Use Python 3.11–3.13. Install the provider you want:

```sh
git clone https://github.com/General-Intelligence-Company/tensorlake-sandbox-benchmarks.git
cd tensorlake-sandbox-benchmarks
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[tensorlake]'
sandbox-bench init --provider tensorlake
```

Set `TENSORLAKE_API_KEY` in your environment, then:

```sh
sandbox-bench doctor --config benchmark.toml
sandbox-bench run --config benchmark.toml
```

`doctor` is local only. It checks the configuration, installed SDK and presence of the required variables without showing their values. **`run` creates billable resources.** The generated config starts with five sandboxes and the command smoke suite. It checks terminal access and basic file operations, then deletes the sandboxes. It is not the historical 20-command throughput test or the full browser/S3 workload.

For another provider, change the extra and generated config:

```sh
pip install -e '.[blaxel]'
sandbox-bench init --provider blaxel --output blaxel.toml
# Set BL_API_KEY and BL_WORKSPACE in your environment.
sandbox-bench doctor --config blaxel.toml
sandbox-bench run --config blaxel.toml
```

Boat and exe.dev need no SDK extra. `sandbox-bench providers` lists credentials and the exact cases supported by each adapter. See [provider setup](docs/providers.md) for image requirements, permissions, preview access and cleanup limitations. No Doppler account or company-specific infrastructure is required.

## Run the full workload

Set `suite = "workflow"` in your config and fill in its `[s3]` section. Use an existing disposable test bucket, an existing seed object and a temporary AWS role profile. The controller verifies the AWS account before any test writes. That role needs to list the configured prefix, read the seed, and read/write/delete the run's objects. Provider control-plane keys never enter the guest.

The workload runs these steps in each sandbox:

1. Execute a terminal command and verify a local file write.
2. Mount the configured S3 prefix with s3fs and read the seed.
3. Clone the included Git bundle, change the app and install its dependencies.
4. Start the app, open Chromium and verify a real interaction.
5. Write through the S3 mount, then independently read and check the bytes from the controller.
6. Run a common 60-second activity window with 12 app, browser, network and S3 checks.

CPU/RAM samples, command outcomes, screenshots and guest logs are collected. No LLM or model API key is needed. A full workflow is only a pass after the outside S3 readback succeeds. A clean activity window is scored separately.

For example, Tensorlake supports all four cases:

```toml
provider = "tensorlake"
suite = "workflow"
count = 10
batch_size = 5
concurrency = 5
lifecycles = ["fresh", "disk", "memory", "resume"]

[s3]
profile = "my-benchmark-role"
expected_account = "123456789012"
bucket = "my-sandbox-test-bucket"
region = "us-east-1"
prefix = "sandbox-bench/tensorlake/"
seed = "readme.txt"
```

`resume` reuses eligible fresh sandboxes. Disk and memory tests prepare a source outside the measured cohort, then fork new IDs. Failed cases are not replaced. Unsupported cases fail configuration validation rather than silently using a different operation.

**`count` is the total per lifecycle, not the concurrent VM count.** `batch_size` limits resident measured sandboxes; `concurrency` limits simultaneous trial setup. A snapshot source adds one resident VM. The defaults are deliberately small. Keep the total below your account capacity and leave room for other users. This runner does not reserve organization-wide capacity against other clients.

Temporary AWS credentials must have at least 25 minutes left before a batch starts. They are not refreshed inside saved memory. If they expire or a quota blocks admission, the run stops, retains unfinished counts and cleans up what it can. There is no automatic continuation or replay of an interrupted lifecycle operation.

## Read the results

The command prints the run directory, for example `outputs/tensorlake-<run-id>/`.

```sh
sandbox-bench report outputs/tensorlake-<run-id>
sandbox-bench cleanup outputs/tensorlake-<run-id>
```

- `run.json`: configuration, every lifecycle intent/result, ownership and per-case outcomes.
- `summary.json`: attempted, passed, failed, ineligible and unrun counts; successful workflow mean/P95; activity and cleanup coverage.
- `events.jsonl`: command timing/outcomes and each independent S3 read attempt.
- `artifacts/`: guest logs, resource samples and browser screenshots.

Readiness milestones are cumulative from the create/fork/resume dispatch. Terminal ready means a command ran. Local-disk, S3 and full-workflow readiness are separate checkpoints. Do not add their times together. Queue waiting, activity and cleanup are separate.

**Cold tool installation is included in portable-runner workflow timing** and reported as `bootstrap_s`. Supplying an image with the tools already installed avoids that work. This differs from the original benchmark's prepared images and authenticated HTTP control transport: the portable runner uses native exec to reach a loopback guest service. Keep results from the two protocols separate.

The S3 verifier retries only connection failures and `SlowDown`, up to three total attempts within a 15-second budget. Every attempt stays in the evidence. Authentication errors and content mismatches are not retried. Provider lifecycle writes are submitted once by the controller; provider SDK internal retries, where applicable, are described in [provider setup](docs/providers.md).

A missed activity check is not proof of a crashed VM. The report separates guest results, outside S3 readbacks, cadence misses, collection failures and unknown causes. Timings summarize successful workflows only; always read their sample count beside failure counts.

## Cleanup and interrupted runs

Cleanup acts only on IDs created by this run, exact names from its create intents, and its generated S3 subprefix. It independently checks VM deletion. A second controller cannot lock the same run directory. There is no `resume` command that might repeat an ambiguous create or fork.

Keep the run directory after an interruption. `cleanup` can reconcile named resources and remove known IDs without starting a replacement cohort. Lost snapshot responses, incomplete provider inventories and SDK deletion limitations remain visible; the tool does not call those fully reconciled. Modal memory snapshots must expire under the provider's retention policy. Cached base images are not removed.

Outputs can contain app files and environment-specific metadata. Review them before sharing. Keys and signed URLs are redacted from JSON logs, and provider keys are never copied into the guest.

## Development

```sh
pip install -e '.[test,tensorlake,modal,blaxel,beam]'
pytest -q
python3 check_source_integrity.py
python3 controller/check_s3_read_retries.py
```

These checks do not start cloud resources. The adapter code was checked against the pinned SDK interfaces, with fake transports and offline lifecycle tests. The portable adapters have **not** been validated by new live provider cohorts. Run a small test in your own account before scaling.

The shared workload lives in `context/`; provider operations are in `sandbox_bench/providers.py`; orchestration and evidence handling are in `sandbox_bench/runner.py`. Add another adapter without copying the workload. The original controller, source manifest and historical artifacts remain separate.
