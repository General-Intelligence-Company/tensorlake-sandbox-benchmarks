# Tensorlake sandbox benchmarks

This repository contains the scripts we used to test Tensorlake sandboxes: how quickly they become usable, whether saved sessions survive a restart, and whether they keep working under load.

The large benchmark used **1,000 sandboxes per test case**, each with **4 vCPU, 8 GiB RAM and 10 GiB disk**. Scripts run the work inside each sandbox; this is not a test of 1,000 AI agents.

## What we test

| Test case | What happens |
| --- | --- |
| New sandbox | Start a new virtual machine from an image with the test tools already installed. |
| Disk snapshot fork | Save a sandbox's disk, then create a different sandbox from it. Check that saved files and installed dependencies are present. |
| Memory snapshot fork | Save disk and RAM, then create a different sandbox from them. Check that the running app, browser state and files survived. |
| Stop and resume | Stop a sandbox, then resume that same sandbox. Check its saved state before changing or restarting anything. |

At most **64 create-and-connect requests run at once**. As one finishes, the next queued request starts. This limits how quickly we launch sandboxes; it does not limit the test to 64 running sandboxes.

To change this limit, edit `LAUNCH_CONCURRENCY = 64` near the top of [controller/run_benchmark.py](controller/run_benchmark.py#L43). `SANDBOX_COUNT` controls the total number of sandboxes per test case; `LAUNCH_CONCURRENCY` controls how many create-and-connect requests can be in progress at once.

## What runs inside a sandbox

The workload checks that the sandbox can:

1. Run a terminal command and write a local file.
2. Mount an S3 folder and read a known file from it.
3. Clone the test app, change its code, and install its dependencies.
4. Start the app and interact with it in Chromium.
5. Write a file through the S3 mount. The controller, running outside the sandbox, then reads it directly from S3 and checks its contents.

The app is cloned from a local Git bundle, so these timings do not include downloading it from GitHub. On resume, we reuse the saved repository and dependencies and make a second code change after checking that the original state survived.

After setup, a separate 60-second activity test repeats app, network and S3 checks. CPU and RAM usage are recorded during the run.

## How to read the timings

These milestones all start from the **same point: immediately before we send that sandbox's create, fork or resume request**.

| Milestone | The timer ends when… |
| --- | --- |
| Terminal ready | A command successfully runs inside the sandbox. |
| Local disk writable | A local file has been written, flushed and read back. |
| S3 write verified | A file written through the mount has been read and checked from outside the sandbox. |
| Full workflow complete | All required app, browser and file checks have passed. |

For example, “terminal ready: 2 seconds” and “S3 write verified: 6 seconds” means the S3 check finished 6 seconds after the original request. It does **not** mean the S3 write itself took 6 seconds. Do not add the milestone times together.

S3 verification is the final workflow step, so its time matches full-workflow completion. Waiting for a launch slot is recorded separately. The later activity test and cleanup are not included in these startup times.

The create request waits on the server and returns as soon as the sandbox is running (Tensorlake SDK 0.5.158 and later). The historical results were produced with SDK 0.5.157, which polled the sandbox status once per second after sending the create, so their startup milestones include up to one second of polling delay that new runs do not. Stop and resume still poll once per second in the SDK, so their timings keep that delay.

- **Mean:** the average time.
- **Median:** the middle time when results are sorted.
- **P95:** 95% of measured results finished within this time.
- **Max:** the slowest measured result.

Stop timing measures how long it takes to confirm the sandbox is suspended. Resume timing starts with the later resume request.

Stop reports separate preparation failures from failures of a dispatched suspend request. For example, a failed saved-state read before suspension appears under **Preparation failed** and does not enter the stop-time measurements. Report denominators come from the saved run's `requested_n` value.

## How to read reliability results

These results count how many sandboxes passed each check. They are separate from the timing measurements above.

| Result | What passing means |
| --- | --- |
| Terminal ready | A command successfully ran inside the sandbox and the expected image files were verified. |
| Initial full workflow | The initial app, browser, local-file and mounted-S3 checks all passed, including reading the written object directly from S3 outside the sandbox. |
| Clean 60-second activity | All 12 scheduled rounds of app, browser, network and mounted-S3 checks during the following minute passed, including the independent S3 readbacks under that run's retry policy. |
| Saved state preserved | Before any new edits or app restart, the restored sandbox kept the saved state required by the test: files, the S3 mount and, for memory forks and resumes, the checked app processes and browser state. |

New sandboxes have no previous state to preserve. Disk forks check saved files and an installed dependency during startup; memory forks and resumes also run the separate process/browser preservation check. An unmeasured or inapplicable preservation result is not a failure.

A failed activity check does **not** necessarily mean the sandbox crashed. One failed or unconfirmed request is enough for a sandbox to miss the clean-minute result. Keep first-attempt success separate from success after a retry; the retry rules below explain which external S3 reads can recover.

## S3 failures and retries

There are two separate S3 paths: the sandbox's mounted-file operations and the controller's independent read used to verify a write. A failed controller read does not, by itself, prove that the sandbox or its mount failed.

Earlier runs allowed only one attempt for that independent read. The current verifier allows **up to three total attempts** for connection failures and S3 `SlowDown` responses. `SlowDown` is S3 asking the caller to wait and retry.

| Failure | Wait before the next attempt |
| --- | --- |
| Connection failure | 0.25 seconds, then 0.5 seconds. |
| S3 `SlowDown` | A random delay between 0–0.5 seconds, then 0–1 second. Random delays spread out simultaneous retries. |

Each connection and read has a 2-second timeout. Before retrying, the verifier checks that its 15-second retry budget has enough time left for another connection and read. DNS lookups can outlast those socket timeouts, so the surrounding test also has an overall deadline.

The verifier does not retry permission errors, other HTTP errors, or incorrect file contents. These retries never repeat a sandbox write or code change. Automatic SDK retries are disabled so the controller can record every attempt itself.

`s3-external.jsonl` records each attempt's error, status, request ID and retry delay. It also records whether the first attempt passed, a retry recovered, or retries ran out. Report those outcomes separately: a recovered read is not a first-attempt success. Changing the retry code does not change older results.

## Where to look in the code

| File | Responsibility |
| --- | --- |
| [controller/run_benchmark.py](controller/run_benchmark.py) | Start and stop sandboxes, measure readiness, verify S3 writes and clean up. |
| [context/sandbox_workload.py](context/sandbox_workload.py) | Own one sandbox's app, browser and mount, and run the measured steps. |
| [context/run_sandbox_workload.py](context/run_sandbox_workload.py) | Receive controller requests and run them on one worker thread. |
| [controller/summarize_results.py](controller/summarize_results.py) | Calculate statistics from saved results. |
| [context/record_resource_usage.py](context/record_resource_usage.py) | Record CPU and RAM usage inside the sandbox. |
| [controller/verify_saved_activity.py](controller/verify_saved_activity.py) | Inspect saved activity logs and, when requested, read S3 objects again. Later reads are separate evidence. |
| [image/](image/) | Build the tools image and copy in the benchmark scripts. |
| [fixture/prepare_network_probe.example.py](fixture/prepare_network_probe.example.py) | Prepare a private, temporary URL for the outbound network check. |
| [check_source_integrity.py](check_source_integrity.py) | Check the published source files against their recorded hashes. |
| [fixture-app/](fixture-app/) | The small app used in the test. Its original Git bundle is in `context/`. |

In `sandbox_workload.py`, start with `SandboxWorkload.run_workload()` for the ordered steps. `measure_step()` records each step's duration, and `run_due_activity_check()` runs the repeated 60-second checks. The app, browser and S3 mount belong to that one object. The small `run_sandbox_workload.py` entrypoint queues HTTP requests so only its main worker thread touches that state.

In `run_benchmark.py`, start with `Runner.run_case()` to follow a complete case. `create_trial()` creates one sandbox and checks readiness, `run_workload()` records its app/browser/S3 milestones, and `suspend_and_resume()` checks the same sandbox after stopping it. `verify_s3_write()` owns the independent S3 read and its bounded retries. Each trial keeps its own results; the runner owns shared connections, launch limits and evidence files.

`generate_report()` reads saved results and writes the comparison files. `audit_saved_activity()` is a separate audit that can reread S3 objects without changing primary results. The image builders, resource sampler and network-probe helper also have explicit entrypoints, so importing those files does not start their work.

The fixture app retains its original `exe.dev` text because the workload replaces that exact text with each trial's unique marker. The Git bundle and pinned dependency versions remain the original test inputs.

## Running it yourself

This is the source for our test setup, not a ready-to-run command. **Review the configuration before launching: `SANDBOX_COUNT` defaults to 1000.**

1. Install Python, `tensorlake==0.5.158`, `boto3`, `httpx` and the AWS CLI. Provide your own `TENSORLAKE_API_KEY`.
2. Set `ROOT`, `OUT`, `FIXTURE` and `IMAGE` in the controller for your environment. These identify the source directory, results directory, shared test files and prepared image. The controller expects image files under `ROOT/context`.
3. Copy [context/manifest.example.json](context/manifest.example.json) to `manifest.json` in both the shared-test directory and image context. Configure your disposable S3 bucket, AWS profile, expected account ID, and a known test file with its expected hash.
4. Adapt [fixture/prepare_network_probe.example.py](fixture/prepare_network_probe.example.py) as the shared-test directory's `prepare_network_probe.py`. It creates a temporary signed URL for the network check. Keep that URL and all credentials out of Git.
5. Set the paths in the image scripts, then run `image/build_tools_image.py` followed by `image/build_benchmark_image.py`. The built image's files must match the controller's copies: readiness checks compare their hashes. Rebuild the image after changing those files, even if you only changed comments.
6. Use `prepare` to create and check the source snapshots. The measured modes are `memory`, `fresh_corrected` and `disk_corrected`; `fresh_corrected` also tests stop/resume. Use `cleanup_vms` to remove the run's sandboxes, and `cleanup` to also remove its snapshots and S3 files. Remove temporary images separately when finished. Keep the saved state file: cleanup uses its resource IDs to avoid deleting unrelated resources.

The signed network-probe URL lasts at most one hour, and may expire sooner with the AWS session. The controller does not rotate credentials inside a running or saved sandbox. Keep a case within those lifetimes and refresh the fixture before another run; the 6,000-second case deadline does not extend credential validity. Failures after expiry need to be investigated as test-setup failures before attributing them to the provider.

Two local checks are available. Neither starts sandboxes or makes cloud requests:

```sh
python3 check_source_integrity.py
python3 controller/check_s3_read_retries.py
```

The first checks the published source files and fixture bundle against `source-manifest.json`, without needing Git history. The second checks the external-read retry behavior. Live provider tests are separate.

## Source release and historical results

This public repository starts with a fresh source history. Its fixture bundle also has a new single-commit history; the example manifest contains that fixture commit. Personal paths and internal ticket references have been removed. `source-manifest.json` records the published file hashes.

Historical benchmark results belong to the code and configuration that produced them. This public release is not a new benchmark run, and no historical measurement has been changed. Keep new runs separate from historical results.
