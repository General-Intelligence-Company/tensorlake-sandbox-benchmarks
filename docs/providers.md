# Provider setup

The portable runner uses the integrations below. The adapters have offline contract checks and [N=5 live validation results](live-validation-2026-11-12.md) for Boat, Tensorlake, Beam VMs and Modal. A passing check does not establish that your account has snapshot access, enough quota, or a supported image.

| Provider | Install extra | Credentials | Implemented lifecycle cases |
| --- | --- | --- | --- |
| Tensorlake | `tensorlake` | `TENSORLAKE_API_KEY` | Fresh, disk fork, memory fork, same-ID resume |
| Modal | `modal` | `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET` | Fresh, disk fork, memory fork |
| Boat | None | `BOAT_API_KEY`, `BOAT_ORG` | Fresh, disk fork, same-ID disk resume |
| Blaxel | `blaxel` | `BL_API_KEY`, `BL_WORKSPACE` | Fresh, memory fork |
| Beam VMs | `beam` | `BEAM_API_KEY` | Fresh, disk fork, same-ID memory-preserving resume |
| exe.dev | None | `EXE_API_KEY` | Fresh, disk copy, same-ID resume |

This table describes **what this runner implements**, not every feature sold by a provider. An unsupported requested case is rejected before any API calls. We do not silently replace a memory fork with a disk copy.

## Tensorlake

Uses the sandbox SDK with request retries disabled. `provider_options.image` can select an existing Linux image; the default is `tensorlake/ubuntu-minimal`. Memory snapshots may require your account to be enabled for them. [Documentation](https://docs.tensorlake.ai/)

## Modal

Uses VM runtime and the experimental memory snapshot API in the pinned SDK. Ask Modal for access if your account does not have it. Restoring a memory snapshot creates a new sandbox ID, so it is called `memory`, not same-ID `resume`, here. Capturing memory stops the source.

Memory snapshots have no delete method in this SDK. The report keeps their IDs, reports cleanup as incomplete, and leaves them to expire after seven days. Disk images use the pinned SDK's internal image deletion RPC followed by an independent image lookup, both on the SDK event loop. The runner only confirms deletion after that lookup returns not found. Modal's SDK has its own internal transport retry behavior; the controller submits each lifecycle operation once. [Snapshots](https://modal.com/docs/guide/sandbox-snapshots)

## Boat

Set `BOAT_ORG` to the organization ID you intend to bill. Requests include `X-Boat-Org`; inherited guest environment injection is disabled with `noEnv`.

The default machine type is `default`. It is a provider-defined shape, so confirm its allocation before comparing it to a literal CPU count. A disk fork uses the stopped source's saved disk. Resuming that same ID is a disk boot: the test verifies saved code and dependencies, then starts a new app and browser. It does not claim that RAM survived.

Start and API limits can stop a run. The runner records the first response and leaves unrun cases visible; it does not buy credits, change plans, or replay a rejected start to replace a case. [Documentation](https://docs.boat.dev/)

## Blaxel

Uses the authenticated SDK with both API key and workspace. Do not replace its data-plane client with an HTTP client that loses workspace headers. Snapshots are a preview feature. They preserve memory and filesystem together; this adapter does not claim a disk-only snapshot.

The default is `blaxel/py-app:latest`, whose Debian/Python base supports the shared workload installer. Creation uses the SDK's default deadline. Persistent test services use native processes with `keep_alive` enabled and the configured TTL, so automatic standby does not interrupt an active test window.

Prepared mode builds a private tools image before measured creation; it does not use a memory fork as a substitute for fresh creation. The build sandbox and its uniquely named image are recorded for cleanup. Workload commands wait through the native API. Authorization failures stop readiness observation. Workload commands and snapshot/fork mutations are not retried by the runner. [Documentation](https://docs.blaxel.ai/)

## Beam

This adapter uses **VMs**, not serverless Sandboxes. It uses the pinned SDK's low-level VM and process APIs from our evaluation. Supplying a separate request ID to each process RPC avoids nested SDK retries and avoids deduplicating different calls.

With no `provider_options.image`, the runner first prepares the default VM image using Beam's image builder. This may take time and incur provider charges. It is recorded separately from VM launch timing. The image remains in the provider's image cache; it is not a per-run VM or snapshot. To reuse a prepared VM image, put its ID in `provider_options.image`. Ordinary container image IDs without the Beam VM services will not work. [VM documentation](https://docs.beam.cloud/vms/overview)

Prepared workflow mode additionally captures a run-owned tools-only disk base. Command execution requests native `wait=True`. The service can return a running PID for a long command; the runner observes that existing PID at 100 ms until completion, records each observation, and never resubmits the command. Guest execution timing excludes this response-observation delay.

## exe.dev

The HTTPS API key must allow `new`, `ls`, `ssh`, `cp`, `pause`, `resume`, and `rm`. Default tokens do not include every command needed for cleanup. Use an existing appropriately scoped key; the runner never changes its grants.

The API transports the same commands as the SSH interface. This adapter uses HTTPS, which may have different rate limits and overhead from the native SSH runs in our original evaluation. Copy is a disk test. The resume check tests the live app/browser state before changing it. [HTTPS API](https://exe.dev/docs/https-api), [command transport](https://exe.dev/docs/https-api-run-on-vm)

Command responses request NDJSON, accept blank keepalive lines, and require an exit-code record. Before a disk snapshot, the adapter runs `sync` in the source VM: pausing alone can leave recent writes in RAM, while the copy reads disk blocks.

In prepared mode, the fresh baseline is also created with `cp` from a stopped tools-only VM. This is a prepared disk-base launch, not a measurement of registry-image `new`. The workload disk phase has a different source with saved app code and dependencies. The report retains this distinction.

## Images and resource shapes

The full workflow needs Linux with `bash`, Python, `apt-get`, root or passwordless `sudo`, and working FUSE mounts. It installs Git, Node/npm, s3fs, Playwright and Chromium when absent. A custom image must support those tools. The runner enables the optional `compat_dir` mount flag only when the installed s3fs advertises it. A FUSE permission error stays in the result; it is not converted into a successful S3 test.

CPU units differ by provider. The report stores the requested configuration; the artifact archive contains `/proc` and cgroup resource samples. Boat's named machine type and Blaxel's memory-based allocation do not map directly to `cpus`. This runner does not claim that all providers supply identical physical cores.

## Adding another provider

Add an entry in `SPECS` and a `Provider` implementation in `sandbox_bench/providers.py`. Implement create, exec, inventory, existence and delete first; then add only the snapshot and resume operations you can identify precisely. Return stable provider IDs, never guest process IDs.

Keep lifecycle writes single-dispatch. Persist the returned ID before polling. If a response is lost, reconcile the run-specific name for cleanup; do not create a replacement. Test failures, request shapes and cleanup with fake transports before running a small live cohort on your own account.
