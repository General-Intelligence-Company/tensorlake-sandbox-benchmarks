# Prepared-image timing validation: 13 November 2026

We ran N=5 per supported lifecycle through the public CLI, with providers sequential and at most five cases in flight. Same-ID resumes ran sequentially. The selected phases contain 90/90 completed workflows and 80/90 clean 60-second activity windows. No N=100 or N=1,000 run was launched. Snapshot-source and tools-image preparation are outside the measured case counts. All 90 actual measured attempts are included; no measured case was replaced.

## Startup and workload execution

Values are means in seconds. The CSV contains each selected case's raw timing values. Terminal readiness uses the same successful-command criterion across providers. Guest work is timed on the guest thread. End-to-end includes setup, saved-state checks, native transport and independent S3 verification.

| Provider | Lifecycle | Workflows | Clean activity | Terminal ready | Guest work | End-to-end |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Tensorlake | fresh | 5/5 | 5/5 | 0.997 | 4.818 | 6.353 |
| Tensorlake | disk | 5/5 | 5/5 | 0.976 | 4.090 | 5.639 |
| Tensorlake | memory | 5/5 | 5/5 | 0.786 | 2.486 | 4.049 |
| Tensorlake | resume | 5/5 | 5/5 | 0.372 | 1.937 | 2.711 |
| Modal | fresh | 5/5 | 4/5 | 0.830 | 5.910 | 7.499 |
| Modal | disk | 5/5 | 4/5 | 1.071 | 6.327 | 8.366 |
| Modal | memory | 5/5 | 4/5 | 2.411 | 4.064 | 27.503 |
| Beam VMs | fresh | 5/5 | 5/5 | 9.737 | 5.364 | 17.262 |
| Beam VMs | disk | 5/5 | 5/5 | 5.107 | 4.869 | 12.324 |
| Beam VMs | resume | 5/5 | 5/5 | 1.249 | 3.320 | 26.651 |
| Blaxel | fresh | 5/5 | 5/5 | 6.539 | 4.758 | 12.479 |
| Blaxel | memory | 5/5 | 5/5 | 1.084 | 3.117 | 5.024 |
| exe.dev | fresh | 5/5 | 5/5 | 2.811 | 6.000 | 13.471 |
| exe.dev | disk | 5/5 | 5/5 | 2.573 | 5.293 | 12.435 |
| exe.dev | resume | 5/5 | 3/5 | 1.868 | 2.517 | 6.650 |
| Boat | fresh | 5/5 | 2/5 | 2.825 | 13.675 | 17.626 |
| Boat | disk | 5/5 | 5/5 | 3.054 | 12.582 | 18.903 |
| Boat | resume | 5/5 | 3/5 | 0.643 | 8.577 | 10.148 |

## Setup, observation and verification

These are measured components, not estimates subtracted from the end-to-end clock. API calls return at provider-specific states, so their durations alone are not a comparable boot benchmark. Setup includes saved-state checks; those checks can encounter genuine browser, filesystem or S3 delays and must not be dismissed as artificial overhead.

| Provider | Lifecycle | Native lifecycle API | Setup + state checks | Workload observation | Outside S3 check |
| --- | --- | ---: | ---: | ---: | ---: |
| Tensorlake | fresh | 0.797 | 0.327 | 0.140 | 0.071 |
| Tensorlake | disk | 0.787 | 0.360 | 0.156 | 0.057 |
| Tensorlake | memory | 0.467 | 0.532 | 0.138 | 0.107 |
| Tensorlake | resume | 0.260 | 0.226 | 0.135 | 0.042 |
| Modal | fresh | 0.124 | 0.589 | 0.126 | 0.045 |
| Modal | disk | 0.193 | 0.738 | 0.183 | 0.046 |
| Modal | memory | 0.174 | 20.811 | 0.161 | 0.056 |
| Beam VMs | fresh | 0.181 | 1.203 | 0.907 | 0.052 |
| Beam VMs | disk | 0.248 | 1.523 | 0.771 | 0.053 |
| Beam VMs | resume | 0.334 | 21.469 | 0.525 | 0.088 |
| Blaxel | fresh | 6.344 | 0.912 | 0.194 | 0.075 |
| Blaxel | memory | 0.834 | 0.606 | 0.154 | 0.063 |
| exe.dev | fresh | 1.199 | 3.625 | 0.919 | 0.116 |
| exe.dev | disk | 1.018 | 3.578 | 0.916 | 0.074 |
| exe.dev | resume | 1.004 | 1.263 | 0.916 | 0.086 |
| Boat | fresh | 0.583 | 0.802 | 0.275 | 0.049 |
| Boat | disk | 0.610 | 2.619 | 0.587 | 0.062 |
| Boat | resume | 0.470 | 0.654 | 0.184 | 0.090 |

No selected prepared case ran cold tools bootstrap. The generated base contains the benchmark files, browser and runtime tools before dispatch. Ordinary application work, including the fixture's dependency installation and S3 operations, remains in guest execution time.

## Prepared base and native wait behavior

| Provider | Prepared baseline | Command completion |
| --- | --- | --- |
| Tensorlake | Tools-only disk snapshot | Native blocking execution; create normally waits server-side; resume observes at 100 ms |
| Modal | Tools-only filesystem image | Native stream reads and process wait; app lookup occurs before timed dispatch |
| Beam VMs | Tools-only VM disk snapshot | Native `wait=True` first; running PIDs are observed at 100 ms without replay |
| Blaxel | Native private image build | Native `wait_for_completion=True` |
| exe.dev | Disk copy of a stopped tools-only VM | Blocking NDJSON command response |
| Boat | Named tools-only disk snapshot | Blocking command endpoint |

The prepared exe.dev fresh baseline uses `cp`, not registry-image `new`. Workload disk forks use a different source containing app state. Boat's resume is a disk boot, not a RAM restore. Requested CPU and memory settings remain provider-defined allocations rather than identical physical cores.

Readiness retry send/receive timestamps and native Beam PID observations remain in the raw logs. Guest milestone offsets and cross-machine observation bounds are recorded separately. We do not subtract half an assumed round trip or polling interval to claim exact provider-internal readiness.

## Earlier attempts and reliability

Beam's first attempt stopped during tools preparation because `wait=True` returned a running PID. No measured cases ran. The corrected adapter observes that same process without resubmitting it. A later generated disk-snapshot name was rejected with HTTP 400; native artifact inventory independently confirmed that no artifact matched the rejected source/name. The completed fresh/resume phases were retained, and only the five unrun disk cases ran in a separate corrected attempt. No first-attempt outcome was overwritten.

Blaxel's first native image build completed after the source terminal-readiness deadline had already elapsed. The harness stopped before making a readiness probe, with all ten measured cases unrun. Readiness now starts after deployment. Independent catalog inspection also exposed a cleanup route mismatch: the native image resource type is `sandbox`, whereas the old lookup used `sandboxes`. Cleanup now resolves the image metadata and confirms catalog absence. The failed preparation's VM, image and S3 prefix were independently removed before the unrun cases continued.

A failed activity window means a check failed, ran late or could not be confirmed. It is not by itself evidence of a crashed VM or a provider-only failure rate.

| Provider | Lifecycle | Index | Checks over 1 s late | Maximum lateness (s) | Observation |
| --- | --- | ---: | ---: | ---: | --- |
| Modal | fresh | 1 | 0 | 0.048 | All checks passed, but final check finished after the window (11.82 s; mounted S3 write/readback 10.54 s) |
| Modal | disk | 1 | 0 | 0.050 | All checks passed, but final check finished after the window (6.80 s; mounted S3 write/readback 5.68 s) |
| Modal | memory | 2 | 0 | 0.048 | Guest direct-HTTPS DNS timeout; mounted S3 and outside readback passed |
| exe.dev | resume | 3 | 1 | 1.129 | All individual checks passed; guest check started late (cause unknown) |
| exe.dev | resume | 5 | 1 | 1.437 | All individual checks passed; guest check started late (cause unknown) |
| Boat | fresh | 2 | 2 | 1.466 | Checks passed; a prior check ran past the next scheduled start |
| Boat | fresh | 4 | 2 | 1.487 | Checks passed; a prior check ran past the next scheduled start |
| Boat | fresh | 5 | 5 | 3.122 | Checks passed; a prior check ran past the next scheduled start |
| Boat | resume | 2 | 10 | 2.310 | Checks passed; a prior check ran past the next scheduled start |
| Boat | resume | 4 | 1 | 3.098 | Checks passed; a prior check ran past the next scheduled start |

## Run provenance

| Provider | Run ID | Measured phases |
| --- | --- | --- |
| Beam VMs | `2f79b492e84a` | fresh, resume |
| Beam VMs | `b2c7e66aa2ef` | disk |
| Tensorlake | `4af5c49c7a44` | disk, fresh, memory, resume |
| Modal | `8674a6b8715d` | disk, fresh, memory |
| Blaxel | `1d182b842458` | fresh, memory |
| exe.dev | `faaac8f9bcf2` | disk, fresh, resume |
| Boat | `74e3adacd3ec` | disk, fresh, resume |

## Cleanup and verification

Independent reads confirmed all 88 run-owned VMs absent and all 9 run-specific S3 prefixes empty. Shared S3 seeds retained their original hashes. All deletable run-owned bases and snapshots were checked independently; 1 Modal memory snapshot remains under provider expiry because the pinned SDK exposes no delete API. The Beam native image cache was retained by design; it is outside the per-run VM/snapshot inventory.

All 90/90 selected evidence ZIPs passed integrity checks. Resource samples cover all 90/90 activity intervals, allowing three seconds for the two-second recorder cadence. Earlier attempts, operation errors, unrun counts, native response observations, guest artifacts and cleanup proofs remain in the private evaluation workspace.

The run evidence records exact controller and guest-file hashes and prepared-image references. Selected Beam fresh/resume phases used frozen revision `362a64a`; its corrected disk phase, Tensorlake and Modal used `671c947`; Blaxel, exe.dev and Boat continued on `3765b93`. Cleanup of the failed Blaxel preparation also used `3765b93`.

The 35 portable-runner regression tests passed with the active checkout explicitly on `PYTHONPATH`. The wheel includes the new tools module and exact guest timing server. Source-integrity, whitespace and Python undefined-name/import checks were also run before the PR. These bounded results establish functionality, not N=1,000 throughput or provider reliability rates.

[Per-case timing CSV](data/prepared-n5-2026-11-13.csv)
