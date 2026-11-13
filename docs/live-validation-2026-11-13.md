# Public CLI validation and timing audit: 13 November 2026

We ran five cases per supported lifecycle with the public `sandbox-bench` CLI: the full app, Chromium and mounted-S3 workload followed by the common 60-second activity window. Providers ran sequentially, with concurrency five. Source preparation is excluded from the measured counts. No N=100 or N=1,000 provider run was launched.

After the fixes, all 90 selected workflow cases passed. Clean activity passed in 85/90 cases. The five missed windows were on Boat and are explained below; they remain failures in the results.

## Results

All times are seconds. Full-workflow values include cold setup where applicable. A zero in the bootstrap column means that phase used saved state rather than installing tools. P95 with five samples is the slowest sample.

| Provider | Lifecycle | Workflows passed | Clean activity | Terminal mean | Full mean | Full P95 | Bootstrap mean |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Tensorlake | fresh | 5/5 | 5/5 | 1.491 | 38.156 | 39.164 | 31.992 |
| Tensorlake | disk | 5/5 | 5/5 | 1.058 | 6.505 | 6.522 | 0.000 |
| Tensorlake | memory | 5/5 | 5/5 | 0.652 | 3.874 | 3.977 | 0.000 |
| Tensorlake | resume | 5/5 | 5/5 | 1.171 | 3.645 | 3.723 | 0.000 |
| Modal | fresh | 5/5 | 5/5 | 0.824 | 37.861 | 44.251 | 32.881 |
| Modal | disk | 5/5 | 5/5 | 0.789 | 5.889 | 7.742 | 0.000 |
| Modal | memory | 5/5 | 5/5 | 0.887 | 4.885 | 4.915 | 0.000 |
| Beam VMs | fresh | 5/5 | 5/5 | 2.230 | 62.706 | 66.700 | 53.382 |
| Beam VMs | disk | 5/5 | 5/5 | 10.052 | 17.474 | 18.356 | 0.000 |
| Beam VMs | resume | 5/5 | 5/5 | 1.543 | 26.940 | 27.515 | 0.000 |
| Blaxel | fresh | 5/5 | 5/5 | 0.691 | 51.904 | 55.032 | 47.215 |
| Blaxel | memory | 5/5 | 5/5 | 0.999 | 4.748 | 4.899 | 0.000 |
| exe.dev | fresh | 5/5 | 5/5 | 2.998 | 94.192 | 95.696 | 83.301 |
| exe.dev | disk | 5/5 | 5/5 | 2.790 | 12.623 | 12.659 | 0.000 |
| exe.dev | resume | 5/5 | 5/5 | 2.111 | 6.895 | 6.981 | 0.000 |
| Boat | fresh | 5/5 | 4/5 | 0.963 | 122.735 | 155.292 | 110.279 |
| Boat | disk | 5/5 | 3/5 | 2.105 | 17.244 | 19.926 | 0.000 |
| Boat | resume | 5/5 | 3/5 | 0.750 | 10.830 | 12.573 | 0.000 |

The table uses the final verification attempt for each lifecycle. The two successful exe.dev phases and its corrected disk-fork phase came from separate attempts. Earlier failures below remain recorded; they were not overwritten. Boat resumes are disk boots, not RAM restores. The requested resource configuration was four CPUs and 8192 MB where the adapter supports it; provider-defined CPU allocations are not identical physical cores.

Boat's five missed windows were cadence failures: one fresh, two disk forks and two resumes. All 15 guests were armed at least 29 seconds before their window, and all 180 individual guest checks and external S3 readbacks passed. Mounted-S3 operations took long enough for subsequent checks to start more than one second late. The worst window accumulated 27.5 seconds of lateness. This establishes a workload cadence problem in this run, not whether the delay belongs to Boat, the S3 mount implementation or S3 itself.

## Harness fixes

- **exe.dev output framing:** explicitly request NDJSON, ignore blank keepalive lines, and still require the final exit-code frame. A real 31-second command returned a leading blank line followed by valid output and exit status.
- **exe.dev disk contents:** flush the guest filesystem before pausing the disk source. A paired probe copied a 1,200,000-byte file and a 13-byte script as zero-byte files without the flush; after `sync`, both sizes and hashes matched the source. The corrected five disk-fork workflows and activity windows passed.
- **Blaxel startup:** remove an invalid 180-second creation timeout, which the installed SDK rejected before making an API call. Use the public Debian/Python image required by the workload installer. Run persistent services through the native process API with keep-alive enabled for their configured lifetime.
- **History-dependent controller work:** persist only the changed trial or operation during dispatch. Keep full checkpoints outside measured workflows, and avoid rewriting the full history for each artifact or deleted VM. Reporting and cleanup reconstruct pending records after an interrupted process.
- **Activity scheduling:** allow for observed native-control latency before the shared window starts, and record each guest's arming time. The original Boat fresh windows started about 17–19 seconds late. The corrected run keeps the same 60-second duration, 12 checks and one-second lateness threshold.
- **Uncertain mutations:** check for an unresolved lifecycle operation before preparing the next snapshot source, as well as before each batch.

## What the timing audit establishes

A local serialization-only check repeated recorded history to model larger evidence files. It did not create cloud resources or generate provider benchmark results.

| History size | Previous full-history save | Changed-record save |
| --- | ---: | ---: |
| About 0.5 MB | 5.7 ms | 0.54 ms |
| About 9.7 MB | 105.7 ms | 0.55 ms |
| About 96.8 MB | 1,103.2 ms | 0.56 ms |

The updated dispatch write was about 22 KB in this deliberately large completed-trial example, independent of accumulated history. This removes the measured history-sized delay; it does not establish N=1,000 provider throughput or reliability.

Portable-CLI fresh timing includes package and browser installation. Historical prepared-image runs used different setup and control transports, so their headline workflow times are not directly comparable.

Native exec transport and external verification still take time and remain part of the portable workflow measurement. This audit removes the demonstrated history-dependent overhead; it does not claim zero measurement overhead or provider-only latency.

## Earlier attempts retained

- exe.dev first failed terminal readiness because the adapter expected NDJSON without requesting it. Its five VMs were cleaned up.
- The next exe.dev attempt exposed blank keepalive lines during long setup commands. Five fresh cases failed and their resumes were ineligible; disk preparation then failed.
- After framing was fixed, exe.dev fresh and resume phases passed, but all five disk copies failed to start the workload because recent guest writes were not flushed.
- Blaxel's first five create attempts and snapshot-source preparation were rejected locally by SDK timeout validation. Independent inventory found no matching VMs. Memory cases remained unrun in that attempt.
- The first Boat run passed 15/15 workflows but 7/15 activity windows. Fresh windows had a late start; several resumed windows began on time but fell behind during mounted-S3 operations. That run also picked up an added timestamp field during later source preparation, so the final Boat validation used a separate frozen checkout.

A missed activity window means a check failed, ran late or could not be confirmed. It does not by itself establish a crashed VM or a provider failure rate.

## Cleanup and evidence

Independent provider reads confirmed all 130 run-owned and diagnostic VMs absent. All 13 run-specific S3 prefixes were empty, and every shared seed matched its original hash. All deletable snapshots were confirmed absent. One Modal memory snapshot remains subject to the provider's seven-day expiry because the pinned SDK exposes no delete API. Beam's pre-existing cached base image was deliberately reused and retained.

The final selected cases have 90/90 valid evidence archives. Resource samples cover the activity interval in 90/90 archives, allowing three seconds for the two-second sampler cadence. Raw per-case outcomes, operation records, command/error logs, S3 read attempts, guest logs and resource samples remain in the evaluation workspace.

Final frozen verification revisions: `aae31cf` for Blaxel and Tensorlake; `32f7915` for Modal; `2f3c237` for Boat. Beam ran from `800d144`; exe.dev fresh/resume ran from `800d144` and its corrected disk phase from `2c5fd00`. Those earlier runs validate the provider operations; the frozen runs also exercise the final shared writer and scheduler.

| Provider | Selected run ID |
| --- | --- |
| Tensorlake | `8b59ad984f2b` |
| Modal | `6306b24854ce` |
| Beam | `afd60d469d37` |
| Blaxel | `407890cf6ac9` |
| exe.dev fresh/resume | `37ab405b6637` |
| exe.dev disk | `5e629039a3f0` |
| Boat | `fb259a6bc965` |

## Local checks

`python -m pytest -q tests/test_portable_runner.py` passed all 35 tests, including interrupted journal recovery without losing ownership or restoring stale checkpoint data. `python check_source_integrity.py` and `git diff --check` also passed. The validation used existing temporary AWS role credentials and provider keys in the controller; no provider keys were installed in guests.
