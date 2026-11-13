# Live CLI validation: 12 November 2026

We ran five cases per supported lifecycle on Boat, Tensorlake, Beam VMs and Modal, using the full app, Chromium, mounted-S3 workload and common 60-second activity window. Snapshot-source VMs were preparation and are excluded from the measured counts. No N=100 test was run.

## Results

| Provider | Case | Workflows passed | Clean 60-second windows |
| --- | --- | ---: | ---: |
| Boat | Fresh | 5/5 | 4/5 |
| Boat | Disk fork | 5/5 | 4/5 |
| Boat | Disk resume | 5/5 | 3/5 |
| Tensorlake | Fresh | 5/5 | 5/5 |
| Tensorlake | Disk fork | 5/5 | 5/5 |
| Tensorlake | Memory fork | 5/5 | 5/5 |
| Tensorlake | Resume | 5/5 | 5/5 |
| Beam VMs | Fresh | 5/5 | 5/5 |
| Beam VMs | Disk fork | 5/5 | 5/5 |
| Beam VMs | Resume | 0/5 | Not eligible |
| Modal | Fresh | 5/5 | 5/5 |
| Modal | Disk fork | 5/5 | 5/5 |
| Modal | Memory fork | 5/5 | 5/5 |

A completed workflow and a clean activity window are separate results. Boat's missed windows contained successful checks that ran late. Beam's resume failures occurred while observing preserved state, before a new workload could pass. These small samples do not establish a provider failure rate.

All 65 measured cases have a valid evidence archive containing resource samples. Beam’s disk-only follow-up completed the five disk cases left unrun after its original preparation-name failure.

## Fixes found by the live runs

- Keep generated VM names within provider limits, including snapshot-preparation VMs.
- Keep an image's working Node/npm installation. Installing the distribution's npm package over Boat's preinstalled Node caused a package conflict.
- Check out the fixture's pinned commit. The public bundle does not contain the private branch name used by the original harness.
- Recreate the S3 mount directory after a disk fork, and enable `compat_dir` only when the installed s3fs advertises it. The tested images included s3fs 1.90 and 1.93.
- Start Tensorlake's persistent guest services through its native process API. A detached child of a short-lived command did not survive that command's deadline.
- Encode guest RPC results before sending them through native exec. The pinned Tensorlake SDK inserts newlines at output chunk boundaries, which broke long JSON responses.
- Preserve the AWS region in the run configuration while redacting the temporary credentials. Overbroad redaction prevented later cleanup from using the saved configuration.
- Drain late activity observations without counting them as timely passes. Keep interrupted cases visible.
- Bound saved-state observation in both the guest and controller. A blocked FUSE request can delay the guest's Python timeout handler.
- Run Modal's image lookup/deletion on the SDK's event loop. A raw call from the controller loop stalled cleanup.

Earlier failed attempts remain separate from corrected attempts. No failed case was silently replaced inside an attempt. The final patch has 34 passing offline tests, including the public Git bundle, long RPC responses, saved-state deadlines and Modal's event-loop boundary.

## Remaining limits

At the end of the November 12 run, Beam's preserved S3 read was still unexplained: the terminal worked, and a later separate read of the same object succeeded with matching bytes, but the original read did not recover during observation. The November 13 investigation below identified a harness configuration regression. The original 0/5 result above remains unchanged.

All 128 run-owned VMs across the original and corrected attempts were independently confirmed deleted or terminal. All 14 run-specific S3 prefixes were empty, and the shared seed was unchanged. Every deletable snapshot was independently confirmed absent. One Modal memory snapshot remains until its seven-day expiry because the pinned SDK has no deletion API. Beam’s cached base image remains by design.

These CLI runs include cold package/browser installation. Their timings must remain separate from the historical benchmark's prepared-image timings. The Blaxel and exe.dev adapters were not run in this validation.

## Beam resume investigation: 13 November 2026

The portable workload omitted the s3fs connection, read/write and retry limits used by the earlier Beam harness. After pause/resume, a preserved S3 connection could stop receiving acknowledgements. The mount then waited longer than our saved-state observation allowed. A working terminal was therefore recorded as a failed resume while the verifier was blocked reading the saved S3 file.

A paired live check used the same Beam image, resources, workload and state checks. Its only behavioral change was adding `connect_timeout=5`, `readwrite_timeout=15` and `retries=2` to s3fs:

| Live check | Resume workflow | Clean resumed activity | Observation |
| --- | --- | --- | --- |
| Existing settings, one VM | Failed after 47.64 s | Not eligible | Verifier blocked on FUSE; the S3 socket had queued data and seven TCP retransmissions. |
| Restored settings, one VM | Passed in 27.60 s | 1/1 | s3fs logged a 15-second read/write timeout, retried the request and recovered. |
| Actual CLI with restored settings, three VMs | 3/3 passed, 26.86–31.12 s | 3/3 | All saved-state comparisons, workflows and activity checks passed. |

Both fresh workflows and fresh activity windows passed. The diagnostic processes shared the same mount and network namespaces, ruling out inspection of a different mount. The corrected case verified saved app, browser, RAM and disk state without remounting S3 or restarting the saved app/browser. The state-verification deadline was not increased.

The subsequent CLI confirmation also passed 3/3 fresh workflows and 3/3 fresh activity windows. Across the paired diagnostic and CLI confirmation, this investigation used five VMs total. Independent API checks confirmed all five deleted and all three run-specific S3 prefixes empty; the shared seed was unchanged. No larger test or other provider was run.

This identifies the missing S3 recovery budget as the actionable harness defect. It does not establish why the pre-pause TCP connection stopped receiving acknowledgements, or imply that arbitrary network connections survive VM suspension. These small checks do not establish a provider reliability rate.
