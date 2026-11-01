[한국어](README.md) / [English](README_EN.md)

# scripts

A collection of scripts that reproduce and check the Worker's failure handling locally. Each failure scenario is one folder, and the test procedure is in that folder's `README_EN.md`.

## Failure scenarios

| Folder | Failure reproduced | Components | Test procedure |
|---|---|---|---|
| [`embedding-outage/`](embedding-outage/) | Transient external embedding API outages (5xx, quota, 429, timeout, 401), breaker transitions, and final completion after 429/5xx backoff retries | Fake server `embedding-stub.py`, progress screen `watch.py`, automatic judge `embedding-backoff-recovery.sh` | [Procedure](embedding-outage/README_EN.md) · [Automated test](embedding-outage/README_EN.md#8-automated-test-429-and-5xx-backoff-recovery) |
| [`worker-crash/`](worker-crash/) | Abnormal exit of a processing Worker, lease expiry, and takeover by another Worker | Fault injection and verification `worker-lease-recovery.sh` | [Procedure](worker-crash/README_EN.md) |
| [`db-outage/`](db-outage/) | OpenSQL outage during processing, DB connection recovery, and completion of the same Job | Fault injection and verification `db-job-recovery.sh` | [Procedure](db-outage/README_EN.md) |

## Adding a new scenario

1. Create a `scripts/<scenario-name>/` folder and put the scripts there.
2. In the folder's `README.md`, write the modes, expected results, and test procedure. Follow the format of [`embedding-outage/README.md`](embedding-outage/README.md), and add a `README_EN.md` with the same content.
3. Add a row to the table above, in both `README.md` and `README_EN.md`.
