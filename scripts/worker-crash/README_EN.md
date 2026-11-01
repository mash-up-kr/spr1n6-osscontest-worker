[한국어](README.md) / [English](README_EN.md)

# Worker Crash Scenario

This guide checks that, when a Worker processing a Job exits abnormally, another Worker claims the unfinished Job after the lease expires and finally completes it. It `SIGKILL`s Worker 1, then observes the DB's automatic reclaim and Worker 2's takeover in order.

> The script prints its progress in Korean. Quoted output below keeps the original Korean text with an English gloss in parentheses.

[← scripts index](../README_EN.md)

## Contents

1. [What to know first](#1-what-to-know-first)
2. [Prerequisites](#2-prerequisites)
3. [Step by step](#3-step-by-step)
4. [Settings and result files](#4-settings-and-result-files)
5. [Cleanup](#5-cleanup)
6. [Troubleshooting](#6-troubleshooting)

---

## 1. What to know first

### Components

| File | Role |
|---|---|
| [`worker-lease-recovery.sh`](worker-lease-recovery.sh) | Controls the run state of two Workers and checks lease expiry, automatic reclaim, re-claim, and completion of the same Job. Saves JSON results and Worker logs |

### Terms and expected result

| Term | Meaning |
|---|---|
| lease | The deadline by which a Worker may process a Job. Extended by heartbeat while running |
| Automatic reclaim | The DB's `pg_cron` reclaim procedure returns a `PROCESSING` Job with an expired lease to `PENDING`. The Worker does not reclaim by itself |
| epoch | The generation of execution ownership. A new claim raises the `execution_epoch` of the Job and the document together |

**Expected result:** the same Job goes `PROCESSING` → lease expiry → `PENDING` → `PROCESSING` under another owner → `COMPLETED`. The final `outcome` is `INDEXED`, at least one chunk is stored, and there are no duplicate chunk numbers.

### Caution

**This really stops the Docker containers you name and force-kills Worker 1.** Prepare a test OpenSQL, OpenProxy, source storage, and two Workers. The Workers consuming this DB queue must be exactly the two named, and only one document is uploaded during the test. The script refuses to start if an unfinished Job exists.

Worker 2 is stopped gracefully at the start and restarted after `PENDING` reclaim is confirmed. This order prevents it from claiming right after the reclaim and ruining the observation. Worker 1 stays stopped, and even on error or Ctrl+C the script tries to resume the Worker 2 it stopped.

The DB is only read. The script neither changes lease or status directly nor calls the reclaim procedure by hand. Test an OpenSQL outage with the separate [DB outage scenario](../db-outage/README_EN.md).

---

## 2. Prerequisites

| Item | Check | Used for |
|---|---|---|
| Bash, Python 3 | `bash --version`, `python3 --version` | Runner helper, Python standard library |
| Docker and two running Workers | `docker ps` | Graceful stop, SIGKILL, restart, and log collection for the Workers |
| `psql` | `psql --version` | Querying DB state through OpenProxy |
| Test OpenSQL and OpenProxy | Connection check below | A writable Primary and the queue and chunk store |
| API server and web, source storage, embedding API | A document can be uploaded on the web | Registering a new INDEX Job and final indexing |
| DB automatic reclaim setup | Check the `pg_cron` and `reclaim_expired_indexing_jobs` setup | Automatically reclaiming expired leases |

Use an environment with the latest Server migrations applied. The Server owns the DB schema and the reclaim procedure, so this script does not install them. Plain PostgreSQL has no OpenProxy-specific `SET SERVER ROLE` statement, so it cannot be used as is.

The two Workers use the same DB and source storage, and their `INDEXING_WORKER_ID` must differ. The default per-run UUID also works. Keep `FAULT_INJECTION_ENABLED=false`, and make sure no external tool recreates the SIGKILLed Worker automatically. Prepare an environment where a normal document indexes successfully first.

Two terminals are used. **Terminal A** is for the script; **Terminal B** is for the OpenProxy SSH tunnel. Upload documents on the web. Terminal A uses Bash and moves to the repository root.

```bash
bash
cd "<this repository path>"
ls gradlew
```

---

## 3. Step by step

### 3-1. Open the OpenProxy tunnel (Terminal B)

Where OpenProxy cannot be reached directly, open a tunnel through the existing access path. Replace the `<...>` values below for your test environment. Skip this step if a tunnel is already open.

```bash
ssh -N -L "15432:<OpenProxy-VIP>:6432" -i "<SSH-key-path>" "<SSH-user>@<access-node>"
```

Keep this terminal open until the test ends.

### 3-2. Set the DB and Workers (Terminal A)

```bash
export DB_HOST=localhost DB_PORT=15432 DB_USER=aidocs DB_NAME=aidocs DB_SCHEMA=public
export WORKER1_CONTAINER=aidocs-dev-worker-1
export WORKER2_CONTAINER=aidocs-dev-worker-2
read -r -s -p 'DB password: ' PGPASSWORD; echo
export PGPASSWORD
```

Run the password prompt above in Bash. With `~/.pgpass` you can skip it. The Worker's `DB_URL`, `DB_USERNAME`, and `DB_PASSWORD` have different names from this script's `DB_*` variables. Point both at the same target.

Set the role and look up the Primary in the same psql session. Both statements go through standard input instead of the `-c` option.

```bash
psql -X -w -qAt -v ON_ERROR_STOP=1 -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<'SQL'
SET SERVER ROLE TO 'default';
SELECT pg_is_in_recovery(), current_setting('transaction_read_only');
SQL
docker inspect --format '{{.Name}} running={{.State.Running}} paused={{.State.Paused}}' \
  "$WORKER1_CONTAINER" "$WORKER2_CONTAINER"
```

**Expected result:** the DB answers `f|off`, and both Workers show `running=true paused=false`.

### 3-3. Start the test (Terminal A)

```bash
bash scripts/worker-crash/worker-lease-recovery.sh
```

Check the two Workers and the consumer count in the start notice, then press Enter. The script stops Worker 2 gracefully and waits for a new upload. **After `지금 웹에서 테스트 문서 하나를 새로 업로드하세요` (upload one new test document on the web now) appears**, upload one document large enough. Finish every existing unfinished Job before uploading.

The default kill point is `EMBEDDING`. If processing passes that phase too quickly, lift the phase restriction.

```bash
bash scripts/worker-crash/worker-lease-recovery.sh --kill-phase ANY
```

### 3-4. Read the progress and success criteria

| Step | Screen / state | Meaning |
|---|---|---|
| 1-2 | Primary and Workers checked, waiting for upload | Only Worker 1 can receive the new Job |
| 3 | Worker 1 SIGKILL, `after-kill.json` saved | It exited mid-processing, and the Job stays `PROCESSING` under the old owner |
| 4 | `PENDING` after lease expiry | Confirms the original lease expired by DB time and observes the reclaim that released ownership |
| 5 | Worker 2 starts, `PROCESSING` under a new owner | Observes a new ownership with a different owner and a higher epoch and attempt count |
| 6-7 | `COMPLETED`/`INDEXED`, success after chunk checks | The handed-over execution and the completing execution have the same epoch, and the result is stored |

After completion, the Job's `worker_id`, `lease_until`, and `next_retry_at` must be empty. There must be exactly one Job with the same `source_event_id`. Wait time varies with the lease length, cron period, reclaim grace time, and Worker startup time.

---

## 4. Settings and result files

### Options

```bash
bash scripts/worker-crash/worker-lease-recovery.sh --help
```

| Option | Default | Purpose |
|---|---|---|
| `--worker1`, `--worker2` | `WORKER1_CONTAINER`, `WORKER2_CONTAINER`, or the names in the example above | The Worker to kill and the Worker that takes over |
| `--kill-phase` | `KILL_PHASE` or `EMBEDDING` | `ANY`, `DOWNLOADING`, `PARSING`, `CHUNKING`, `EMBEDDING` |
| `--poll-interval` | `0.25` s | State query interval |
| `--upload-timeout` | `300` s | Wait after upload for the chosen processing phase |
| `--recovery-timeout` | `300` s | Wait for lease expiry and automatic reclaim after SIGKILL |
| `--completion-timeout` | `600` s | Wait for Worker 2 to re-claim and complete |
| `--output-dir` | `worker-recovery-<time>-<PID>` in the current folder | A new result folder. Refused if it exists |
| `--yes` | off | Skip the Enter confirmation before starting. The upload wait stays |

The defaults of `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_NAME`, and `DB_SCHEMA` match the example in 3-2.

### Result files

| File | Contents |
|---|---|
| `environment.json`, `summary.json` | Test targets and settings; exit code and message; old and new owners |
| `before-kill.json`, `after-kill.json` | The Job just before and just after the kill |
| `recovered-pending.json`, `handoff.json`, `completed.json` | Automatic reclaim, new ownership, final completion |
| `timeline.jsonl` | Per-step Job state recorded with DB time |
| `result-checks.json` | Number of Jobs for the same event, chunk count, duplicate chunk numbers |
| `worker1.log`, `worker2.log` | Worker logs since the test started |

Files exist only up to the step reached. Exit codes: `0` = success, `1` = failure or environment error, `2` = cannot judge for lack of observations or a race, `130` = interrupted. Result files are run records, so do not commit them with the source.

---

## 5. Cleanup

After checking the final message and the container state, restart Worker 1.

```bash
docker start "$WORKER1_CONTAINER"
docker inspect --format '{{.Name}} running={{.State.Running}}' \
  "$WORKER1_CONTAINER" "$WORKER2_CONTAINER"
```

If Worker 2 did not resume automatically, also run `docker start "$WORKER2_CONTAINER"`. If you opened the tunnel for this test, close it with Ctrl+C in Terminal B. The script does not delete Jobs and chunks in the DB or the uploaded source.

## 6. Troubleshooting

| Symptom | What to check |
|---|---|
| Primary connection failure or a `SET SERVER ROLE` error | SSH tunnel, OpenProxy path, DB credentials. Confirm you did not connect straight to plain PostgreSQL |
| Cannot parse the DB JSON response | Whether psql standard output is mixed with non-JSON text such as notices or proxy messages. The script assumes a pure JSON response |
| Refused to start because an unfinished Job exists | Wait until existing work finishes. Stop other uploads and consumers |
| Finished before the kill | Retry with a document that takes longer, or use `--kill-phase ANY` |
| Reclaim timeout | `pg_cron` run history and permissions, the reclaim procedure, lease length, Primary reclaim grace. Raise `--recovery-timeout` if needed |
| Worker 1 started again | Whether automatic restart or the deployment tool recreates the container |
| Completed but no re-claim was observed | Cannot judge. Shorten the query interval or retest with a document that takes longer |
| Not completed on Worker 2 | A unique worker-id, fault injection settings, source storage, embedding API, breaker state, and logs |
