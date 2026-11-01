[한국어](README.md) / [English](README_EN.md)

# OpenSQL Outage Scenario

This guide checks that, when OpenSQL stops and recovers during Job processing, the unfinished Job resumes from its stored state and finally completes. It force-kills Patroni on the current Primary, then observes the DB connection failure and recovery through OpenProxy and the final completion of the same Job.

> The script prints its progress in Korean. Quoted output below keeps the original Korean text with an English gloss in parentheses.

[← scripts index](../README_EN.md)

## Contents

1. [What to know first](#1-what-to-know-first)
2. [Prerequisites](#2-prerequisites)
3. [Step by step](#3-step-by-step)
4. [Settings and success criteria](#4-settings-and-success-criteria)
5. [Cleanup](#5-cleanup)
6. [Troubleshooting](#6-troubleshooting)

---

## 1. What to know first

### Components

| File | Role |
|---|---|
| [`db-job-recovery.sh`](db-job-recovery.sh) | Observes a new Job in `PROCESSING` and sends SIGKILL to the Primary's Patroni. After the DB connection failure and write-path recovery, it waits for `COMPLETED` of the same Job |

### Terms and expected result

| Term | Meaning |
|---|---|
| OpenProxy | The path the Worker and psql use to reach the DB. Each session sets the write target with `SET SERVER ROLE TO 'default'` |
| Primary | The DB node that currently accepts writes. The leader name comes from Patroni's `/cluster` |
| Write-path recovery | State where, connected through OpenProxy, `pg_is_in_recovery()=false` and `transaction_read_only=off` are confirmed |

**Expected result:** observe `PROCESSING` of a new Job → SIGKILL the current Primary's Patroni → real DB query failure → writable DB connection recovered → `COMPLETED` of the same Job. The Worker keeps running throughout.

### Caution

**This really force-kills the current Primary's Patroni and blocks systemd automatic restart.** Run it in an isolated test OpenSQL, OpenProxy, and Patroni environment. Do not overlap it with other tests or operations work, and upload only one document. It does not start if an unfinished Job exists.

The fault injected is `SIGKILL` on the Patroni process. The script does not stop a whole DB node or the whole OpenSQL cluster. It separately observes whether a real DB query failure happened; if it cannot observe one, it does not report success.

Promotion and DB recovery after the injection are left to the environment's HA setup. The old Primary's Patroni and the `Restart=no` setting are not restored automatically, so follow [chapter 5](#5-cleanup). Test an abnormal Worker exit with the separate [Worker crash scenario](../worker-crash/README_EN.md).

---

## 2. Prerequisites

| Item | Check | Used for |
|---|---|---|
| Bash, Python 3 | `bash --version`, `python3 --version` | Runner helper, psql response and time-limit handling |
| `psql`, `ssh` | `psql --version`, `ssh -V` | DB queries, connecting to Patroni nodes |
| Test OpenSQL, OpenProxy, Patroni | Connection check below | DB fault injection and automatic recovery |
| SSH key and permission for the Patroni nodes | SSH to each node | Remote `curl`, non-interactive `sudo systemctl` and drop-in file creation |
| A running Worker | Check logs and state in the deployment | Resuming unfinished work after DB recovery |
| API server and web, source storage, embedding API | A document can be uploaded on the web | A new INDEX Job and indexing completion |

Use an environment with the latest Server migrations and the recovery settings applied. The Worker's DB path must pass through the same OpenProxy. Plain PostgreSQL has no OpenProxy-specific `SET SERVER ROLE` statement, so it cannot be used as is.

Confirm the Worker has `FAULT_INJECTION_ENABLED=false`, and prepare an environment where source storage and the embedding API are healthy. During the DB outage, do not restart the Worker by hand or add other Jobs.

Two terminals are used. **Terminal A** is for the script and recording results; **Terminal B** is for the OpenProxy SSH tunnel. Upload documents on the web. Terminal A uses Bash and moves to the repository root.

```bash
bash
cd "<this repository path>"
ls gradlew
```

---

## 3. Step by step

### 3-1. Open the OpenProxy tunnel (Terminal B)

Open a tunnel through the existing access path. Replace the `<...>` values below for your test environment. Skip this step if a tunnel is already open.

```bash
ssh -N -L "15432:<OpenProxy-VIP>:6432" -i "<SSH-key-path>" "<SSH-user>@<access-node>"
```

Keep the tunnel open until the test ends.

### 3-2. Set the DB and SSH connection details (Terminal A)

```bash
export DB_HOST=localhost DB_PORT=15432 DB_USER=aidocs DB_NAME=aidocs DB_SCHEMA=public
read -r -s -p 'DB password: ' PGPASSWORD; echo
export PGPASSWORD

export SSH_KEY='<SSH key file path>'
export SSH_USER='<SSH user>'
export DB1='<SSH address of postgresql1>'
export DB2='<SSH address of postgresql2>'
export DB3='<SSH address of postgresql3>'
export PATRONI_API_PORT=8008
```

With `~/.pgpass` you can skip the password prompt. `DB1`, `DB2`, and `DB3` are **the SSH addresses of the Patroni nodes**, not DB query addresses. `DB1` to `DB3`, `SSH_KEY`, and `SSH_USER` have no defaults, and if any is empty the script stops before injecting the fault. If you wrote the same names in the `.env` at the repository root, load them with `set -a; source .env; set +a`.

`DB3` is the access point for the `/cluster` query. The returned names `postgresql1`, `postgresql2`, and `postgresql3` must match the address mapping above. A cluster with other names cannot run with the current script.

### 3-3. Check the connection (Terminal A)

Set the role and look up the Primary in the same psql session. Both statements go through standard input instead of the `-c` option.

```bash
psql -X -w -qAt -v ON_ERROR_STOP=1 -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<'SQL'
SET SERVER ROLE TO 'default';
SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), host(inet_server_addr());
SQL
ssh -i "$SSH_KEY" -o ConnectTimeout=8 -o BatchMode=yes "$SSH_USER@$DB3" \
  "curl -fsS -m 3 localhost:${PATRONI_API_PORT}/cluster"
```

**Expected result:** the DB answers `f|off|<server address>`. `/cluster` has exactly one leader and its name is in the node mapping above. You must be able to SSH to every node, and on the target node run the needed `sudo` commands without a password.

### 3-4. Start the test and upload a document (Terminal A)

This script has no `--help` option. Running it after the connection check starts the fault injection procedure. Results print to the terminal, so use `tee` to keep them in a file.

```bash
set -o pipefail
bash scripts/db-outage/db-job-recovery.sh 2>&1 | tee /tmp/db-outage-demo.log
result=${PIPESTATUS[0]}
echo "test exit code: $result"
```

When the prompt `준비되면 ENTER를 누른 뒤 웹에서 ... 업로드하세요` (press ENTER when ready, then upload ... on the web) appears, press Enter and upload one new document. Use one large enough to observe `PROCESSING`. After that, Patroni on the Primary is force-killed without further confirmation.

### 3-5. Read the progress and success criteria

| Step | Screen / state | Meaning |
|---|---|---|
| 1 | Writable Primary confirmed | The current OpenProxy path is healthy |
| 2-3 | New Job found, `PROCESSING` | Records the unfinished Job's ID, owner, attempt count, and lease |
| 4 | `장애 주입 대상: <leader> (<IP>)` (fault target: <leader> (<IP>)), DB failure observed | The old Primary's Patroni was SIGKILLed and a real DB query failed |
| 5 | `DB 쓰기 경로 복구` (DB write path recovered) | A writable DB was queried again through OpenProxy |
| 6 | `COMPLETED` of the same Job, final `PASS` | The existing Job's final completion was confirmed without creating a new Job |

**Write down the fault target name and IP.** You need them to restore the node even if the test is interrupted or fails. If you ran with `tee`, they are in `/tmp/db-outage-demo.log`.

---

## 4. Settings and success criteria

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DB_HOST`, `DB_PORT` | `localhost`, `15432` | OpenProxy query path |
| `DB_USER`, `DB_NAME`, `DB_SCHEMA` | `aidocs`, `aidocs`, `public` | Query account, DB, schema |
| `SSH_KEY`, `SSH_USER` | None. Set before running | SSH to Patroni nodes |
| `DB1`, `DB2`, `DB3` | None. Set all three before running | Mapping of Patroni node names to SSH addresses |
| `PATRONI_API_PORT` | `8008` | Patroni API port on the remote node |
| `UPLOAD_TIMEOUT` | `300` | Polls for finding a new Job |
| `PROCESSING_TIMEOUT` | `300` | Polls for observing the processing state |
| `OUTAGE_TIMEOUT` | `180` | Polls for observing the DB query failure |
| `RECOVERY_TIMEOUT` | `300` | Polls for observing write-path recovery |
| `COMPLETION_TIMEOUT` | `600` | Polls for observing final completion |
| `POLL_INTERVAL` | `1` s | Wait between queries |

The existing code uses `*_TIMEOUT` as a poll count. The default interval is 1 second, but psql and SSH call time adds to it, so it is not a strict upper bound on elapsed time. Changing `POLL_INTERVAL` changes the total wait too.

### Judgment scope

Success requires observing all of: `PROCESSING` before the injection, a real DB query failure after it, write-path recovery, and **`COMPLETED` of the same Job ID.** If the DB failure was missed or the Job finished before the injection, exit code `2` asks for a retest. A final `FAILED` or a recovery or completion timeout is `1`; success is `0`.

`RETRY_WAIT 관측` (RETRY_WAIT observed) and `재선점 정황 관측` (re-claim indication observed) are reference output. The Worker may resume with its existing ownership, so success is reported even when both are `0` as long as the completion criteria hold. Whether a re-claim happened, whether the promoted node is the same, and the `INDEXED` result and chunk integrity are not required checks of this script. It also does not verify that duplicate external API calls are prevented.

The script only reads the DB and never edits Job status. There is no automatic JSON result folder or Worker log collection, so check the terminal record above together with the Worker logs in the deployment.

---

## 5. Cleanup

SSH to the **fault target node** the script printed. Replace `<fault-target-IP>` below with the IP you wrote down. Do not run this on the newly promoted Primary.

```bash
ssh -i "$SSH_KEY" -o ConnectTimeout=8 -o BatchMode=yes "$SSH_USER@<fault-target-IP>"
```

On that node, delete the drop-in the test created and start Patroni again.

```bash
sudo rm -f /run/systemd/system/patroni.service.d/norestart.conf
sudo systemctl daemon-reload
sudo systemctl start patroni
sudo systemctl status patroni --no-pager
```

If a drop-in existed at the same path before the test, restore its original content. The script overwrites this file, so in an environment that already has one, keep a copy of its content before running. Check the cluster rejoin state with your environment's own procedure. Forcing a switchover back to the original Primary is not included.

The script does not stop the Worker. If you opened the tunnel for this test, close it with Ctrl+C in Terminal B. The script does not delete Jobs and chunks in the DB or the uploaded source.

## 6. Troubleshooting

| Symptom | What to check |
|---|---|
| Cannot connect to a writable Primary | SSH tunnel, OpenProxy path, DB credentials. Confirm the Worker's DB path is the same |
| Current Primary not found | `DB3` SSH address, Patroni API port, exactly one leader in the `/cluster` response |
| Unknown DB node | Whether Patroni names are `postgresql1/2/3` and the `DB1/2/3` mapping is right |
| SSH or sudo failure | Key, user, node address, non-interactive sudo permission. Some commands may have run, so also check the target node's state |
| DB connection failure not observed | Whether stopping Patroni led to a SQL connection failure, or recovery was faster than the query. Restore and retest without reporting success |
| Job completed before the injection | Retest with a document that takes longer to process |
| Recovery or completion timeout | OpenProxy and Patroni state, Worker DB wait logs, source storage, embedding API, and breaker |
| Test failed or interrupted with Ctrl+C | If after the injection, restore as in chapter 5. There is no automatic restore |
