[한국어](README.md) / [English](README_EN.md)

# Embedding API Outage Scenarios

This guide shows how to watch, locally, the Worker retry and the circuit breaker open and close when the external embedding API fails transiently. A fake server (stub) in this folder replaces the embedding API the Worker calls, which creates the outage. No production code changes. Chapters 1-7 are manual tests where a person watches a terminal screen in an isolated local environment. [Chapter 8](#8-automated-test-429-and-5xx-backoff-recovery) attaches to an existing test environment, where a script judges 429/5xx retries and final completion.

> The scripts print their screens and logs in Korean. Quoted output below keeps the original Korean text with an English gloss in parentheses.

[← scripts index](../README_EN.md)

## Contents

1. [What to know first](#1-what-to-know-first)
2. [Prerequisites](#2-prerequisites)
3. [Step by step](#3-step-by-step) — from creating the environment to the first test
4. [Tests by mode](#4-tests-by-mode)
5. [Cleanup](#5-cleanup)
6. [Troubleshooting](#6-troubleshooting)
7. [Reference: checking only the stub responses](#7-reference-checking-only-the-stub-responses)
8. [Automated test: 429 and 5xx backoff recovery](#8-automated-test-429-and-5xx-backoff-recovery) — attach to a test environment and judge PASS/FAIL

---

## 1. What to know first

### Components

| File | Role |
|---|---|
| [`embedding-stub.py`](embedding-stub.py) | Fake embedding API. It returns errors for a set time (the outage window), then normal responses (1536-dimension vectors) |
| [`watch.py`](watch.py) | Terminal screen that redraws stub, breaker, and Job state in color every 2 seconds. It only reads the DB |

### Terms

| Term | Meaning |
|---|---|
| Job | A task that indexes one document. A row of `indexing_job` in the DB; its status goes `PENDING` → `PROCESSING` → `COMPLETED`. On failure it waits in `RETRY_WAIT` and tries again |
| breaker | A device that briefly blocks calls when an external API keeps failing. `CLOSED` (normal) → `OPEN` (blocked, 60 s) → `HALF_OPEN` (one probe call) → `CLOSED` on success, `OPEN` again on failure |
| Outage window | `--outage-seconds` (default 90 s) from the moment the stub starts. Inside it the stub returns errors; afterwards, normal responses |

### Caution

**The breaker row (`external_circuit_breaker`) is shared by every Worker that uses the same DB.** Running this test on a development or production DB affects all Workers on it. [Chapter 3](#3-step-by-step) avoids this by **creating a new isolated DB on your computer.** To use a shared DB, stop all other Workers and confirm the breaker is `CLOSED` first.

---

## 2. Prerequisites

| Item | Check | Used for |
|---|---|---|
| Docker Desktop (must be running) | `docker info` | Temporary DB and MinIO, psql for `watch.py` |
| Python 3 (verified on 3.11) | `python3 --version` | Stub, `watch.py` |
| JDK 21 | `java -version` | Running the Worker |
| This repository (worker) cloned | `./gradlew --version` at the repository root | Running the Worker |
| `git`, `curl`, `openssl` | | Fetching the schema, checks, hash calculation |

Four terminals are used. This document calls them **Terminal A** (preparation, uploading documents), **B** (Worker), **C** (progress screen), and **D** (stub).

**In B, C, and D, move to the repository root before running commands.** The commands below use the relative paths `./gradlew` and `scripts/embedding-outage/...`. `<path>` is the folder where you cloned this repository (worker).

```bash
cd <path>          # e.g. cd ~/project/spr1n6
ls gradlew         # you are in the right place if gradlew is listed
```

---

## 3. Step by step

Run the most common `5xx` mode from start to finish. Preparing the environment takes about 3 minutes (longer the first time, to pull images), and the test takes about 3 minutes.

### 3-1. Create a temporary DB (Terminal A)

The Worker's DB schema is owned by the API server repository. Fetch the server repository's migration files and apply them to a temporary PostgreSQL.

```bash
export WORK=/tmp/embedding-outage-demo
mkdir -p "$WORK"
git clone --depth 1 https://github.com/mash-up-kr/spr1n6-osscontest-server.git "$WORK/server"
```

Start PostgreSQL and wait until it is ready.

```bash
docker run -d --name fault-db -e POSTGRES_USER=worker -e POSTGRES_PASSWORD=worker \
  -e POSTGRES_DB=worker_integration_test -p 55434:5432 pgvector/pgvector:0.8.1-pg17
until docker exec fault-db pg_isready -U worker -d worker_integration_test >/dev/null 2>&1; do sleep 1; done
echo "DB ready"
```

Install and enable `pg_cron`, which the server migrations use. The DB restarts once to apply the setting.

```bash
docker exec --user root fault-db sh -c 'apt-get update -qq && apt-get install -y -qq --no-install-recommends postgresql-17-cron' >/dev/null
docker exec fault-db sh -c "printf \"shared_preload_libraries = 'pg_cron'\ncron.database_name = 'worker_integration_test'\ncron.use_background_workers = on\n\" >> \"\$PGDATA/postgresql.conf\""
docker restart fault-db >/dev/null
until docker exec fault-db pg_isready -U worker -d worker_integration_test >/dev/null 2>&1; do sleep 1; done
echo "pg_cron applied"
```

Create the extensions and apply the migrations in file-name order.

```bash
docker exec fault-db psql -U worker -d worker_integration_test -v ON_ERROR_STOP=1 -q \
  -c "CREATE EXTENSION IF NOT EXISTS pgcrypto" -c "CREATE EXTENSION IF NOT EXISTS pg_cron"
for f in $(ls "$WORK"/server/src/main/resources/db/migration/*.sql | sort); do
  docker exec -i fault-db psql -U worker -d worker_integration_test -v ON_ERROR_STOP=1 -q < "$f" >/dev/null \
    || { echo "migration failed: $f"; break; }
done
docker exec fault-db psql -U worker -d worker_integration_test -tAc \
  "SELECT 'breaker=' || state FROM external_circuit_breaker"
```

**Expected result:** the last line prints `breaker=CLOSED`. If it does not, see [Troubleshooting](#6-troubleshooting).

### 3-2. Create the document store (MinIO) (Terminal A)

The Worker reads uploaded documents from MinIO (S3-compatible storage).

```bash
docker run -d --name fault-minio -p 19100:9000 -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
until curl -fs http://127.0.0.1:19100/minio/health/ready >/dev/null; do sleep 1; done
echo "MinIO ready"
```

### 3-3. Create the upload helper (Terminal A)

Define the upload step as a function named `add_docs`. `add_docs 1 5` uploads documents 1 to 5 to MinIO and registers them in the DB. The server's DB trigger creates a `PENDING` Job for each document version, so you do not insert Jobs yourself. Paste the whole block below into Terminal A. The function disappears when you close this terminal, so paste it again.

```bash
export WORK=/tmp/embedding-outage-demo
mcx() { docker run --rm -v "$WORK/files:/data" --add-host=host.docker.internal:host-gateway \
  --entrypoint sh minio/mc -c "mc alias set f http://host.docker.internal:19100 minioadmin minioadmin >/dev/null && $*"; }
psqlw() { docker exec -i fault-db psql -U worker -d worker_integration_test -v ON_ERROR_STOP=1 -q "$@"; }

add_docs() {  # usage: add_docs <first number> <last number>
  mkdir -p "$WORK/files"
  mcx "mc mb --ignore-existing f/demo" >/dev/null
  psqlw -c "INSERT INTO tenant(name) SELECT 'demo' WHERE NOT EXISTS (SELECT 1 FROM tenant WHERE name = 'demo')"
  for n in $(seq "$1" "$2"); do
    printf 'Demo document %s. Text for the embedding outage test %s\n' "$n" "$RANDOM" > "$WORK/files/$n.txt"
    mcx "mc cp /data/$n.txt f/demo/docs/$n.txt" >/dev/null
    size=$(wc -c < "$WORK/files/$n.txt" | tr -d ' ')
    hash="sha256:$(openssl dgst -sha256 "$WORK/files/$n.txt" | awk '{print $NF}')"
    psqlw -v n="$n" -v size="$size" -v hash="$hash" <<'SQL'
WITH d AS (
  INSERT INTO document (tenant_id, owner_principal_id, title, latest_upload_version_no)
  SELECT id, 'demo', 'demo-' || :'n', 1 FROM tenant WHERE name = 'demo' ORDER BY id LIMIT 1
  RETURNING id
)
INSERT INTO document_version (document_id, version_no, embedding_version_no, source_object_key, original_filename,
                              mime_type, file_size, content_hash, created_by_principal_id)
SELECT id, 1, 1, 'docs/' || :'n' || '.txt', convert_to(:'n' || '.txt', 'UTF8'), 'text/plain', :size, :'hash', 'demo' FROM d;
SQL
    echo "document $n registered"
  done
}
```

### 3-4. Start the Worker (Terminal B)

Plain PostgreSQL has no OpenProxy-specific connection statement (`SET SERVER ROLE ...`), so the Worker fails to connect. First create a config file that empties that value. Run this once in Terminal A.

```bash
cat > "$WORK/local-override.yml" <<'EOF'
spring:
  datasource:
    hikari:
      connection-init-sql: ""
EOF
```

In Terminal B, go to the repository root and start the Worker. `OPENAI_API_KEY` is a dummy value, and `SPRING_AI_OPENAI_BASE_URL` points at the stub (`18081`). `SERVER_PORT` is changed so it does not collide with other services.

```bash
DB_URL=jdbc:postgresql://localhost:55434/worker_integration_test DB_USERNAME=worker DB_PASSWORD=worker \
SPRING_CONFIG_ADDITIONAL_LOCATION=/tmp/embedding-outage-demo/local-override.yml \
OPENAI_API_KEY=fault-test SPRING_AI_OPENAI_BASE_URL=http://127.0.0.1:18081/v1 \
INDEXING_STORAGE_BUCKET=demo INDEXING_STORAGE_ENDPOINT=http://127.0.0.1:19100 INDEXING_STORAGE_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin SERVER_PORT=18080 \
./gradlew bootRun
```

**Expected result:** the build runs first, then `Started WorkerApplicationKt in ... seconds` appears. That line means startup is complete. Keep this terminal open.

### 3-5. Open the progress screen (Terminal C)

Run from the repository root. The first run takes a while to pull the `postgres:17` image. The stub will run on `18081`, so match `--stub-url`.

```bash
DB_URL=jdbc:postgresql://localhost:55434/worker_integration_test DB_USERNAME=worker DB_PASSWORD=worker \
  python3 scripts/embedding-outage/watch.py --stub-url http://127.0.0.1:18081
```

**Expected result:** the screen refreshes every 2 seconds. There is no stub yet, so the first line reads `스텁 응답 없음` (no stub response), and you see `breaker CLOSED` and `Job PENDING 0 ...`.

### 3-6. Start the stub (Terminal D)

From the repository root, start it in `5xx` mode with a 90-second outage window. **The 90-second window starts at this moment.**

```bash
python3 scripts/embedding-outage/embedding-stub.py --port 18081 --mode 5xx --outage-seconds 90
```

**Expected result:** `stub mode=5xx outage=90.0s from=start port=18081` appears. The stub line in Terminal C changes to `mode=5xx 장애 구간 ████ NNs 남음` (outage window, NN s left).

### 3-7. Upload documents and watch (Terminal A → C)

Upload five documents at once, matching the Worker's concurrency (default 5). Look at Terminal C right away.

```bash
add_docs 1 5
```

Terminal C changes in the order below. Timing varies by environment.

| When | Screen | Meaning |
|---|---|---|
| Right after upload | Job `PENDING 5` → soon `RETRY_WAIT 5` | The Worker called the stub, got 503, and failed |
| Just after | breaker `OPEN`, `failures=5`, countdown from `열림 60s 남음` (open, 60 s left) | Five consecutive failures blocked calls. Stub request count: `503×5` |
| About 1 minute later | breaker `HALF_OPEN` (yellow) or `OPEN` again (red) | One probe call went out. Inside the outage window it fails and the breaker reopens |
| After the outage window | breaker `CLOSED` (green), Job `COMPLETED 5` | The probe call succeeded and the system recovered. The `오류/결과` (error/result) column of recent Jobs shows `INDEXED` |

The whole run takes about 3 minutes. If a probe call fails inside the outage window, `failures` goes above 5 and `OPEN` lasts one more period. If you saw all this, the `5xx` test succeeded.

---

## 4. Tests by mode

There are five modes, each imitating a **different real outage**. The goal is to compare, on the same screen (Terminal C), how the breaker and Jobs react differently.

| Mode | Outage imitated | breaker | Job | One-line summary |
|---|---|---|---|---|
| [`5xx`](#5xx--api-server-overload-or-maintenance) | API server overload or maintenance (503) | `OPEN` after 5 consecutive failures | `RETRY_WAIT`, then automatic recovery | Collects transient failures, blocks, then recovers by itself |
| [`timeout`](#timeout--the-api-does-not-respond) | The API does not respond | `OPEN` after 5 consecutive failures | `RETRY_WAIT`, then automatic recovery | No response also counts as a failure |
| [`401`](#401--wrong-api-key) | API key error or expiry | `OPEN` immediately on **1** failure | `RETRY_WAIT` | A configuration error needs no waiting; block at once |
| [`quota`](#quota--billing-limit-exceeded) | Billing limit or credits exhausted | `OPEN` immediately on **1** failure | `RETRY_WAIT` | Everything fails until a top-up, so block at once |
| [`429`](#429--too-many-requests-rate-limit) | Rate limit (too many requests) | **Does not open** | `RETRY_WAIT`, wait is at least `Retry-After` | The API is alive, so only the Job waits instead of blocking |

### Common procedure

`5xx` was already done in chapter 3. The other modes repeat the four steps below. **Keep the Worker (B) and progress screen (C) running.**

1. **Terminal D** — stop the running stub with Ctrl+C.
2. **Terminal A** — [reset](#reset-before-changing-mode). Leftover breaker state and Jobs from a previous test mix up the result.
3. **Terminal D** — start the stub for the mode. **The outage window starts now.**
4. **Terminal A** — upload documents right after starting the stub (`add_docs`). Continue the document numbers so they do not overlap earlier ones. Uploading late lets the outage window pass and the run ends without an outage.

Then watch Terminal C. All recovery is automatic. Nobody retries or closes the breaker by hand.

### Reset before changing mode

Stop the stub in Terminal D with Ctrl+C, then run the following in Terminal A. **Isolated DB only. Never run it on a shared DB.**

```bash
psqlw <<'SQL'
UPDATE external_circuit_breaker SET state = 'CLOSED', consecutive_failures = 0, opened_until = NULL, generation = generation + 1;
UPDATE document SET searchable_version_id = NULL;
DELETE FROM document_chunk;
DELETE FROM indexing_job;
DELETE FROM document_version;
DELETE FROM document;
SQL
```

---

### 5xx — API server overload or maintenance

**What outage:** the embedding API server is overloaded or under maintenance and returns `503`. A transient outage that improves shortly.

**What to watch:** the whole flow in which accumulated failures open the breaker and block API calls, then after some time a single probe call checks whether the API is back and the breaker closes by itself.

**Run:** do 3-6 and 3-7 of chapter 3 as they are (stub `--mode 5xx --outage-seconds 90`, five documents).

**What to see on screen**

| When | breaker | Job | Meaning |
|---|---|---|---|
| Right after upload | `CLOSED` | `PENDING 5` → `RETRY_WAIT 5` | All five got 503, failed, and wait to retry. Stub request count `503×5` |
| Just after | **`OPEN`**, `failures=5`, countdown from `열림 60s 남음` (open, 60 s left) | `RETRY_WAIT 5` | The 5-consecutive-failure threshold blocked calls. Even when the retry time arrives, no Job is claimed |
| About 1 minute later | `HALF_OPEN` (yellow) or `OPEN` again | | One probe call went out. Inside the outage window it fails, `failures` rises to 6, and the breaker reopens |
| After the outage window | **`CLOSED`** (green), `failures=0` | `COMPLETED 5`, result `INDEXED` | The probe call succeeded and the system recovered. The backlog was processed |

**Why:** 503 is classified as "the API cannot accept now" (`FAILURE`) and adds to the consecutive-failure count (default 5). At the threshold, calls are blocked for 60 seconds so the already struggling API is not hit further.

**Time:** about 3 minutes in total.

---

### timeout — the API does not respond

**What outage:** the network is cut or the server hung, so requests get no response. Not even an error code arrives, and the client gives up on timeout after waiting.

**What to watch:** even with no error response, the timeout counts as a failure and the breaker opens just as with `5xx`.

**Run:** only this mode requires **restarting the Worker.** The Worker's default embedding timeout is 60 seconds, which makes the test too long, so add `SPRING_AI_OPENAI_EMBEDDING_TIMEOUT=PT5S` (5 seconds). It must be shorter than the time the stub holds the connection (`--hang-seconds`), so the Worker times out first.

1. Do steps 1-2 of the common procedure (stop the stub, reset).
2. **Terminal B** — stop the Worker with Ctrl+C, and from the repository root start it again with one more environment variable. Wait until `Started WorkerApplicationKt ...` appears.

   ```bash
   DB_URL=jdbc:postgresql://localhost:55434/worker_integration_test DB_USERNAME=worker DB_PASSWORD=worker \
   SPRING_CONFIG_ADDITIONAL_LOCATION=/tmp/embedding-outage-demo/local-override.yml \
   OPENAI_API_KEY=fault-test SPRING_AI_OPENAI_BASE_URL=http://127.0.0.1:18081/v1 \
   SPRING_AI_OPENAI_EMBEDDING_TIMEOUT=PT5S \
   INDEXING_STORAGE_BUCKET=demo INDEXING_STORAGE_ENDPOINT=http://127.0.0.1:19100 INDEXING_STORAGE_REGION=us-east-1 \
   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin SERVER_PORT=18080 \
   ./gradlew bootRun
   ```

3. **Terminal D** and **Terminal A**:

   ```bash
   python3 scripts/embedding-outage/embedding-stub.py --port 18081 --mode timeout --hang-seconds 30 --outage-seconds 150
   add_docs 9 13
   ```

**What to see on screen:** about 5 seconds after upload, `RETRY_WAIT 5`, breaker `OPEN` (`failures=5`), and error column `OpenAIIoException`. Stub request count is `hang×5`. Later transitions match `5xx`.

**Why:** a timeout is not a mapped exception, so it is classified as a general failure (`FAILURE`) and adds to the count like `5xx`.


---

### 401 — wrong API key

**What outage:** the API key is wrong, expired, or revoked. No request succeeds until the key is fixed.

**What to watch:** there is no reason to wait for five failures on a configuration error, so the breaker opens **immediately on a single failure.**

**Run:**

```bash
# Terminal D
python3 scripts/embedding-outage/embedding-stub.py --port 18081 --mode 401 --outage-seconds 120
# Terminal A (one document)
add_docs 6 6
```

**What to see on screen**

| When | breaker | Job | Meaning |
|---|---|---|---|
| Right after upload | **`OPEN`**, `failures=1` | `RETRY_WAIT 1`, error column `EMBEDDING_AUTHENTICATION_FAILED` | Opened on one failure. Stub request count `401×1` |
| About 1 minute later | `HALF_OPEN` or `OPEN` again | | Inside the outage window the probe call gets 401 again and the breaker reopens |
| After the outage window (120 s) | **`CLOSED`** | `COMPLETED`, `INDEXED` | Normal responses return as if the key were fixed, and the system recovers |

The Worker log (Terminal B) shows `EMBEDDING_CALL_REJECTED` once.

**Why:** 401 is classified as a configuration error (`CONFIGURATION`), so the breaker opens at once regardless of the failure count. The Job still goes to `RETRY_WAIT`, not a permanent `FAILED`, because it can succeed once the key is fixed.


---

### quota — billing limit exceeded

**What outage:** the OpenAI account ran out of credits or passed its billing limit (429 `insufficient_quota`). Every request fails until a top-up.

**What to watch:** even though it is a 429, it is handled differently from a rate limit, and the breaker opens on one failure like `401`.

**Run:**

```bash
# Terminal D
python3 scripts/embedding-outage/embedding-stub.py --port 18081 --mode quota --outage-seconds 90
# Terminal A (one document)
add_docs 7 7
```

**What to see on screen:** the breaker becomes `OPEN` with `failures=1`, and the Job is `RETRY_WAIT` with error column `EMBEDDING_QUOTA_EXCEEDED`. Recovery looks the same as `401`.

**Why:** when the response body `code` is `insufficient_quota`, the problem is common to every call and is classified as a configuration error (`CONFIGURATION`). It is told apart from a rate limit (`rate_limit_exceeded`) by the body.


---

### 429 — too many requests (rate limit)

**What outage:** requests crowd into a short time and the API says "send again later (`Retry-After`)". The API itself is fine.

**What to watch:** here the **breaker does not open**; instead each Job waits `Retry-After` and tries again. Unlike the four modes above, **the place to look is the Job's `다음 재시도` (next retry) column, not the breaker.**

**Run:**

```bash
# Terminal D
python3 scripts/embedding-outage/embedding-stub.py --port 18081 --mode 429 --outage-seconds 90
# Terminal A (one document)
add_docs 8 8
```

**What to see on screen**

| When | breaker | Job | Meaning |
|---|---|---|---|
| Right after upload | **stays `CLOSED`**, `failures=0` | `RETRY_WAIT 1`, error column `RATE_LIMITED` | A 429 arrived but the breaker is unchanged. Stub request count `429×1` |
| Then | `CLOSED` | `다음 재시도` (next retry) 30 s or more ahead | `Retry-After: 30` is respected; it waits at least that long |
| When time comes | `CLOSED` | `attempts` increases | It tries again. Inside the outage window (90 s) it gets another 429 and repeats |
| After the outage window | `CLOSED` | `COMPLETED`, `INDEXED` | The retry succeeded |

**Why:** a rate limit (`RATE_LIMIT`) is a call-rate problem, not an API outage, so it neither adds to the failure count nor changes the breaker. Instead of blocking all calls, only that Job's next retry time is pushed back by at least `Retry-After`. The breaker opens only when a `HALF_OPEN` probe call receives a 429.


---

## 5. Cleanup

In each terminal, stop the Worker (B), progress screen (C), and stub (D) with Ctrl+C. The temporary container used by the progress screen is removed with it. Then run the following in Terminal A.

```bash
docker rm -f fault-db fault-minio
rm -rf /tmp/embedding-outage-demo
```

---

## 6. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `no such file or directory: ./gradlew`, or a `scripts/embedding-outage/...` file is not found | You ran it outside the repository root. `cd <path>`, check with `ls gradlew`, and run again |
| `Cannot connect to the Docker daemon` | Start Docker Desktop |
| `port is already allocated` | Stop the other process using that port (`55434`, `19100`). If containers remain, `docker rm -f fault-db fault-minio` |
| `debconf: delaying package configuration` | A warning during the `pg_cron` install in 3-1. Safe to ignore |
| `migration failed: ...` | Read the error for the printed file. Check whether creating the `pgcrypto`/`pg_cron` extensions failed earlier |
| `syntax error at or near "ROLE"` in the Worker log | The `local-override.yml` of [3-4](#3-4-start-the-worker-terminal-b) is missing or the path differs. It must be emptied through a config file, not an environment variable |
| `Connection is not available` in the Worker log | The DB is down or the port in `DB_URL` differs |
| `DB 조회 실패` (DB query failed) on the progress screen | Read the error shown. A wrong password or port, or unapplied migrations, is the usual cause |
| `스텁 응답 없음` (no stub response) on the progress screen | The stub in Terminal D is off, or `--stub-url` and the stub port differ. The `--stub-url` default is `http://127.0.0.1:8081` |
| Jobs stay `PENDING` after upload | The Worker (Terminal B) is still starting or is down. Look for `Started WorkerApplicationKt`. If the breaker is `OPEN`, not claiming Jobs while it is open is normal |
| `5xx` but the breaker does not open | Fewer than five documents were uploaded. The threshold is five consecutive failures |

---

## 7. Reference: checking only the stub responses

To check only the stub, without the Worker or DB, run the following.

```bash
python3 scripts/embedding-outage/embedding-stub.py --port 18081 --mode 429 --outage-seconds 5 &
curl -si -X POST localhost:18081/v1/embeddings -d '{"input":["a","b"]}' | head -5   # 429 + Retry-After: 30
sleep 5
curl -s  -X POST localhost:18081/v1/embeddings -d '{"input":["a","b"]}' | head -c 120 # normal response (two vectors)
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18081/other -d '{}'         # 404
curl -s localhost:18081/stats                                                          # mode, time left, request count per response
kill %1
```

Stub options are `--port` (8081), `--outage-seconds` (90), `--mode` (5xx), `--hang-seconds` (10), and `--outage-from` (start). `--outage-from first-request` counts the outage window from the first embedding request instead of from startup. In `timeout` mode the stub gives no response for `--hang-seconds`, then drops the connection. To run it with Docker:

```bash
docker run --rm -p 8081:8081 -v "$PWD/scripts/embedding-outage/embedding-stub.py:/stub.py:ro" python:3-slim \
  python -u /stub.py --port 8081 --mode 5xx --outage-seconds 90
```

`watch.py` options are `--stub-url` (default `http://127.0.0.1:8081`, environment variable `STUB_URL`), `--interval` (2 s), `--once` (draw once and exit), and `--no-color`. Job counts and recent Jobs cover the whole `indexing_job` table in the DB, not only what the scenario created. Ending with Ctrl+C or SIGTERM removes the temporary psql container (`spr1n6-watch-psql`); a forced kill (SIGKILL) leaves it, but the next run removes it first.


---

## 8. Automated test: 429 and 5xx backoff recovery

One script judges PASS/FAIL on whether the Worker retries by the backoff policy when the embedding API returns 429 or 5xx, and finally completes the same Job after the API recovers. Unlike chapters 3-4, it **does not create an environment.** Like [`db-outage`](../db-outage/README_EN.md) and [`worker-crash`](../worker-crash/README_EN.md), it attaches to an already running test environment, only **reads** the DB, and handles the existing Worker containers directly with `docker`. It changes neither the server's compose file nor the Worker settings.

How it works: the script reads the image, environment variables, and network of the two existing Workers with `docker inspect` and stops both for the test. It creates **one temporary Worker** (`embedding-test-worker`) from the same image and environment variables, with only `SPRING_AI_OPENAI_BASE_URL` redirected to a **temporary stub** (`embedding-test-stub`). The stub is reachable by container name inside the same user-defined network. The stub source is passed as a base64 argument, so no host file mount or published port is needed, and it works with a remote Docker. If the stub image is missing, it is pulled before the Workers are stopped. When the test ends (including on error or Ctrl+C), the temporary containers are removed and **only the previously stopped Workers** are restarted.

| File | Role |
|---|---|
| [`embedding-backoff-recovery.sh`](embedding-backoff-recovery.sh) | Run this single file (it contains Python code, and the stub uses `embedding-stub.py` in the same folder). It starts the temporary stub and Worker, observes the Job of a document a person uploads, and judges backoff, error code, breaker, and final completion |

### Preparation

| Item | Check |
|---|---|
| Python 3, `psql` | `python3 --version`, `psql --version`. A `psql` installed by `brew install libpq` on macOS is not linked into PATH, but the script finds and uses the Homebrew path (`/opt/homebrew/opt/libpq/bin` and so on) |
| `docker` | Where the script runs, `docker ps` must show the existing Worker containers (default `aidocs-dev-worker-1`, `aidocs-dev-worker-2`). Same approach as [`worker-crash`](../worker-crash/README_EN.md); a remote Docker works. The Workers must be attached to a user-defined network (such as a compose network). The stub image (default `python:3.12-slim`), unlike the Worker image, may not exist on the server. If missing, the script pulls it once **before stopping the Workers** (up to 10 minutes). If the image registry is unreachable, pass a Python image already on the server with `--stub-image` |
| OpenProxy SSH tunnel | Default `localhost:15432`. Open it beforehand with `ssh -N -L 15432:<OpenProxy-VIP>:6432 ...`. The script does not open a tunnel |
| DB password | `~/.pgpass` or `PGPASSWORD`. Never written in the script |
| Uploading a document from the web | The API server and web must be running |

Connection environment variables are `DB_HOST` (localhost), `DB_PORT` (15432), `DB_USER` (aidocs), `DB_NAME` (aidocs), and `DB_SCHEMA` (public). Only when attaching to plain PostgreSQL (the isolated DB of chapter 3), add `DB_SERVER_ROLE=` (empty) to drop the OpenProxy-specific `SET SERVER ROLE` statement.

**Cautions:**

- Run it **last**, after finishing the other scenarios (`db-outage`, `worker-crash`).
- During the test, **the existing Workers stop.** Only the two designated Workers may consume this queue.
- The `external_circuit_breaker` row is shared by all Workers. Do **not run it at the same time** as other tests or uploads using the same DB. Before starting, the script checks that no unfinished Job exists and the breaker is `CLOSED`, and does not start otherwise.
- The stub returns fake vectors even in the normal period, so test embeddings remain temporarily in the DB.
- If cleanup did not happen because of an interruption or failure, recover with the command below.

```bash
docker rm -f embedding-test-worker embedding-test-stub; docker start aidocs-dev-worker-1 aidocs-dev-worker-2
```

### Running

Run from the repository root.

```bash
bash scripts/embedding-outage/embedding-backoff-recovery.sh --check-only   # check DB and Worker settings and pre-pull the stub image (does not stop Workers)
bash scripts/embedding-outage/embedding-backoff-recovery.sh                # test 429 then 5xx
bash scripts/embedding-outage/embedding-backoff-recovery.sh --modes 429    # one mode only
```

Start with `--check-only` to confirm connection, settings comparison, and the image pull. Then run one mode with `--modes 429`, and finally run everything.

When `>>> 지금 웹에서 테스트 문서 하나를 업로드하세요 <<<` (upload one test document on the web now) appears for a mode, upload **exactly one** document. With `--upload-command './upload.sh'`, that command uploads automatically. After uploading, one line prints each time the Job status changes, and the stub's request log (`docker logs -f`) is mixed into the same terminal. Waiting for the temporary Worker to start takes up to 2 minutes at first, and each mode takes about 2-3 minutes. If the stub receives no request at all (the temporary Worker cannot see the stub), the script ends with exit code 2 without judging.

### Judgment

| Item | Criterion |
|---|---|
| At least 2 retries observed | If fewer, `판정 불가` (cannot judge, exit code 2). Increase `--outage-seconds` and run again |
| attempt_count increases | The attempt count rises at each `RETRY_WAIT` |
| Backoff interval | The wait is `base × attempts × 0.8-1.2`. For 429 it is at least `Retry-After` (30 s). The amount by which the first observation lags the real failure (polling interval + 2 s) is tolerated |
| Error code | 429: `RATE_LIMITED` and the breaker stays `CLOSED`. 5xx: the error code is non-empty |
| Final | The same Job is `COMPLETED`, `outcome=INDEXED`, within 5 attempts |

For 5xx, the breaker state is only shown on screen and not judged, because one document cannot reach five consecutive failures and the breaker may not open. Watch the `OPEN` transition with the `5xx` test of chapter 4 (five documents).

Exit codes: 0 = all PASS, 1 = FAIL or environment error, 2 = cannot judge for lack of observations, 130 = interrupted.

### Settings

| Option / environment variable | Default | Meaning |
|---|---|---|
| `--outage-seconds` | 90 | Outage window, counted from the first embedding request. Out-of-range values prevent starting |
| `INDEXING_RETRY_BASE_DELAY` (`--base-delay`) | PT30S | Must be the **same as the Worker's value.** Used to compute the backoff range. If the copied Worker environment has the same setting, the script compares it automatically and, if different, prints `--base-delay <value> 으로 다시 실행하세요` (run again with `--base-delay <value>`) and does not start (if absent it assumes the default and passes) |
| `INDEXING_QUEUE_POLL_INTERVAL` (`--queue-poll`) | PT30S | Same as the Worker (compared automatically). Used to compute the lower bound of the outage window |
| `INDEXING_MAX_ATTEMPTS` (`--max-attempts`) | 5 | Same as the Worker (compared automatically) |
| `WORKER1_CONTAINER` (`--worker1`) | aidocs-dev-worker-1 | Existing Worker 1 to stop during the test. Its image, environment variables, and network are copied to the temporary Worker |
| `WORKER2_CONTAINER` (`--worker2`) | aidocs-dev-worker-2 | Existing Worker 2 to stop during the test. Its image must match Worker 1 |
| `STUB_IMAGE` (`--stub-image`) | python:3.12-slim | Image that runs the stub. It only needs a `python` command. If missing, it is pulled before starting |
