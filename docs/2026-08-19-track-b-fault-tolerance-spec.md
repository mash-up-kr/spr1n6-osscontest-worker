# Track B 구현 스펙 — 장애 복구 확장

> **담당**: 은지 (A — 수집 & 워커 확장성, `[임베딩 워커 - 그룹2]`)
> **작성일**: 2026-08-19 (갱신: 2026-08-19, FAULT_TOLERANCE.md 최신본 + 실제 코드 상태 반영)
> **상태**: 구현 진행 중 — **P0-4(인라인 재시도 전환)·배치 리스너 전환은 이미 완료**(`5f52bd5`, `d4ff1e6`). 나머지 P0-5부터 착수.
> **근거 문서**: [`docs/FAULT_TOLERANCE.md`](../../FAULT_TOLERANCE.md)(설계 근거·논의 과정), [`2026-08-16-indexing-worker-pipeline-design.md`](./2026-08-16-indexing-worker-pipeline-design.md) §4(기존 Track B 개요, 이 문서로 대체)
> **갱신 사유**: 초판 작성 이후 FAULT_TOLERANCE.md가 개정되며 세 가지가 바뀌었다 — ① P0-4/배치 리스너가 실제로 구현·병합됨, ② P0-3 백오프가 지수(미결정)에서 선형(확정·구현됨)으로 바뀜, ③ P0-1 에러 분류가 `sealed class` 설계 대신 기존 예외 화이트리스트 방식으로 구현됨. 이 개정판은 실제 코드(`IndexingPipelineRunner`, `IndexingFailureService`, `IndexingKafkaListener`)를 직접 확인해 반영했다.

---

## 0. 이 문서의 성격

`FAULT_TOLERANCE.md`는 **왜 이렇게 하는가**(장애 시나리오, 논의, 트레이드오프)를 담은 설계 근거 문서다. 이 문서는 그 결론을 **무엇을 구현하는가**(인터페이스, 상태 전이 규칙, 스키마, 설정값, 완료 조건)로 옮긴 실행 스펙이다. 각 항목은 근거가 필요하면 `FAULT_TOLERANCE.md`의 절 번호를 인용하고, 재서술하지 않는다.

**핵심 전환 한 줄 요약**: 재시도를 ack **이후**(폴러 기반)에서 ack **이전**(리스너 인라인)으로 옮긴다 — **이미 완료됨**(P0-4, `5f52bd5`/`d4ff1e6`). 이 전환 하나가 B-Gap-1(`PROCESSING` 고아)을 구조적으로 없앴고, 나머지 항목 대부분(P0-1~P0-3, P0-5)은 이 전환이 안전하게 성립하기 위한 전제 조건이다.

**범위 밖**: Track A가 이미 완성한 정합성 메커니즘(세 겹 멱등성, fencing, UPSERT 수렴, 청킹 결정성)은 그대로 유지하며 이 문서에서 재설계하지 않는다(`FAULT_TOLERANCE.md` 부록 원칙 7).

**(2026-08-19 추가) trailing DELETE는 `IndexingProcessor` 구현 요구사항에서 제외됐다.** 재청킹으로 청크 수가 줄어드는 경우를 대비한 trailing DELETE(`chunk_no > :finalChunkCount`)는 PR #4 리뷰(`docs/track-a-design.md` §6)에서 제외하기로 정리됐다 — 청킹이 결정적(같은 입력 → 같은 출력)이고 현재 도달 가능한 전략이 `FixedTokenChunker` 하나뿐이라, 이 절이 막으려던 시나리오(같은 Job을 다른 워커가 처리했더니 청크 수가 달라지는 경우) 자체가 발생하지 않는다는 데 합의했다. 이 문서(Track B)는 애초에 trailing DELETE를 별도 항목으로 다루지 않으므로 P0~P2 목록에는 영향이 없다.

---

## 1. 상태 전이 불변조건 (모든 항목의 전제)

> **`indexing_job`이 `PROCESSING` 또는 `RETRY_WAIT`인 동안에는, 그에 대응하는 커밋되지 않은 Kafka 메시지가 반드시 존재한다.**

- 이 불변조건을 지키는 유일한 규칙: **ack 이후에 Job 상태를 다시 `PROCESSING`/`RETRY_WAIT`으로 만드는 코드를 작성하지 않는다.**
- 위반 여부는 §8.2 카오스 테스트로 검증한다 — `PROCESSING`이 하나라도 30분 뒤 남아있으면 위반.
- `RETRY_WAIT`은 상태값으로 유지하되 **관측 전용**이다. 이 값을 읽어 재처리를 트리거하는 스케줄러/폴러를 두지 않는다 — 이미 이 원칙대로 전환 완료됨(§5 제거 대상 참고).

---

## 2. 스키마 변경

```sql
-- P1-1: 진행률 노출 (phase만 — total_chunks/processed_chunks는 두지 않는다, 2026-08-19 정리)
ALTER TABLE indexing_job ADD COLUMN phase VARCHAR(30);   -- DOWNLOADING/PARSING/CHUNKING/EMBEDDING/PUBLISHING
```

삭제 스윕 chunk 삭제가 반복 실패하는 경우에 대한 별도 방어(무한 재시도 카운터)는 두지 않는다(2026-08-19 결정) — 실제로 발생하지 않는다고 보고 리스크를 감수한다. 필요성이 관측되면 그때 추가한다(부록 원칙 8).

Outbox 보정(발행 실패 감지·재발행)은 `doc-relay` 쪽에서 이미 구현돼 있어 Track B 스코프에서 제외한다(2026-08-19 확인) — §4에서 별도 P1-3 항목을 두지 않는다.

`indexing_job.status` 허용값(`PENDING/PROCESSING/RETRY_WAIT/COMPLETED/FAILED`)은 변경하지 않는다 — Track A §1.5/§2 결정 유지.

---

## 3. 설정 스키마

```yaml
indexing:
  limits:
    max-file-size-bytes: 209715200      # 200MB — P0-2(a)
    parse-timeout: PT60S                # P0-2(b)
    download-timeout: PT30S             # 기존 Track A 값 유지
    embedding-timeout: PT30S            # P0-2
  retry:
    max-attempts: 5                     # P0-3, INDEXING_MAX_ATTEMPTS — 이미 구현·적용됨
    base-delay: PT30S                   # P0-3, INDEXING_RETRY_BASE_DELAY — 선형 백오프 기준 간격, 이미 구현·적용됨
  consumer:
    max-poll-interval-ms: 900000        # P0-3 — 미반영, 실제 application.yml은 아직 600000
  db-health-gate:
    check-interval-ms: 5000             # P0-5
    pause-nack-delay: PT5S
```

---

## 4. 컴포넌트 스펙

우선순위 기준: ①막지 않으면 데이터가 조용히 사라지는가 ②장애를 감지할 수 있는가 (`FAULT_TOLERANCE.md` §3 도입부).

### P0-1. 에러 분류 (Permanent / Transient)

**막는 장애**: 영구 실패에 재시도 낭비, 일시 실패의 재시도 부족.

**실제 구현 방식 — sealed class가 아니라 화이트리스트.** 초판 스펙은 `PermanentIndexingException`/`TransientIndexingException` sealed class 설계를 제시했지만, 실제 구현(`IndexingPipelineRunner.isRetryable()`)은 기존 예외 타입(`InvalidEventException`, `ContentIntegrityException`, `EmptyExtractionException`, `ChunkLimitExceededException`, `TotalTokenLimitExceededException`, `UnsupportedMimeTypeException`)을 화이트리스트로 `when`에 나열해 `permanent`를 판정하는 방식이다. sealed class는 코드베이스에 존재하지 않는다.

```kotlin
// IndexingPipelineRunner.kt — 실제 구현 형태
private fun isRetryable(e: Exception): Boolean = when (e) {
    is InvalidEventException,
    is ContentIntegrityException,
    is EmptyExtractionException,
    is ChunkLimitExceededException,
    is TotalTokenLimitExceededException,
    is UnsupportedMimeTypeException -> false   // 영구 실패
    else -> true                               // 그 외는 기본적으로 재시도 가능
}
```

**에러 코드 매핑** (판정 로직이 아니라 `last_error_code`에 기록되는 값 기준)

| 분류 | 코드 | 발생 지점 |
|---|---|---|
| Permanent | `EMPTY_EXTRACTION` | `ChunkGuard` |
| Permanent | `CHUNK_LIMIT_EXCEEDED` | `ChunkGuard` |
| Permanent | `HASH_MISMATCH` | `ContentIntegrityException` |
| Permanent | `UNSUPPORTED_MIME` | `DocumentParserRegistry` |
| Permanent | `CORRUPTED_FILE` | 파서가 열지 못함 |
| Permanent | `FILE_TOO_LARGE` | P0-2 |
| Permanent | `DOCUMENT_DELETED` | `failActiveJobsForDocument()` |
| Transient | `STORAGE_TIMEOUT` / `STORAGE_SERVER_ERROR` | S3 |
| Transient | `EMBEDDING_RATE_LIMIT`(429) / `EMBEDDING_SERVER_ERROR`(5xx) | 임베딩(태성 — §7.1) |
| Transient | `PARSE_TIMEOUT` | P0-2 |
| (별도) | `DB_CONNECTION_LOST` | 재시도 예산을 쓰지 않고 `nack`(P0-5) |

**동작 규칙**

- **(정정)** 임베딩 API는 429 응답에 `Retry-After`를 실어주지 않는다. 그래서 `Retry-After` 기반 조기종료 로직은 두지 않기로 확정됐다 — 429도 다른 재시도 가능 예외와 동일하게 P0-3의 선형 백오프로 5회 재시도 후 `FAILED`로 종결한다. (초판의 "Retry-After > 75초면 즉시 FAILED" 규칙은 폐기)
- `IndexingProcessor`(태성 소유)가 던지는 예외도 `isRetryable()`의 화이트리스트에 새 케이스로 추가돼야 리스너(P0-4)의 분기가 성립한다 — §7.1 요청 사항.

**미결 사항 — 착수 전 결정 필요**: `isRetryable()`의 화이트리스트 방식을 그대로 유지하며 예외가 늘 때마다 `when` 절에 추가할지, 아니면 `PermanentIndexingException`/`TransientIndexingException` sealed class로 마이그레이션해 예외 자체에 분류 책임을 위임할지 결정이 필요하다(`FAULT_TOLERANCE.md` §3 P0-1 원문 명시).

**완료 조건**

- [ ] 위 "미결 사항"이 확정됨(화이트리스트 유지 vs sealed class 마이그레이션)
- [ ] 위 표의 모든 코드가 `isRetryable()`(또는 마이그레이션 결정 시 sealed class)의 분류와 일치함
- [ ] `DB_CONNECTION_LOST`는 `isRetryable()` 분기 대상이 아니라 별도 catch 절(P0-5)에서 처리됨
- [ ] 0바이트 PDF·스캔 PDF·300MB 파일·429 반복·임베딩 반복 실패 케이스 테스트 통과(`FAULT_TOLERANCE.md` §5.4)

---

### P0-2. 리소스 가드 — 파일 크기 상한 + 파싱 타임아웃 + 스트리밍

**막는 장애**: OOM으로 인한 fleet 붕괴, 재시도 예산 붕괴 (B-Gap-3, B-Gap-4).

**전제**: P0-3의 예산 계산이 "1회 처리 시간 ≤ 120초"를 전제로 하므로, 이 항목이 없으면 예산 자체가 무의미하다.

**예외 타입 명명 — P0-1의 실제 분류 방식(화이트리스트)을 따른다.** 아래 코드의 `FileTooLargeException`/`ParseTimeoutException`은 `PermanentIndexingException`/`TransientIndexingException`(sealed class, 미채택 — P0-1 참고) 대신 일반 `RuntimeException` 서브타입으로 만들고, `IndexingPipelineRunner.isRetryable()`의 `when` 절에 새 케이스로 등록한다: `FileTooLargeException`은 영구 실패 목록에 추가(`-> false`), `ParseTimeoutException`은 기본값(`else -> true`, 재시도 가능)을 그대로 따르므로 별도 등록이 필요 없다.

**(a) 파일 크기 상한 — 다운로드 전 판정**

```kotlin
if (documentVersion.fileSize > properties.maxFileSizeBytes) {
    throw FileTooLargeException("FILE_TOO_LARGE", "...")   // isRetryable()에 영구 실패로 등록 필요
}
```

**(b) 파싱 타임아웃**

```kotlin
private val parseExecutor = Executors.newFixedThreadPool(concurrency)

fun parseWithTimeout(parser: DocumentParser, source: Path): List<ParsedBlock> {
    val future = parseExecutor.submit<List<ParsedBlock>> { parser.parse(source).toList() }
    return try {
        future.get(properties.parseTimeout.seconds, TimeUnit.SECONDS)   // 60초
    } catch (e: TimeoutException) {
        future.cancel(true)
        throw ParseTimeoutException("PARSE_TIMEOUT", "...")   // isRetryable() 기본값(재시도 가능)을 그대로 따름
    }
}
```

타임아웃 처리는 워커를 살려두므로 `ParseTimeoutException`이 `isRetryable()`의 기본 분기(재시도 가능)를 타 인라인 재시도(P0-4)로 이어진다 — hang을 방치해 리밸런스가 나는 것과는 다른 경로임을 구분한다.

**(c) 스트리밍 복원**

- `Sequence`를 청커까지 그대로 흘린다(`.toList()` 호출 제거).
- 다운로드는 메모리 `ByteArray` 대신 임시 파일로 스풀한다.
- 해시는 `DigestInputStream`으로 스트리밍 계산한다(전체 바이트를 다시 메모리에 올리지 않는다).
- 임시 파일은 `finally` 블록에서 반드시 삭제한다.

**완료 조건**

- [ ] 세 타임아웃(다운로드 30s + 파싱 60s + 임베딩 30s) 합이 120초와 일치
- [ ] 1만 페이지 PDF 처리 시 힙 사용량이 상한 내(`FAULT_TOLERANCE.md` §5.6)
- [ ] 순환 참조 PDF가 `PARSE_TIMEOUT`(60초)으로 종결되고 다음 메시지가 정상 처리됨, `kafka_rebalance_total` 증가 없음
- [ ] `future.cancel(true)` 후에도 스레드가 누수되지 않는지 확인(`parse_thread_leaked` 메트릭, P1-2)

---

### P0-3. 재시도 예산 설계

**막는 장애**: 인라인 재시도가 `max.poll.interval.ms`를 넘겨 발생하는 리밸런스 폭풍.

**공식**: `처리시간 × 시도횟수 + 백오프 총합 + 마진 < max.poll.interval.ms`

**재시도 횟수·백오프는 이미 확정·구현되어 있다** — 초판 스펙의 "미결 사항"(지수 백오프 5s→40s)은 폐기됐다. 실제 구현(`IndexingFailureService.recordFailure()`, 5f52bd5)은 **선형** 백오프이고, `next_retry_at = failedAt + base_delay × attempt_count`다.

| 항목 | 값 | 근거 |
|---|---|---|
| 1회 처리 시간 상한 | 120초 | 다운로드 30s + 파싱 60s + 임베딩 30s (P0-2가 보장 — **미구현**, 아래 참고) |
| 인라인 재시도 횟수 | **5회** | `INDEXING_MAX_ATTEMPTS` 기본값 5 — 이미 구현·적용됨 |
| 백오프 | **선형** `base-delay(기본 30s) × attempt_count` → 30/60/90/120초, 합 300초 | `IndexingFailureService.recordFailure()` — 이미 구현·적용됨 |
| 처리 합계 | 120 × 5 = 600초 | |
| **총합** | **900초** | |
| `max.poll.interval.ms` | **900초로 상향 필요 — 실제 `application.yml`은 아직 600000(600초)** | 지금 이대로면 예산 초과로 리밸런스 위험 |
| 마진 | 0초(상향 전까지는 마이너스) | 상향이 선행되지 않으면 위험 |

```kotlin
// IndexingFailureService.recordFailure() — 실제 구현 형태
val multiplier = attemptCount.coerceAtLeast(1)
val nextRetryAt = failedAt.plus(baseDelay.multipliedBy(multiplier.toLong()))
```

**주의**: `max.poll.interval.ms`를 올려도 진짜 크래시 감지 속도는 그대로다 — 워커 생사 판정은 `session.timeout.ms`(45초, heartbeat 기반)가 별개로 담당한다.

**흡수 범위**: 인라인 5회로 총 675초(약 11분) 동안의 일시적 장애(순간 429, 커넥션 리셋, 임베딩 서버 재시작)를 자동으로 흡수한다. 그 이상 — 예를 들어 임베딩 서버가 20분 다운되는 경우 — 는 `FAILED`로 종결되고, `indexing_job WHERE status='FAILED'` 조회 + 기존 재인덱싱 API로 복구한다(개별/일괄 모두 이 조회 하나로 가능, 별도 DLQ 저장소 없음 — B-Gap-8).

**손잡이는 두 개이고 상충한다**: `max.poll.interval.ms`를 더 늘리면 재시도 여유는 늘지만 진짜 hang 감지가 그만큼 늦어진다(B-Gap-4). 재시도 횟수를 늘리면 흡수 범위는 넓어지지만 파티션 블로킹이 커진다(B-Gap-6). 지금 숫자는 이 둘의 균형점이고, P1-2의 `kafka_rebalance_total`·`indexing_job_duration_seconds` 실측을 보며 재조정한다.

**완료 조건**

- [ ] `max.poll.interval.ms` = 900000 적용(현재 600000) — 인프라팀에 배포 설정 반영 요청(§7.2), 이게 유일하게 남은 미구현 항목
- [ ] P0-2(리소스 가드) 완료로 "1회 처리 ≤120초" 전제가 실제로 보장됨
- [ ] 정상 예산 경계 테스트: 120초×5회(마지막 성공) → `COMPLETED`, `kafka_rebalance_total` 증가 없음(`max.poll.interval.ms` 900초 상향 후에만 유효한 테스트)
- [ ] 예산 초과 경계 테스트: 250초×5회 → `kafka_rebalance_total` 증가 확인(예산 계산이 맞다는 뜻이지, 실패해야 정상이라는 뜻이 아님 — `FAULT_TOLERANCE.md` §5.2)

---

### P0-4. 인라인 재시도 전환 — 재시도 폴러 제거 ✅ 완료 (`5f52bd5`, `d4ff1e6`)

**막는 장애**: 재시도 경로 크래시로 인한 영구 유실(B-Gap-1).

**실제 구현 — 초판 스펙과 형태가 다르다.** 초판은 리스너 안 `for` 루프로 재시도 횟수를 세는 설계였지만, 실제 구현은 ① Kafka 리스너가 **배치**로 레코드를 받아 `documentId`로 그룹핑하고, ② 그룹마다 `IndexingPipelineRunner.run()`을 한 번 호출하며, ③ 재시도는 `run()` 내부의 `while` 루프가 전담하고, ④ ack은 배치 전체(모든 그룹)가 끝난 뒤 한 번만 호출된다.

```kotlin
// IndexingKafkaListener.kt — 배치 리스너, documentId 그룹핑, 배치 전체 완료 후 단일 ack
@KafkaListener(topics = ["indexing"], id = "indexing")
fun onMessage(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment) {
    records
        .groupBy { it.key() }          // documentId
        .values
        .map { group -> executor.submit { group.forEach(::processRecord) } }
        .forEach { it.get() }
    ack.acknowledge()
}

// IndexingPipelineRunner.kt — 재시도 루프는 여기 안에서 완결
fun run(event: IndexingRequestedEvent) {
    val jobId = acquireJobId(event, documentVersionId) ?: return

    while (true) {
        val acquired = indexingJobRepository.start(jobId, workerId, maxAttempts)
        if (acquired != 1) {
            indexingJobRepository.failIfAttemptsExceeded(jobId, maxAttempts)
            return
        }
        try {
            processAcquiredJob(jobId, event, documentVersion)
            return                                              // 성공 → COMPLETED
        } catch (e: Exception) {
            val status = indexingFailureService.recordFailure(
                jobId, errorCode = errorCodeOf(e), permanent = !isRetryable(e),
                maxAttempts = maxAttempts, baseDelay = baseDelay, failedAt = LocalDateTime.now(),
            )
            if (status != IndexingJobStatus.RETRY_WAIT) return   // FAILED, 또는 다른 워커가 이미 COMPLETED
            val nextRetryAt = indexingJobRepository.findById(jobId).orElseThrow().nextRetryAt!!
            val waitMillis = Duration.between(LocalDateTime.now(), nextRetryAt).toMillis().coerceAtLeast(0)
            Thread.sleep(waitMillis)
            // 루프 재진입 — start()가 RETRY_WAIT을 PROCESSING으로 되돌리며 attempt_count를 다시 올린다
        }
    }
}
```

**제거 완료** (§5 참고)

| 대상 | 상태 |
|---|---|
| `IndexingRetryScheduler` | 제거됨. `DocumentDeletionSweepScheduler.kt` 주석에 "삭제됨"으로 명시 |
| `RetryEventSource` | 제거됨 |
| `findRetryWaitDue()` 쿼리 | 제거됨 |

**유지 대상**

- `RETRY_WAIT` 상태값 + `next_retry_at` — 관측 전용(P1-1 진행률 API에서 노출 예정). 이 값을 읽어 깨우는 별도 주체는 없다.
- `start()`의 재획득 조건에 `RETRY_WAIT` 포함 — 인라인 대기(`Thread.sleep`) 중 워커가 죽으면, 재전달된 메시지를 받은 다른 워커의 `run()`이 `start()`를 호출해 `RETRY_WAIT` 상태의 Job을 재획득한다. 이때도 같은 `attempt_count`를 그대로 증가시킨다(별도 카운터 없음).

**선행**: P0-1, P0-2, P0-3(원칙상 — 실제로는 P0-2/P0-3 완료 전에 이미 병합됐다. 즉 지금 시점에서 "1회 처리 ≤120초" 상한이 아직 코드로 보장되지 않은 채로 인라인 재시도가 돌고 있다는 뜻이므로, P0-2/P0-3을 서둘러 채워야 하는 이유이기도 하다).

**완료 조건 — 전부 충족됨**

- [x] `IndexingRetryScheduler`, `RetryEventSource`, `findRetryWaitDue()` 코드베이스에서 제거됨
- [x] 배치 리스너 + documentId 그룹핑 + 배치 전체 완료 후 단일 ack 구현됨
- [ ] (회귀 확인 필요) 인라인 백오프 sleep 중 `docker kill` → 리밸런스 → 재전달 → 다른 워커가 처음부터 재처리 → 완료, `indexing_job`에 `PROCESSING`으로 남는 행 없음(`FAULT_TOLERANCE.md` §5.3) — 코드는 있으나 이 시나리오의 실측 검증 여부는 별도 확인 필요
- [ ] (회귀 확인 필요) 임베딩 2회 실패 후 성공 시 `attempt_count = 3`, `COMPLETED`, 추가 Kafka 메시지 발행 없음(§5.1)

---

### P0-5. 컨슈머 게이트 + `nack` — ⚠️ 지금 가장 시급함, 선행 조건 없음

**막는 장애**: DB 다운 중 메시지 소각(B-Gap-10).

**우선순위 재조정 사유**: P0-4(인라인 재시도 전환)가 이미 완료되면서, 이 항목이 유일하게 선행 조건 없이 바로 착수 가능한 P0다. `processRecord()`는 아직 `DataAccessResourceFailureException`을 구분하지 않고 다른 예외와 동일하게 로그만 남기고 삼키므로(`IndexingKafkaListener.kt:39`, 항상 `ack.acknowledge()`), **지금도 DB 장애 시 데이터가 조용히 사라질 수 있는 미구현 상태**다.

**배치 리스너 기준 주의**: 리스너가 배치로 전환됐으므로(P0-4 완료) `nack`은 레코드 하나가 아니라 **배치 전체** 단위로 걸린다. 배치 안 한 레코드에서 DB 장애가 나면, 같은 배치의 다른 문서(정상 처리 가능했던 Job들)까지 통째로 되감기는 트레이드오프를 감수해야 한다.

**(a) DB 장애는 ack이 아니라 `nack`**

```kotlin
ack.nack(Duration.ofSeconds(5))
// ① 이번 poll의 남은 레코드를 버리고
// ② 실패한 레코드 위치로 파티션을 되감고
// ③ 5초 쉬었다 다시 폴링 → 그 메시지가 다시 온다
```

**(b) 컨테이너 pause로 hot loop 방지**

```kotlin
@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
) {
    @Scheduled(fixedDelay = 5_000)
    fun check() {
        val healthy = runCatching { jdbcTemplate.queryForObject("SELECT 1", Int::class.java) }.isSuccess
        val container = registry.getListenerContainer("indexing") ?: return
        when {
            !healthy && container.isRunning -> { container.pause(); log.warn("DB down — consumer paused") }
            healthy && container.isPauseRequested -> { container.resume(); log.info("DB up — consumer resumed") }
        }
    }
}
```

`pause()`는 폴링 자체를 멈추지 않으므로(heartbeat 계속) 리밸런스가 나지 않는다.

**(c) always-ack 원칙의 유일한 예외**: DB에 아무것도 기록하지 못한 실패는 ack하지 않고 `nack`한다.

**선행**: P0-4 — 완료됨. 지금 바로 착수 가능.

**완료 조건**

- [ ] `docker stop postgres` → 30초 후 `docker start postgres` → `nack` → pause → 복구 후 resume → 전부 처리, Kafka lag 원복, 유령 `PROCESSING` 행 없음(§5.5)
- [ ] `db_health_gate_paused_total` 메트릭이 pause/resume 전이마다 증가(P1-2)

---

### P1-1. 진행률 API (폴링 전용 — SSE는 채택하지 않음)

**막는 장애**: 사용자가 실패한 최신 버전을 인지하지 못함(B-Gap-7), `FAILED` 종결을 아무도 모름(B-Gap-8).

**스키마**: §2의 `phase` 컬럼 하나만 둔다 — `total_chunks`/`processed_chunks` 같은 청크 단위 진행률은 두지 않는다(2026-08-19 정리). `phase`는 `DOWNLOADING`/`PARSING`/`CHUNKING`/`EMBEDDING`/`PUBLISHING` 다섯 값만 갖는다.

**(정정) SSE는 두지 않는다.** 워커→클라이언트 직접 연결 구조가 아니고, 워커→API 서버로 상태를 실시간 전달하는 별도 경로도 없으므로 API 서버가 `indexing_job`을 폴링 조회하는 것과 실질적으로 다르지 않다 — 별도 스트리밍 인프라를 두는 비용만 추가된다. (초판의 `GET .../indexing/events` SSE 엔드포인트는 폐기)

**API**

| 엔드포인트 | 설명 |
|---|---|
| `GET .../indexing` | 폴링. 워커는 `phase` 컬럼을 채우는 것까지만 담당하고, 노출은 API 서버가 이 테이블을 폴링 조회해서 처리한다 |

**`phase` 갱신 시점 — 각 단계 "완료 후"가 아니라 "진입 직전"에 UPDATE한다.** 완료 후에 쓰면 그 단계를 처리하는 동안(가장 오래 걸리는 구간, 특히 임베딩)에는 직전 단계 값이 그대로 남아 사용자가 "멈췄다"고 오인한다.

| 단계 | 갱신 시점 | 갱신 주체 |
|---|---|---|
| `DOWNLOADING` | 다운로드 시작 직전(Job 획득 직후) | `IndexingPipelineRunner` |
| `PARSING` | 다운로드 완료 + 파싱 시작 직전 | `IndexingPipelineRunner` |
| `CHUNKING` | 파싱 완료 + 청킹 시작 직전 | `IndexingPipelineRunner` |
| `EMBEDDING` | 청킹 완료 + `IndexingProcessor.process()` 호출 직전 | `IndexingPipelineRunner` |
| `PUBLISHING` | 임베딩 완료 + UPSERT/fencing 비교 시작 직전 | **`IndexingProcessor`(태성) — §7.1 요청 사항, `process()` 내부라 워커 쪽 코드가 이 전이를 볼 수 없음** |

인라인 재시도(P0-4)로 같은 Job이 여러 번 `start()`를 타더라도, 재시도 진입 시 `phase`는 항상 `DOWNLOADING`부터 다시 시작한다 — 청킹 결정성과 마찬가지로 재시도는 처음부터 다시 처리하므로(§1.4-(1)), 직전 시도에서 도달했던 `phase` 값을 이어받지 않는다.

**동작 규칙**

- 버전 목록 응답에 `searchable: boolean` + `indexingStatus` 노출 — "최신 버전을 올렸지만 실패해서 이전 버전이 검색된다"는 상태를 보이게 함.
- `FAILED` 상태는 재인덱싱 버튼과 함께 노출.

**완료 조건**

- [ ] `IndexingPipelineRunner`가 `DOWNLOADING`/`PARSING`/`CHUNKING`/`EMBEDDING` 네 단계 진입 직전마다 `phase` UPDATE를 호출함
- [ ] `IndexingProcessor`가 `PUBLISHING` 진입 직전 `phase` UPDATE를 호출함(§7.1 요청, 태성 쪽 구현 필요)
- [ ] 인라인 재시도 진입 시 `phase`가 `DOWNLOADING`으로 리셋됨(직전 시도 값을 이어받지 않음)
- [ ] v3 파싱 실패 후 버전 목록 API가 `searchable: false`(v3), 검색은 여전히 v2 콘텐츠를 반환함을 확인
- [ ] API 서버의 폴링 조회가 `phase`를 정확히 반영함

---

### P1-2. 메트릭 + 알림

| 메트릭 | 타입 | 알림 기준 | 의미 |
|---|---|---|---|
| `indexing_job_failed_total{errorCode}` | Counter | 급증 시 | DLQ 없이 실패를 감지하는 핵심 지표. `WHERE status='FAILED'`와 대응 |
| `kafka_rebalance_total` | Counter | 즉시 | 재시도 예산이 poll 간격을 넘고 있음(P0-3 실패 신호) |
| `indexing_inline_retry_total{attempt}` | Counter | 급증 시 | 어느 시도에서 성공/실패하는지, 예산 튜닝 근거 |
| `indexing_job_duration_seconds{phase}` | Histogram | p99 감시 | 1회 처리 120초 가정 검증 |
| `kafka_consumer_lag{partition}` | Gauge | 지속 증가 | 파티션 hot spot, 배치 블로킹 |
| `db_health_gate_paused_total` | Counter | 즉시 | DB가 흔들리고 있음 |
| `parse_thread_leaked` | Gauge | 임계 초과 | P0-2 취소 실패 누적 |

**핵심 지표**: `kafka_rebalance_total`(오르면 예산 초과), `indexing_job_duration_seconds` p99(120초 초과 시 예산 가정 붕괴), `indexing_job_failed_total`(DLQ 대체 — 반드시 대시보드에 포함).

**완료 조건**

- [ ] 7개 메트릭 전부 계측됨
- [ ] `kafka_rebalance_total`, `db_health_gate_paused_total`에 즉시 알림 연결
- [ ] 대시보드에 `indexing_job_failed_total{errorCode}` 브레이크다운 포함

---

### P2-1. DB 페일오버 대응 (2차 과제)

```yaml
spring:
  datasource:
    hikari:
      connection-timeout: 5000
      validation-timeout: 3000
      keepalive-time: 30000
      max-lifetime: 600000
```

`DB_URL=jdbc:postgresql://node1:5432,node2:5432/...?targetServerType=primary`

```
페일오버 발생 → DataAccessResourceFailureException → nack(5초) → 컨슈머 pause
  → promote 완료 → 재연결 → resume → 되돌려둔 메시지부터 재처리
```

고아 회수가 필요 없음 — §1의 불변조건에 의해 대응하는 미커밋 메시지가 항상 존재.

**선행**: P0-4, P0-5(재활용) + 인프라 선행(OpenSQL 클러스터 구성).

**측정 지표**

| 지표 | 측정 방법 |
|---|---|
| 데이터 유실 건수 | 페일오버 중 업로드한 N건 중 최종 완료 수 → N/N |
| RTO | Primary kill → 첫 정상 완료 |
| 컨슈머 복구 시간 | pause → resume |

---

### P2-2. 시각 통일

**막는 장애**: `next_retry_at`(애플리케이션 시각) vs `start()` 비교(DB 시각) 불일치, `LocalDateTime`/`TIMESTAMPTZ` 혼재(B-Gap-5).

**작업 내용**

- `next_retry_at`을 UPDATE 문 안에서 `CURRENT_TIMESTAMP + :interval`로 계산(애플리케이션에서 계산해 넘기지 않음).
- `LocalDateTime` → `Instant`/`OffsetDateTime`으로 통일.
- 워커 JVM `TZ=UTC` 고정(인프라 요청 — §7.4).

**완료 조건**

- [ ] `next_retry_at` 계산이 DB 함수(`CURRENT_TIMESTAMP`) 기준으로 통일됨
- [ ] 코드베이스에서 `LocalDateTime` 사용처가 인덱싱 도메인에서 제거됨

---

### P2-3. 파티션 / 처리량 튜닝

**막는 장애**: `documentId` 키의 hot spot(B-Gap-6), 인라인 재시도가 이를 악화(최대 675초 동안 배치 커밋 불가).

| 문제 | 대응 |
|---|---|
| 재시도 중인 Job이 배치 커밋을 붙잡음 | `max-poll-records`를 작게 유지 |
| 대용량 문서가 파티션 점유 | 파일 크기별 토픽 분리 |
| 특정 테넌트 독점 | 테넌트별 동시 처리 상한 |

**선행**: P1-2(실측 없이 미리 튜닝하지 않는다).

---

### P2-5. 비동기 경계 트레이싱

- API 서버: `SET LOCAL app.trace_id = :traceId` (스펙 §6.3, API 담당 요청 필요)
- 워커: MDC 세팅 → `indexing_job.trace_id` 저장 → 모든 로그에 포함

인라인 재시도는 같은 스레드에서 돌아 MDC가 자동 유지된다(폴러 기반이었다면 스레드 경계마다 재설정이 필요했을 것).

**완료 조건**

- [ ] `trace_id`가 Kafka 이벤트 수신부터 최종 상태 기록까지 모든 로그 라인에 포함됨

---

## 5. 제거된 컴포넌트 ✅ 완료 (`5f52bd5`)

| 대상 | 위치(참고) | 대체 | 상태 |
|---|---|---|---|
| `IndexingRetryScheduler` | Track A plan Task 17 | P0-4 인라인 루프 | 제거됨 |
| `RetryEventSource` | Track A plan Task 17 | 원본 메시지 재사용(리스너가 이미 들고 있음) | 제거됨 |
| `findRetryWaitDue()` 쿼리 | — | 폴링 대상 없음 | 제거됨 |

---

## 6. 구현 순서 (의존성 순) — 2026-08-19 갱신, P0-4 완료 반영

| 순서 | 항목 | 선행 | 이유 |
|---|---|---|---|
| — | **P0-4 인라인 재시도 전환** | — | **완료**(`5f52bd5`, `d4ff1e6`) |
| 1 | **P0-5 컨슈머 게이트 + `nack`** | — | 선행 없음, 최우선 — 미구현 상태라 지금도 DB 장애 시 데이터가 조용히 사라질 수 있다 |
| 2 | P0-1 에러 분류 | — | 인라인 리스너의 분기 조건 정교화(`isRetryable()` 화이트리스트 확장 + 미결 사항 확정) |
| 3 | P0-2 리소스 가드 | 2 | "1회 처리 ≤120초" 상한 확보 |
| 4 | P0-3 재시도 예산 확정 | 2, 3 | `max.poll.interval.ms`를 900초로 상향 |
| 5 | P1-2 메트릭 | 1~4 | 지금 상태가 실제로 안전한지 확인할 유일한 수단 |
| 6 | P1-1 진행률 API | 스키마 1컬럼 | 인라인 재시도 가시성 + FAILED 노출 |
| 7 | P2-1 DB 페일오버 | 1 | 재활용 + 인프라 선행 |
| 8 | P2-2 시각 통일 | — | 미루면 재현 안 되는 버그로 돌아온다 |
| 9 | P2-5 트레이싱 | API 서버 `SET LOCAL` | 외부 의존 |
| 10 | P2-3 파티션 튜닝 | 5 | 실측 후에만 |

**P0-4가 이미 완료됐으므로 순서가 초판과 달라졌다.** 초판은 "에러 분류 → 리소스 가드 → 예산 확정 → 전환"의 선(先)조건 순서를 전제했지만, 실제로는 전환이 이 셋보다 먼저 병합됐다. 그 결과 **1회 처리 시간 상한이 코드로 보장되지 않은 채 인라인 재시도가 이미 운영되는 상태**이므로, P0-1·P0-2·P0-3을 뒤늦게라도 서둘러 채워야 한다는 점은 초판의 논리와 동일하게 유효하다. 그중 P0-5는 P0-4와 무관하게 독립적으로 착수 가능해 최우선으로 올렸다.

---

## 7. 다른 담당자에게 요청할 것

### 7.1 태성님 (`IndexingProcessor`)

- 임베딩 실패 예외를 `IndexingPipelineRunner.isRetryable()`의 화이트리스트에 등록 가능한 형태로 던지기(P0-1 — sealed class가 아니라 기존 예외 화이트리스트 방식으로 확정됨)
- **(정정)** 429의 `Retry-After` 값은 실어줄 필요 없음 — 임베딩 API가 애초에 이 헤더를 주지 않으므로, 429도 다른 재시도 가능 예외와 동일하게 선형 백오프로 처리한다(P0-1)
- 임베딩 호출에 타임아웃 30초 — P0-3 예산의 구성 요소
- 벡터 차원 불일치는 영구 실패(`isRetryable()` 화이트리스트에 `-> false`로 등록)
- `process()` 안에서 UPSERT/fencing 비교를 시작하기 직전에 `indexing_job.phase = 'PUBLISHING'`로 UPDATE(P1-1) — 이 전이는 `process()` 내부에서만 일어나 워커(`IndexingPipelineRunner`) 쪽 코드가 볼 수 없음

### 7.2 스키마 / API 서버 담당

| 대상 | 요청 |
|---|---|
| `indexing_job` | `phase` 추가(P1-1) |
| 트랜잭션 | `SET LOCAL app.trace_id`(P2-5) |
| 재처리 API | 게이트 조건을 `status = 'FAILED'`로(스펙 §6.3, 이미 확정된 사항 재확인) |
| `consumer.max-poll-interval-ms` | 600초 → 900초로 변경(P0-3) — 인프라/배포 설정 반영 필요 |

### 7.3 인프라 담당

- Kafka를 독립 배포 단위로(스펙 §6.5)
- OpenSQL 클러스터 + 페일오버 절차(P2-1)
- Toxiproxy를 개발 환경 compose에 포함
- 워커 컨테이너 `TZ=UTC` 고정(P2-2)

---

## 8. 테스트 계획

`FAULT_TOLERANCE.md` §5의 시나리오를 각 항목의 완료 조건(§4)에 이미 매핑해뒀다. 아래는 개별 항목 테스트로 커버되지 않는 **통합/회귀 테스트만** 별도로 정리한다.

### 8.1 (회귀) 워커 크래시는 Kafka가 회수한다

```bash
docker kill -s SIGKILL worker-1   # 리스너가 처리 중일 때
```
**기대**: 리밸런스 → 다른 워커가 재처리 → 완료. 청크 중복 0건. (Track A 성과 유지 확인용 회귀 테스트)

### 8.2 카오스 테스트 (최종 성적표)

```bash
while true; do
  sleep $((RANDOM % 60 + 30))
  docker kill -s SIGKILL "worker-$((RANDOM % 3 + 1))"
  sleep 10; docker compose up -d
done
```

30분 후:

```sql
-- ① 모든 Job이 최종 상태에 도달했는가
SELECT status, count(*) FROM indexing_job GROUP BY status;
-- ★ PROCESSING이 하나라도 남아 있으면 = §1 불변조건 위반

-- ② 각 문서의 검색 버전에 청크가 정상적으로 남아있는가(참고용 — total_chunks 컬럼을 두지 않기로
--   했으므로 "저장돼야 할 개수"와의 자동 비교는 불가능하다. COMPLETED인데 청크가 0건인 문서가
--   있는지만 육안으로 확인한다 — 있다면 조기 STALE 판정(§1.5)이 아닌 이상 버그다).
SELECT d.id, count(c.id) AS actual
FROM document d
JOIN document_version v ON v.id = d.searchable_version_id
JOIN indexing_job j ON j.document_version_id = v.id AND j.status = 'COMPLETED'
LEFT JOIN document_chunk c ON c.document_version_id = v.id
GROUP BY d.id;
```

**Definition of Done — Track B 전체**: ①이 "PROCESSING 0건"을 반환하고, §4 전 항목의 완료 조건 체크박스가 모두 닫혀야 한다.

---

## 부록. 이 스펙이 따르는 원칙 (`FAULT_TOLERANCE.md` 부록 원문 인용)

1. `PROCESSING`/`RETRY_WAIT`인 Job에는 항상 대응하는 미커밋 Kafka 메시지가 있다. ack 이후에 Job을 다시 활성 상태로 만드는 코드를 쓰지 않는다.
2. Kafka가 이미 해주는 일을 다시 만들지 않는다.
3. 문제를 막을 장치를 만들기 전에, 문제가 안 생기는 구조가 있는지 먼저 본다.
4. 재시도는 `max.poll.interval.ms` 예산 안에서만 한다.
5. 복구 가능하면 별도 저장소를 새로 만들지 않는다.
6. 기록하지 못한 실패는 ack하지 않고 `nack`한다.
7. Track A의 정합성 메커니즘을 대체하지 않는다.
8. 관측이 먼저, 튜닝은 나중.
9. 감지할 수 없으면 대비한 게 아니다.
