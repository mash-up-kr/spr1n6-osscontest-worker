# Track B 장애 대비 설계 — 수집 파이프라인 워커

> **담당**: 은지 (A — 수집 & 워커 확장성, `[임베딩 워커 - 그룹2]`)
> **작성일**: 2026-08-17
> **상태**: Track A 구현 완료 → Track B 착수 전 범위 확정용

---

## 0. 이 문서의 목적

Track A는 **"정합성이 깨지지 않는 파이프라인"**을 목표로 했고, 그 목표는 달성했다. Track B는 정합성을 *다시 만드는* 작업이 아니라, Track A가 의도적으로 미뤄둔 것과 **구현하고 나서야 드러난 구멍**을 메우는 작업이다.

1. **§1 — Track A로 막아진 장애**: 이미 해결된 것.
2. **§2 — Track A가 남긴 구멍**: 미룬 것 + 구현하고 발견한 것.
3. **§3~§6 — Track B 항목별 대응 방법**: 무엇으로 어떻게 막는지, 우선순위와 검증 방법.

---

## 1. Track A 구현으로 막아진 장애

### 1.1 요약표

| # | 장애 | 막은 장치 | 근거 |
|---|---|---|---|
| A-1 | 워커 프로세스 크래시(SIGKILL, OOM 킬) | **ack이 처리 완료 후이므로** offset 미커밋 → Kafka가 파티션을 재할당해 자동 재전달 + `status IN ('PENDING','PROCESSING')` 재획득 | 스펙 §1.3 문제1, §1.4-(1) |
| A-2 | 리밸런스 중 같은 이벤트 중복 소비 | `source_event_id UNIQUE` + Kafka topic/partition/offset 비교 + `uq_indexing_job_active_version` + `UNIQUE(document_version_id, chunk_no)` UPSERT | 스펙 §1.1, 현재 구현 문서 §5.1 |
| A-3 | 두 워커가 동시에 같은 Job 소유 | 배제하지 않고 수렴 — 청킹 결정성(§3.6) + UPSERT | 스펙 §1.2, 부록 원칙 4 |
| A-4 | poison pill 크래시 루프 | 재획득 쿼리의 `attempt_count < :maxAttempts` 캡 → `FAILED('MAX_ATTEMPTS_EXCEEDED')` | 스펙 §1.4-(1), plan Task 3 |
| A-5 | 실패 시 ack 보류로 인한 hot loop | Job이 최종 상태(success/failed)에 도달하거나 영구적인 잘못된 이벤트로 판정된 뒤 ack | 스펙 §3.1 |
| A-6 | 잘못된 이벤트의 무한 재시도 | `IndexingEventValidator` → `InvalidEventException` 즉시 종결 + ack | 스펙 §0.3, plan Task 5 |
| A-7 | 역직렬화 실패 메시지가 파티션을 막는 것 | `DeserializationException` catch → 로그 + ack | plan Task 11 |
| A-8 | 오래된 업로드가 최신 업로드를 덮어씀 | `document_version.embedding_version_no` fencing 비교 | 스펙 §1.3 문제2, §1.4-(3) |
| A-9 | 느린 이전 버전이 최신 검색 결과를 덮어씀 | 이전 버전도 청크는 보관하되 검색 포인터 승격만 `embedding_version_no`로 차단 | 스펙 §3.1 |
| A-10 | 실패 쓰기가 성공 결과를 덮어씀 | 실패 쓰기에만 `status = 'PROCESSING'` 가드 | 스펙 §1.4-(4) |
| A-11 | 재청킹으로 청크 수가 줄었을 때 잔존 청크 | 현재 구현은 trailing DELETE(`chunk_no > :lastChunkNo`)를 수행한다. 같은 코드·설정이면 청킹이 결정적이지만 배포 버전, 전략, parser/tokenizer 또는 과거 데이터가 다를 때를 방어한다 | 현재 구현 문서 §5.9 |
| A-12 | 스캔 PDF가 "성공"으로 위장 | `ChunkGuard.assertValid()` — 빈 청크 → `EMPTY_EXTRACTION` | 스펙 §3.6, plan Task 9 |
| A-13 | 대용량 문서 청크 폭발 | `max-chunks-per-document: 5000` 상한 | 스펙 §3.6 |
| A-14 | 손상/변조된 원문 인덱싱 | 다운로드 후 SHA-256 vs `content_hash` 비교 | 스펙 §3.3 |
| A-15 | Storage 무응답으로 워커 스레드 영구 잠김 | 다운로드 타임아웃 30초 | 스펙 §3.3 |
| A-16 | 비즈니스 예외 후 재시도 경로 없음 | `RETRY_WAIT` + `IndexingPipelineRunner.run()` 안 인라인 재시도 루프(`5f52bd5`) | 스펙 §1.4-(4), §3.8 |
| A-17 | 여러 폴러 인스턴스의 중복 재시도 | 해당 없음 — 폴러 자체가 없다(`5f52bd5`) | 스펙 §3.8 |
| A-18 | 삭제된 문서의 유령 청크가 검색에 노출 | `DOCUMENT_DELETED` 처리 + `document_chunk` 삭제 | 스펙 §3.9 |
| A-19 | 삭제 이벤트 유실 시 청크 잔존 | 정리 스윕 스케줄러 | 스펙 §3.9 |
| A-20 | 삭제된 문서를 재시도 폴러가 되살림 | `failActiveJobsForDocument()` | 스펙 §3.9, plan Task 19 |
| A-21 | 삭제 후 뒤늦게 끝난 Job이 검색 버전을 되살림 | 버전 전환 UPDATE에 `d.deleted_at IS NULL` 가드 | 스펙 §3.9, §6.1 |
| A-22 | 업로드↔삭제 순서 역전 | 같은 토픽·같은 파티션(`documentId` 키) | 스펙 §0.3 |

### 1.2 핵심 두 가지

**① ack은 처리가 전부 끝난 뒤에만 한다 — 워커 크래시 복구의 전부.**

```
워커가 처리 도중 죽는다
  → offset 미커밋 → heartbeat 끊김 → Kafka가 파티션 재할당(리밸런스)
  → 넘겨받은 워커가 committed offset부터 다시 읽음
  → start()가 status IN ('PENDING','PROCESSING')로 재획득 → 처음부터 재처리
```

여기에 추가 장치는 필요 없다. 대가는 **중복**(at-least-once) — 청킹 결정성 + UPSERT로 무해하게 만든다.

**② 재전달의 방아쇠는 ack 누락이 아니라 파티션 재배정이다.**

Kafka 브로커는 파티션당 offset 숫자 하나만 기억한다. "ack이 안 왔다"를 감지할 방법이 없고, 메시지가 다시 오는 유일한 순간은 컨슈머가 파티션을 새로 배정받아 "어디부터 읽죠?"를 물을 때뿐이다 — 즉 **리밸런스가 나야만** 재전달이 일어난다.

```
재전달 = ① 방아쇠(리밸런스, 워커가 죽어야 당겨짐)  +  ② 되감을 거리(커밋 안 된 offset)
```

**워커가 살아있는 채로 로직만 실패하면 ①이 없다.** heartbeat가 계속 가므로 Kafka는 개입하지 않는다. 이게 §1.3의 출발점이다.

**③ 상호 배제를 포기하고 수렴을 택했다.** 두 워커가 같은 Job을 동시에 만져도 청킹 결정성 + UPSERT로 결과가 수렴한다.

### 1.3 Track A가 택한 재시도 구조와, 그것이 만든 문제

워커가 살아있는 실패는 Kafka가 모르고, Kafka에는 지연 재전달 기능이 없다. Track A는 이렇게 풀었다.

```
파싱/임베딩 실패 (워커 생존)
  → RETRY_WAIT + next_retry_at 기록 → ack ★ 여기서 Kafka와의 관계가 끝난다
  → 30초 뒤 폴러가 집어 재처리
```

**★ 지점 이후로 그 Job에 대응하는 Kafka 메시지가 사라진다.** 폴러가 집어 `PROCESSING`으로 바꾼 직후 워커가 죽으면 되감을 메시지가 없고, 폴러는 `RETRY_WAIT`만 조회하니 아무도 회수하지 못한다.

**구멍의 원인은 "재시도를 ack 이후로 미룬 것" 하나다.** Track A는 이후 `5f52bd5` 커밋에서 폴러(`IndexingRetryScheduler`/`RetryEventSource`/`findRetryWaitDue()`)를 제거하고 인라인 재시도(`IndexingPipelineRunner.run()` 안 루프)로 전환해서 이 구멍을 자체적으로 없앴다. 아래 §3 P0-4는 그 전환의 설계 근거다.

---

## 2. Track A가 남긴 구멍

### 2.1 의도적으로 미룬 것 (스펙 §4에 이미 명시)

| 구멍 | 현재 상태 | 왜 문제인가 |
|---|---|---|
| 에러 분류 없음 | 원인 불문 상한까지 재시도 | 영구 실패에 재시도 낭비, 반대로 일시 실패는 재시도 부족 가능 |
| 선형 백오프만 | `next_retry_at = now() + 30s × attempt_count` | 임베딩 서버 복구 시 실패한 Job들이 동시에 재시도(thundering herd) |
| Outbox 보정 없음 | `PUBLISHED`인데 Job이 없는 경우를 감지 못함 | 브로커 재시작만으로도 발생. 유실이 조용히 일어남 |
| 진행률 미노출 | `phase`, `total_chunks`, `processed_chunks` 컬럼 없음 | 처리 중인지 멈춘 건지 구분 못 함 |
| 트레이싱 없음 | `trace_id`는 컬럼에만 있고 전파 안 됨 | 관측 공백 |
| 메트릭 없음 | 카운터/게이지 전무 | **감지할 수 없으면 대비한 게 아니다** |
| DB HA 대응 없음 | 페일오버 중 워커 동작 미정의 | 2차 과제 요구사항 |

### 2.2 구현하고 나서 드러난 것

**B-Gap-1. `PROCESSING` 고아 — 개정 2로 소멸.**

§1.3의 문제다. 재시도를 ack 이전으로 당기면(§3 P0-4) 문제 자체가 발생하지 않는다. 전환 후 성립하는 불변조건:

> **`indexing_job`이 `PROCESSING` 또는 `RETRY_WAIT`인 동안에는, 그에 대응하는 커밋되지 않은 Kafka 메시지가 반드시 존재한다.**

이게 지켜지면 어느 시점에 죽든 리밸런스가 회수한다. **"ack 이후에 Job 상태를 다시 `PROCESSING`으로 만드는 코드를 쓰지 않는다"**를 부록 원칙에 못 박는다.

---

**B-Gap-2. `uq_indexing_job_active_version` 위반 시 이벤트는 조용히 종결된다 — 의도된 요청 병합(coalescing)이다.**

```kotlin
insertIfAbsent(...)                          // ON CONFLICT DO NOTHING
val job = findBySourceEventId(event.eventId)
if (job == null) {
    log.info("... another active job already targets ...")
    return null                              // ← 그리고 리스너가 ack한다
}
```

같은 `document_version_id`를 가리키는 활성 Job이 이미 있으면 이 이벤트는 로그 한 줄 남기고 종결된다. 이건 유실이 아니다 — 버려지는 이벤트가 요청하는 작업(이 `document_version_id`를 인덱싱하는 것)은 이미 활성 상태인 Job이 수행 중이고, 그 Job은 §1 불변조건에 의해 결국 `COMPLETED`/`FAILED` 중 하나로 수렴한다. 즉 버려지는 이벤트가 대표하는 작업은 이미 진행 중인 Job이 대신 끝까지 책임진다. 앞선 Job이 이미 `FAILED`로 끝난 뒤에 도착한 이벤트라면 `uq_indexing_job_active_version`(활성 상태에만 걸리는 부분 인덱스)에 걸리지 않으므로 정상적으로 새 Job이 만들어진다.

**대응**: 없음 — 별도 보정 장치가 필요하지 않다.

---

**B-Gap-3. 파일 전체를 메모리에 올린다 — OOM 경로가 세 겹.**

```kotlin
downloadClient.download(objectKey)            // ByteArray 전체
  → Loader.loadPDF(input.readAllBytes())      // 또 전체 복사
  → parser.parse(...).toList()                // Sequence의 스트리밍 이점을 여기서 폐기
  → chunkingService.chunk(blocks, ...)        // encode된 토큰 리스트 전체
```

파일 크기 상한도 없다. → P0-2

---

**B-Gap-4. 파싱에 타임아웃이 없다.** ★인라인 재시도 전환으로 더 중요해짐

악성 PDF나 hwplib 무한 루프에 걸리면 `poll()`을 못 불러 `max.poll.interval.ms`를 넘기고, 리밸런스로 넘겨받은 다른 워커가 똑같이 걸린다. **인라인 재시도 예산(P0-3) 계산이 "1회 처리 시간 상한"을 전제로 하므로, 이 항목이 그 전제 조건이다.** → P0-2

---

**B-Gap-5. 시각의 출처가 섞여 있다.**

`recordFailure()`의 `nextRetryAt`은 애플리케이션 시각, `start()`의 비교는 DB 시각. `LocalDateTime`과 `TIMESTAMPTZ`도 섞여 있다. 인라인 전환으로 왕복 자체가 줄어 심각도는 낮아졌지만, 관측용으로 `next_retry_at`을 계속 남긴다면 정리는 필요하다. → P2-2

---

**B-Gap-6. 파티션 hot spot — `documentId` 키의 대가.** ★인라인 재시도 전환으로 더 중요해짐

같은 문서를 반복 업데이트하는 사용자가 파티션을 독점한다. **인라인 재시도가 이걸 악화시킨다** — 재시도하는 최대 675초(§3 P0-3) 동안 그 배치가 커밋을 못 하고, 파티션 뒤의 다른 문서가 대기한다. → P2-3

---

**B-Gap-7. 최신 버전이 실패하면 사용자는 옛 버전을 보면서 그 사실을 모른다.**

```
v2 완료 → searchable = v2
v3 업로드 → 파싱 실패 → FAILED
→ 사용자는 v3을 올렸는데 검색은 v2 내용을 반환. Job 상태를 안 보면 모른다
```

"검증 후에만 노출한다"(부록 원칙) 관점에서 의도된 동작이지만, 알리는 채널이 없다. → P1-1

---

**B-Gap-8. `FAILED` 종결 후 사용자/운영자가 모른다.**

인라인 재시도 5회를 소진하면 `indexing_job`에 `status=FAILED`와 에러 코드가 남고 재인덱싱 API로 복구 가능하지만, **알림이 없으면 아무도 그 사실을 모른다.** 별도 저장소(DLQ)를 두는 대신, 이 문제는 노출(P1-1 진행률/상태 API)과 관측(P1-2 메트릭: `indexing_job_failed_total{errorCode}`)으로 해결한다 — `indexing_job WHERE status='FAILED'`를 그대로 조회 대상으로 삼는다.

---

**B-Gap-9. 삭제 스윕이 무한 재실행될 수 있다.**

chunk 삭제가 계속 실패하면 매 60초마다 같은 문서를 영원히 재시도한다. → P2-4

---

**B-Gap-10. 컨슈머가 DB 상태를 신경 쓰지 않는다.**

DB가 내려간 상태에서도 리스너는 계속 소비하고 매번 ack한다. `insertIfAbsent`조차 실패했으니 Job 행도 없고 실패 기록도 없다. → P0-5

---

## 3. Track B — 장애별 대응 방법

우선순위는 **"막지 않으면 데이터가 조용히 사라지는가"**를 첫 기준, **"장애를 감지할 수 있는가"**를 두 번째 기준으로 잡았다.

---

### P0-1. 에러 분류 (Permanent / Transient)

**막는 장애**: 영구 실패에 재시도 낭비

인라인 구조에서는 이게 **리스너의 분기 조건 자체**다. `PermanentIndexingException`이면 재시도 없이 즉시 종결, 아니면 인라인 재시도(P0-4)를 탄다.

`FAILED` 종결 시 원인을 설명하는 필드는 `indexing_job.last_error_code`/`last_error_message`로 이미 있다 — Track A 구현 시점부터 `IndexingJobEntity`에 존재하고 `IndexingFailureService.recordFailure()`가 채운다. 아래 에러 코드 매핑표가 이 컬럼에 들어가는 값이다.

```kotlin
sealed class IndexingException(val code: String, message: String, cause: Throwable? = null)
    : RuntimeException(message, cause) { abstract val permanent: Boolean }

class PermanentIndexingException(...) : IndexingException(...) { override val permanent = true }
class TransientIndexingException(...) : IndexingException(...) { override val permanent = false }
```

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
| Transient | `EMBEDDING_RATE_LIMIT`(429) / `EMBEDDING_SERVER_ERROR`(5xx) | 임베딩 |
| Transient | `PARSE_TIMEOUT` | P0-2 |
| (별도) | `DB_CONNECTION_LOST` | → `nack`(P0-5). 재시도 예산을 쓰지 않는다 |

임베딩 API는 429 응답에 `Retry-After`를 실어주지 않는다. 그래서 `Retry-After` 기반 조기종료 로직은 두지 않는다 — 429도 다른 `TransientIndexingException`과 동일하게 기존 선형 백오프로 재시도한다.

실제 구현(`IndexingPipelineRunner.isRetryable()`)은 위 `PermanentIndexingException`/`TransientIndexingException` sealed 클래스 대신, 기존 예외 타입(`InvalidEventException`, `ContentIntegrityException`, `EmptyExtractionException` 등)을 화이트리스트로 `when`에 나열하는 방식으로 분류한다. sealed 클래스로 마이그레이션할지, 화이트리스트에 새 예외만 계속 추가할지는 착수 전 결정 필요.

---

### P0-2. 리소스 가드 — 파일 크기 상한 + 파싱 타임아웃 + 스트리밍 (B-Gap-3, B-Gap-4)

**막는 장애**: OOM으로 인한 fleet 붕괴, 재시도 예산 붕괴

P0-3의 예산 계산은 "1회 처리 시간 ≤ 120초"를 전제로 한다. 이 상한이 없으면 예산 전체가 무의미하다.

**(a) 파일 크기 상한 — 다운로드 전 판정**

```kotlin
if (documentVersion.fileSize > properties.maxFileSizeBytes) {
    throw PermanentIndexingException("FILE_TOO_LARGE", "...")
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
        throw TransientIndexingException("PARSE_TIMEOUT", "...")
    }
}
```

```yaml
indexing:
  limits:
    max-file-size-bytes: 209715200      # 200MB
    parse-timeout: PT60S
    download-timeout: PT30S
    embedding-timeout: PT30S            # 협업
```

세 타임아웃 합(120초)이 P0-3의 "1회 처리 시간" 상한과 일치해야 한다.

**중요**: 타임아웃 처리는 워커를 살려두므로 `TransientIndexingException`으로 인라인 재시도를 탄다. hang을 방치해 리밸런스가 나는 것과 다르다.

**(c) 스트리밍 복원**

`Sequence`를 청커까지 흘리고(`.toList()` 제거), 다운로드는 임시 파일로 스풀, 해시는 `DigestInputStream`으로 스트리밍 계산. 임시 파일은 `finally`에서 삭제.

---

### P0-3. 재시도 예산 설계 — `max.poll.interval.ms` 안에 넣기

**막는 장애**: 인라인 재시도가 poll 간격을 넘겨 발생하는 리밸런스 폭풍

배치/병렬 처리를 도입한 이유는 ACK 시간을 줄이기 위해서가 아니라, 한 번에 하나씩 순차 처리하는 구조에서 한 Job이 재시도에 들어가면 그 재시도가 끝날 때까지 배치 안 다른 문서의 Job들이 대기하는 문제를 완화하기 위해서다(`documentId`별 병렬 처리, `d4ff1e6`).

**단, "배치를 병렬로 돌리면 예산 문제가 풀린다"는 오해는 정리해야 한다.** 병렬화는 서로 다른 Job들이 동시에 진행되게 할 뿐, **Job 하나가 혼자 쓰는 시간**은 못 줄인다. 그리고 리밸런스를 유발하는 건 후자다.

```
처리시간 × 시도횟수 + 백오프 총합 + 마진 < max.poll.interval.ms
```

**재시도 횟수는 5회로 고정한다.** 백오프는 **선형**(`base-delay × attempt_count`)이다 — `IndexingFailureService.recordFailure()`에 구현돼 있다. 지수 백오프는 채택하지 않고, 필요성이 실측으로 확인되면 그때 "추후 고도화" 항목으로 검토한다.

| 항목 | 값 | 근거 |
|---|---|---|
| 1회 처리 시간 상한 | 120초 | 다운로드 30s + 파싱 60s + 임베딩 30s (P0-2가 보장, 미구현) |
| 인라인 재시도 횟수 | **5회** | Track A와 동일(`INDEXING_MAX_ATTEMPTS` 기본값 5) |
| 백오프 | **선형** `base-delay(기본 30s) × attempt_count` → 30/60/90/120초, 합 300초 | `IndexingFailureService.recordFailure()` |
| 처리 합계 | 120 × 5 = 600초 | |
| **총합** | **900초** | |
| `max.poll.interval.ms` | **900초로 상향 필요 — 현재 코드는 아직 600000(600초)** | 지금 이대로면 예산 초과로 리밸런스 위험 |
| 마진 | 0초 (상향 전까지는 마이너스) | 상향이 선행되지 않으면 위험 |

```yaml
indexing:
  retry:
    max-attempts: 5              # 이미 구현됨(INDEXING_MAX_ATTEMPTS, 기본값 5)
    base-delay: PT30S            # 이미 구현됨(INDEXING_RETRY_BASE_DELAY, 선형 백오프 기준 간격)
  consumer:
    max-poll-interval-ms: 900000 # 미반영 — 아직 600000(600초)
```

> 위 표의 "총합 900초"는 선형 백오프(합 300초) + 처리 5회(600초) 기준 재계산치다. `max.poll.interval.ms`를 900초로 올리지 않으면 마진이 없다 — P0-2(리소스 가드)로 1회 처리 시간이 실제로 120초 안에 들어오는지 보장되기 전까지는 이 계산 자체도 낙관적 상한이라는 점에 유의.

**`max.poll.interval.ms`를 올려도 진짜 크래시 감지 속도는 그대로다.** 워커 생사 판정은 `session.timeout.ms`(45초, heartbeat 기반)가 담당하는 별개 축이라, poll 간격을 늘리는 건 "느리지만 살아있는 컨슈머"를 더 오래 봐주는 것뿐이다.

**흡수 범위**: 인라인 5회로 총 675초(약 11분) 동안의 일시적 장애(순간 429, 커넥션 리셋, 임베딩 서버 재시작)를 자동으로 흡수한다. 그 이상 — 예를 들어 임베딩 서버가 20분 다운되는 경우 — 는 `FAILED`로 종결되고, `indexing_job WHERE status='FAILED'`로 조회해 기존 재인덱싱 API로 복구한다(개별/일괄 모두 이 조회 하나로 가능하며, 별도 저장소는 두지 않는다 — §2.2 B-Gap-8).

**손잡이는 두 개이고 상충한다**: `max.poll.interval.ms`를 더 늘리면 재시도 여유는 늘지만 진짜 hang 감지가 그만큼 늦어진다(B-Gap-4). 재시도 횟수를 늘리면 흡수 범위는 넓어지지만 파티션 블로킹이 커진다(B-Gap-6). 지금 숫자는 이 둘의 균형점이고, P1-2의 `kafka_rebalance_total`·`indexing_job_duration_seconds` 실측을 보며 재조정한다.

---

### P0-4. 인라인 재시도 전환 — 재시도 폴러 제거 (B-Gap-1)

**막는 장애**: 재시도 경로 크래시로 인한 영구 유실

재시도를 ack 이전으로 옮긴다. Job이 최종 상태에 도달할 때까지 대응하는 Kafka 메시지가 계속 미커밋 상태로 남으므로, 어느 시점에 죽든 리밸런스가 회수한다.

Kafka 리스너는 배치로 레코드를 받아 `documentId`로 그룹핑한 뒤, 그룹마다 `IndexingPipelineRunner.run()`을 한 번 호출한다. 재시도는 이 `run()` 안의 루프가 전담하고, ack은 배치 전체(모든 그룹)가 끝난 뒤 한 번만 호출된다:

```kotlin
fun run(event: IndexingEvent, recordIdentity: KafkaRecordIdentity) {
    val jobId = acquireJobId(event, documentVersionId, recordIdentity) ?: return

    while (true) {
        val acquired = indexingJobRepository.start(jobId, workerId, maxAttempts)  // PROCESSING 전환 + attempt_count += 1
        if (acquired != 1) {
            indexingJobRepository.failIfAttemptsExceeded(jobId, maxAttempts)
            return
        }
        try {
            processAcquiredJob(jobId, event, documentVersion)   // 검증→다운로드→파싱→청킹→임베딩
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

`next_retry_at`은 `attempt_count` 기반 **선형** 백오프로 계산된다(`IndexingFailureService.recordFailure()`):

```
next_retry_at = failedAt + base_delay × attempt_count
```

예를 들어 `base_delay = 30s`이고 이번 실패로 `attempt_count`가 3이 됐다면 `next_retry_at = failedAt + 90s`다. `Thread.sleep`은 그 시각까지 남은 시간만큼만 대기하고, 깨어나면 같은 루프 안에서 `start()`를 다시 호출해 `attempt_count`를 4로 올리며 재시도한다 — 별도 폴러나 스케줄러 없이 이 스레드 하나가 대기와 재시도를 모두 담당한다.

**제거된 것**: `IndexingRetryScheduler`, `RetryEventSource`, `findRetryWaitDue()` 쿼리 — 재시도가 리스너 호출 스레드 안에서 끝나므로 존재하지 않는다.

**`RETRY_WAIT`/`next_retry_at`의 역할**: 단순 관측용이 아니라, 위 루프가 대기 시간을 계산하는 데 직접 쓰인다. 추가로 인라인 대기(`Thread.sleep`) 중 워커가 죽으면, 재전달된 메시지를 받은 다른 워커의 `run()`이 `start()`를 호출해 `RETRY_WAIT` 상태의 Job을 재획득한다 — 이때도 같은 `attempt_count`를 그대로 증가시킨다(별도 카운터 없음).

---

### P0-5. 컨슈머 게이트 + `nack` (B-Gap-10)

**막는 장애**: DB 다운 중 메시지 소각

**(a) DB 장애는 ack이 아니라 `nack`이다**

```kotlin
ack.nack(Duration.ofSeconds(5))
// ① 이번 poll의 남은 레코드를 버리고
// ② 실패한 레코드 위치로 파티션을 되감고
// ③ 5초 쉬었다 다시 폴링 → 그 메시지가 다시 온다
```

`acknowledge()`와 `nack()`이 같은 인터페이스에 따로 있다는 사실 자체가 "ack 생략 ≠ 재전달"의 증거다 — ack 안 하는 것만으로는 컨슈머의 읽기 위치가 되감기지 않는다.

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

**(c) always-ack 원칙의 예외**: DB에 아무것도 기록하지 못한 실패는 ack하지 않고 `nack`한다.

**배치 리스너 기준 주의**: 리스너가 배치로 전환되면서(§0) `nack`은 레코드 하나가 아니라 **배치 전체** 단위로 걸린다. 배치 안 한 레코드에서 DB 장애가 나면, 같은 배치의 다른 문서(정상 처리 가능했던 Job들)까지 통째로 되감긴다는 트레이드오프를 감수한다. `processRecord()`는 아직 `DataAccessResourceFailureException`을 구분하지 않고 다른 예외와 동일하게 로그만 남기고 삼키므로, 이 절 전체가 미구현 상태다 — 선행 조건이 없어 바로 착수 가능한 최우선 작업이다.

---

### P1-1. 진행률 API (B-Gap-7, B-Gap-8)

```sql
ALTER TABLE indexing_job ADD COLUMN phase            VARCHAR(30);   -- DOWNLOADING/PARSING/CHUNKING/EMBEDDING/PUBLISHING
ALTER TABLE indexing_job ADD COLUMN total_chunks     INTEGER;
ALTER TABLE indexing_job ADD COLUMN processed_chunks INTEGER;
```

인라인 재시도가 리스너 안에서 도는 동안 외부에는 아무것도 안 보이므로, `phase`와 재시도 상태를 노출하지 않으면 사용자는 멈춘 것으로 인식한다.

워커는 위 컬럼을 채우는 것까지만 담당한다. 노출은 API 서버가 `indexing_job`을 폴링 조회해서 처리한다 — SSE는 두지 않는다(워커→클라이언트 직접 연결 구조가 아니고, 워커→API 서버로 상태를 실시간 전달하는 경로도 없으므로 API 서버가 폴링하는 것과 실질적으로 다르지 않다).

버전 목록 응답에 `searchable: boolean` + `indexingStatus`를 노출해 "최신 버전을 올렸지만 실패해서 이전 버전이 검색된다"는 상태를 보이게 한다. `FAILED` 상태는 재인덱싱 버튼과 함께 노출한다.

---

### P1-2. 메트릭 + 알림

| 메트릭 | 타입 | 알림 | 무엇을 알려주나 |
|---|---|---|---|
| `indexing_job_failed_total{errorCode}` | Counter | 급증 시 | **DLQ 없이 실패를 감지하는 핵심 지표.** `WHERE status='FAILED'`와 대응 |
| `kafka_rebalance_total` | Counter | **즉시** | 재시도 예산이 poll 간격을 넘고 있다(P0-3 실패 신호) |
| `indexing_inline_retry_total{attempt}` | Counter | 급증 시 | 어느 시도에서 성공/실패하나. 예산 튜닝 근거 |
| `indexing_job_duration_seconds{phase}` | Histogram | p99 감시 | 1회 처리 120초 가정이 유효한지 검증 |
| `outbox_reconciled_total` | Counter | **즉시** | 릴레이 서버의 Kafka 발행 실패로 인한 유실 감지 |
| `kafka_consumer_lag{partition}` | Gauge | 지속 증가 | 파티션 hot spot, 배치 블로킹 |
| `db_health_gate_paused_total` | Counter | **즉시** | DB가 흔들리고 있다 |
| `parse_thread_leaked` | Gauge | 임계 초과 | P0-2의 취소 실패 누적 |

**`kafka_rebalance_total`과 `indexing_job_duration_seconds` p99가 이번 설계의 핵심 지표다.** 전자가 오르면 예산을 넘긴 것이고, 후자가 120초를 넘으면 예산 가정이 깨진 것이다. `indexing_job_failed_total`은 DLQ를 대신하는 눈이므로 반드시 대시보드에 둔다.

---

### P1-3. Outbox 보정 배치

**막는 장애**: 릴레이 서버가 Kafka 발행 자체에 실패해서, outbox엔 `PUBLISHED`로 기록됐는데 실제로는 메시지가 나가지 않은 경우. 이 케이스는 `doc-relay` 담당(@Junhyukkkk) 확인 필요 — 릴레이 서버가 실제로 이런 실패 경로를 갖고 있는지부터 확인한다.

```sql
SELECT e.* FROM outbox_event e
LEFT JOIN indexing_job j ON j.source_event_id = e.id
WHERE e.status = 'PUBLISHED' AND j.id IS NULL
  AND e.published_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
ORDER BY e.published_at LIMIT 100 FOR UPDATE SKIP LOCKED;
```

```sql
CREATE INDEX idx_outbox_published ON outbox_event (published_at) WHERE status = 'PUBLISHED';
```

**경계 주의**: `outbox_event`는 `doc-relay` 소유다(스펙 §0.2). **`doc-relay` 담당과 사전 합의 필수**이고, 경계상 그쪽에 두는 게 깨끗하므로 먼저 제안한다.

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

고아 회수가 필요 없다 — 그 Job에 대응하는 미커밋 메시지가 항상 있다(§2.2 불변조건).

| 지표 | 측정 방법 |
|---|---|
| 데이터 유실 건수 | 페일오버 중 업로드한 N건 중 최종 완료 수 → N/N |
| RTO | Primary kill → 첫 정상 완료 |
| 컨슈머 복구 시간 | pause → resume |

---

### P2-2. 시각 통일 (B-Gap-5)

`next_retry_at`을 UPDATE 안에서 `CURRENT_TIMESTAMP + :interval`로 계산, `LocalDateTime` → `Instant`/`OffsetDateTime`, 워커 JVM `TZ=UTC` 고정.

---

### P2-3. 파티션 / 처리량 튜닝 (B-Gap-6)

| 문제 | 대응 |
|---|---|
| 재시도 중인 Job이 배치 커밋을 붙잡음 | `max-poll-records`를 작게 유지 |
| 대용량 문서가 파티션 점유 | 파일 크기별 토픽 분리 |
| 특정 테넌트 독점 | 테넌트별 동시 처리 상한 |

실측(P1-2) 없이 미리 튜닝하지 않는다.

---

### P2-4. 삭제 스윕 무한 재시도 방어 (B-Gap-9)

```sql
ALTER TABLE document ADD COLUMN chunk_purge_failed_count INTEGER NOT NULL DEFAULT 0;
```

실패 시 카운터 증가, 스윕 쿼리에 상한 조건 추가.

> 삭제 스윕 자체는 이번 구조 변경과 무관하게 유지한다 — 재시도 폴러와 달리 이벤트 유실 시 유령 청크를 막는 별도 목적이다.

---

### P2-5. 비동기 경계 트레이싱

- API 서버: `SET LOCAL app.trace_id = :traceId`(스펙 §6.3)
- 워커: MDC 세팅 → `indexing_job.trace_id` 저장 → 모든 로그에 포함

인라인 재시도는 같은 스레드에서 도니 MDC가 자동 유지된다.

---

## 4. 구현 순서

| 순서 | 항목 | 선행 | 상태 / 이유 |
|---|---|---|---|
| — | **P0-4 인라인 재시도 전환** | — | **완료**(`5f52bd5`, `d4ff1e6`) |
| 1 | **P0-5 컨슈머 게이트 + `nack`** | — | 선행 없음, 최우선 — 미구현 상태라 지금도 DB 장애 시 데이터가 조용히 사라질 수 있다 |
| 2 | **P0-1 에러 분류** | — | 인라인 리스너의 분기 조건 정교화 |
| 3 | **P0-2 리소스 가드** | 2 | "1회 처리 ≤120초" 상한 확보 |
| 4 | **P0-3 재시도 예산 확정** | 2, 3 | `max.poll.interval.ms`를 900초로 상향, 선형 백오프 기준 재계산 |
| 5 | **P1-2 메트릭** | 1~4 | 지금 상태가 실제로 안전한지 확인할 유일한 수단 |
| 6 | **P1-1 진행률 API** | 스키마 3컬럼 | 인라인 재시도 가시성 + FAILED 노출 |
| 7 | **P1-3 Outbox 보정** | doc-relay 합의 | 경계를 넘음 |
| 8 | **P2-1 DB 페일오버** | 1 | 재활용 + 인프라 선행 |
| 9 | **P2-2 시각 통일** | — | 미루면 재현 안 되는 버그로 돌아온다 |
| 10 | **P2-4 삭제 스윕 방어** | — | |
| 11 | **P2-5 트레이싱** | API 서버 `SET LOCAL` | 외부 의존 |
| 12 | **P2-3 파티션 튜닝** | 5 | 실측 후에만 |

---

## 5. 테스트 시나리오

### 5.0 (회귀) 워커 크래시는 Kafka가 회수한다

```bash
docker kill -s SIGKILL worker-1   # 리스너가 처리 중일 때
```
**기대**: 리밸런스 → 다른 워커가 재처리 → 완료. 청크 중복 0건.

### 5.1 인라인 재시도 — 예산 안에서 성공

임베딩을 2회 실패 후 성공하도록 구성.

```sql
SELECT status, attempt_count FROM indexing_job WHERE id = :jobId;
-- 기대: COMPLETED, attempt_count = 3
```
**기대**: `kafka_rebalance_total` 증가하지 않음. 추가 메시지 발행 없음.

### 5.2 ★ 재시도 예산 경계 검증 — 가장 중요

**정상 예산**: 1회 처리 120초 × 5회 + 선형 백오프(30+60+90+120=300초) = 900초. `max.poll.interval.ms`를 900초로 올린 뒤 테스트한다 — 현재 설정(600000)으로는 이 시나리오 자체가 예산 초과다.
```sql
SELECT status FROM indexing_job WHERE id = :jobId;  -- COMPLETED
```
**기대**: `kafka_rebalance_total` **증가하지 않음.**

**예산 초과**: 1회 처리를 250초로 늘려 5회 재시도 유발(처리 1250초 + 백오프 300초 > 900초).
**기대**: `kafka_rebalance_total`이 증가하고 파티션이 재할당된다. **이 테스트는 "실패해야 정상"이 아니라 예산 계산이 맞는지 확인하는 경계 테스트다.**

### 5.3 ★ 재시도 대기 중 크래시 — 고아가 안 생긴다

```bash
docker kill -s SIGKILL worker-1   # 인라인 백오프 sleep 중
```
**기대**: ack이 아직 안 됐으므로 리밸런스 → 재전달 → 다른 워커가 처음부터 재처리 → 완료. `indexing_job`에 `PROCESSING`으로 남는 행이 **없다.**

### 5.4 에러 분류 + 5회 소진 (P0-1, P0-3)

| 입력 | 기대 |
|---|---|
| 0바이트 PDF | `attempt_count = 1`에서 즉시 `FAILED('CORRUPTED_FILE')`. 재시도 없음 |
| 스캔 PDF | 즉시 `FAILED('EMPTY_EXTRACTION')`, **검색 버전 전환 안 됨** |
| 300MB 파일 | 다운로드 전에 `FAILED('FILE_TOO_LARGE')` — S3 트래픽 0 |
| 429 계속 발생 | `Retry-After`를 안 주므로 다른 `TransientIndexingException`과 동일하게 선형 백오프로 5회 재시도 후 `FAILED` |
| 임베딩 계속 실패 | 5회 모두 실패 → `FAILED`. `indexing_job_failed_total` 증가. 재인덱싱 API로 복구 확인 |

### 5.5 DB 장애 → `nack` (P0-5)

```bash
docker stop postgres
sleep 30
docker start postgres
```
**기대**: `nack` → 컨슈머 pause → 복구 후 resume → 전부 처리. Kafka lag이 원복. 유령 `PROCESSING` 행 없음.

### 5.6 리소스 가드 (P0-2)

- 1만 페이지 PDF → 힙 상한 내 유지
- 순환 참조 PDF → `PARSE_TIMEOUT`(60초) 후 다음 메시지 정상 처리. `kafka_rebalance_total` 증가 없음
- `indexing_job_duration_seconds` p99 < 120초 확인

### 5.7 Kafka 장애 + Outbox 보정 (P1-3)

```bash
docker stop kafka   # 문서 3건 업로드 → outbox PENDING
docker start kafka
```
**기대**: 자동 발행 → 완료.

```sql
UPDATE outbox_event SET status='PUBLISHED', published_at = CURRENT_TIMESTAMP - INTERVAL '10 minutes' WHERE id=:id;
DELETE FROM indexing_job WHERE source_event_id = :id;
```
**기대**: 보정 배치가 `PENDING` 리셋 → 재발행 → 완료.

### 5.8 카오스 테스트 (최종 성적표)

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
-- ★ PROCESSING이 하나라도 남아 있으면 = 불변조건 위반

-- ② 검색 버전의 청크 수가 실측과 일치하는가
SELECT d.id, j.total_chunks, count(c.id) AS actual
FROM document d
JOIN document_version v ON v.id = d.searchable_version_id
JOIN indexing_job j ON j.document_version_id = v.id AND j.status = 'COMPLETED'
LEFT JOIN document_chunk c ON c.document_version_id = v.id
GROUP BY d.id, j.total_chunks
HAVING j.total_chunks <> count(c.id);
-- 0건이어야 한다
```

---

## 6. 데모 시나리오

**① 워커 강제 종료 → Kafka 자동 재할당** *(Track A 성과)*
100페이지 PDF 인덱싱 중 `docker kill` → 리밸런스 → 다른 워커가 처음부터 재처리 → 완료.
*설명 포인트*: ack을 마지막에 하기 때문에 Kafka가 알아서 넘겨준다. 우리가 만든 장치는 없다.

**② 재시도 중 워커 사망 → 그래도 회수됨** *(Track B 전환 성과)*
임베딩 실패로 인라인 재시도 대기 중 `docker kill` → 리밸런스 → 재처리 → 완료.
*설명 포인트*: "재시도를 ack 이전으로 옮겼더니 회수 장치가 필요 없어졌다." ①과 나란히 보여주면 경계가 한 장면에 들어온다.

**③ 손상 파일 → 즉시 실패**
스캔 PDF → `EMPTY_EXTRACTION`으로 1회 만에 FAILED → 기존 검색 버전 유지 확인 → 정상 파일 교체 후 재처리 → 완료.

**④ DB 페일오버 → 무손실 복구** (2차)
업로드 중 Primary kill → `nack`으로 메시지 되돌림 → pause → promote → resume → 전부 완료.

---

## 7. 다른 담당자에게 요청할 것

### 7.1 인덱싱 처리 보강

- 임베딩 실패를 `TransientIndexingException`/`PermanentIndexingException`으로 감싸기
- 임베딩 호출에 타임아웃 30초 — P0-3 예산의 구성 요소
- `indexing_job.processed_chunks` 갱신(P1-1)
- 벡터 차원 불일치는 `Permanent`

### 7.2 스키마 / API 서버 담당

| 대상 | 요청 |
|---|---|
| `indexing_job` | `phase`, `total_chunks`, `processed_chunks` 추가(P1-1) |
| `outbox_event` | `idx_outbox_published` 부분 인덱스(P1-3) |
| `document` | `chunk_purge_failed_count` 추가(P2-4) |
| 트랜잭션 | `SET LOCAL app.trace_id`(P2-5) |
| 재처리 API | 게이트 조건을 `status = 'FAILED'`로(스펙 §6.3) |
| `consumer.max-poll-interval-ms` | 600초 → 900초로 변경(P0-3) — 인프라/배포 설정에 반영 필요 |

### 7.3 `doc-relay` 담당

- **P1-3 Outbox 보정 배치의 소유권 합의.** 경계상 `doc-relay` 쪽이 깨끗하므로 먼저 제안
- 발행 실패 지속 시 서킷 브레이커

### 7.4 인프라 담당

- Kafka를 독립 배포 단위로(스펙 §6.5)
- OpenSQL 클러스터 + 페일오버 절차(P2-1)
- Toxiproxy를 개발 환경 compose에 포함
- 워커 컨테이너 `TZ=UTC` 고정(P2-2)

---

## 부록. Track B 핵심 원칙

1. **`PROCESSING`/`RETRY_WAIT`인 Job에는 항상 대응하는 미커밋 Kafka 메시지가 있다.** 이번 전환의 불변조건. **ack 이후에 Job을 다시 활성 상태로 만드는 코드를 쓰지 않는다.**
2. **Kafka가 이미 해주는 일을 다시 만들지 않는다.** 워커 크래시 복구는 Kafka의 몫이고, 우리 장치는 Kafka가 손댈 수 없는 구간만 대상으로 한다 — 그 구간은 이제 없다.
3. **문제를 막을 장치를 만들기 전에, 문제가 안 생기는 구조가 있는지 먼저 본다.** 고아 회수 스윕 대신 재시도를 ack 이전으로 옮겼다.
4. **재시도는 `max.poll.interval.ms` 예산 안에서만 한다.** 넘기는 순간 리밸런스가 나고, 리밸런스는 재시도의 대체재가 아니라 장애 확산 경로다.
5. **복구 가능하면 별도 저장소를 새로 만들지 않는다.** `FAILED`는 `indexing_job`에 이미 다 남고 재인덱싱 API로 복구되므로, 그 상태를 그대로 조회·노출·알림의 대상으로 쓴다. Job 자체가 안 만들어지는 실패(역직렬화 등)만 예외적으로 흔적이 없다는 걸 인지하고, Outbox 보정 배치로 일부 완화한다.
6. **기록하지 못한 실패는 ack하지 않고 `nack`한다.** always-ack 원칙의 유일한 예외.
7. **Track A의 정합성 메커니즘을 대체하지 않는다.** 세 겹 멱등성, fencing, UPSERT 수렴, 청킹 결정성은 그대로다.
8. **관측이 먼저, 튜닝은 나중.** 특히 `kafka_rebalance_total`과 `indexing_job_duration_seconds` p99 — 이번 설계가 안전한지는 이 두 숫자가 말해준다.
9. **감지할 수 없으면 대비한 게 아니다.**
