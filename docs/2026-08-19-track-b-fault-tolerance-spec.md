# Track B 구현 스펙 — 장애 복구 확장

> **담당**: 은지 (A — 수집 & 워커 확장성, `[임베딩 워커 - 그룹2]`)
> **작성일**: 2026-08-19 (갱신: 2026-08-20, Kafka record identity 설계와 실제 코드 상태 반영)
> **상태**: 핵심 장애 복구 경로 구현 완료 — 배치 수신, 인라인 재시도, Kafka record identity 판정, DB 장애 batch nack, DB health gate가 반영돼 있다. 임베딩 전용 timeout, 운영 대시보드·알림, 실제 Kafka/PostgreSQL 기반 장애 주입 검증은 별도 잔여 과제다.
> **근거 문서**: [`FAULT_TOLERANCE.md`](./FAULT_TOLERANCE.md)(설계 근거·논의 과정), [`indexing-worker-flow-and-failure-design.md`](./indexing-worker-flow-and-failure-design.md)(현재 구현의 전체 흐름), [`2026-08-19-track-b-fault-tolerance-plan.md`](./2026-08-19-track-b-fault-tolerance-plan.md)(구현 작업 이력)
> **갱신 사유**: 초판 이후 ① 재시도 폴러가 리스너 인라인 재시도로 대체됐고, ② 백오프가 선형으로 확정됐으며, ③ 에러 분류가 예외 화이트리스트로 구현됐고, ④ `indexing_job`에 Kafka topic/partition/offset을 저장해 동일 record 재전달과 새 record 중복 발행을 구분하도록 설계가 바뀌었다. 이 개정판은 실제 코드(`IndexingPipelineRunner`, `IndexingFailureService`, `IndexingKafkaListener`, V1 schema)를 기준으로 모호하거나 오래된 표현을 바로잡는다.

---

## 0. 이 문서의 성격

`FAULT_TOLERANCE.md`는 **왜 이렇게 하는가**(장애 시나리오, 논의, 트레이드오프)를 담은 설계 근거 문서다. 이 문서는 그 결론을 **무엇을 구현하는가**(인터페이스, 상태 전이 규칙, 스키마, 설정값, 완료 조건)로 옮긴 실행 스펙이다. 각 항목은 근거가 필요하면 `FAULT_TOLERANCE.md`의 절 번호를 인용하고, 재서술하지 않는다.

**핵심 전환 한 줄 요약**: 재시도를 ack **이후**(폴러 기반)에서 ack **이전**(리스너 인라인)으로 옮긴다 — **이미 완료됨**(P0-4, `5f52bd5`/`d4ff1e6`). 정상 Runner 재시도 경로에서는 미커밋 Kafka record가 미완료 Job 회수 수단이 되며, generic 예외를 로그 후 ack하는 잔여 경계는 §0.2와 §1에 별도로 명시한다.

**범위 밖**: Track A가 이미 완성한 정합성 메커니즘(`source_event_id`/활성 버전 제약, chunk UPSERT, 결정적 청킹, 검색 버전 fencing)은 그대로 유지한다. 여기서 말하는 검색 버전 fencing은 낮은 `embedding_version_no`가 `searchable_version_id`를 덮어쓰지 못하게 하는 조건부 UPDATE다. Job 실행권을 배타적으로 보장하는 Lease나 fencing token은 현재 설계에 없다.

**(2026-08-20 현재 구현 정정) 2026-08-19 검토에서는 trailing DELETE를 요구사항에서 제외하기로 했지만, 현재 `IndexingPublicationService.publish()`는 `deleteTrailingChunks(documentVersionId, lastChunkNo)`를 호출한다.** 같은 코드·설정의 재실행은 결정적이어서 정상적으로 청크 수가 줄지 않지만, 현재는 `FIXED_TOKEN` 외 전략도 설정으로 선택할 수 있고 사용 전략을 Job에 저장하지 않는다. 따라서 실제 구현의 trailing DELETE는 배포 버전·전략·parser/tokenizer 변경이나 과거 데이터로 더 큰 `chunk_no`가 남은 경우를 방어한다. 현재 동작과 근거는 `indexing-worker-flow-and-failure-design.md` §5.7·§5.9를 우선한다.

### 0.1 이벤트와 Kafka key 계약

| 필드 | 현재 계약 |
|---|---|
| `eventId` | 같은 논리 이벤트의 멱등 키. 중복 발행 시에도 같은 값을 유지해야 한다 |
| `eventType` | `INDEXING_REQUESTED` 또는 `DOCUMENT_DELETED` |
| `tenantId` | DB의 문서 tenant와 일치해야 한다 |
| `documentId` | 처리 대상 문서 ID이며 Kafka key로 사용해야 하는 값 |
| `documentVersionId` | 인덱싱 이벤트에는 필수, 삭제 이벤트에는 `null` |

`DOCUMENT_DELETED.documentVersionId = null`은 특정 버전을 지정하지 않는다는 뜻이다. 삭제 핸들러는 이 필드를 읽지 않고 `documentId`만 사용하여 해당 문서의 모든 활성 Job을 실패 처리하고 모든 버전의 청크를 삭제한다. 현재 구현은 삭제 이벤트에 non-null 값이 들어와도 이를 삭제 범위로 사용하거나 거부하지 않고 무시한다.

Producer는 Kafka key를 `documentId`의 일관된 문자열 표현으로 발행해야 한다. 배치 리스너는 payload가 아니라 `ConsumerRecord.key()`로 그룹화하므로 같은 key의 record는 수신 순서대로 처리되고 서로 다른 key 그룹은 executor에서 병렬 처리된다. Worker는 Kafka key와 payload `documentId`의 일치 여부를 검증하지 않는다. 따라서 key가 누락되거나 잘못되면 문서 단위 순서 보장도 깨진다.

### 0.2 역직렬화·예외·ack/nack 경계

이 문서에서 **역직렬화 실패**는 Kafka가 이미 `String`으로 전달한 `ConsumerRecord.value()`의 JSON을 Jackson이 `IndexingRequestedEvent`로 매핑하지 못한 경우다. JSON 문법 오류, UUID/`Instant`/숫자 변환 실패, non-null 필드 누락 등이 해당한다. PDF/HWP 원문 파싱 실패가 아니며, Kafka wire payload를 `String`으로 만드는 consumer deserializer의 실패는 `processRecord()`의 catch 범위 밖이다.

| 경계 | 현재 동작 | 의미 |
|---|---|---|
| JSON→이벤트 역직렬화 실패 | 로그 후 해당 record 종료 | 다른 그룹까지 끝나면 배치 ack. DB/DLQ 기록은 없음 |
| 이벤트 검증 실패 | Runner에서 Job `FAILED` 또는 리스너에서 로그 후 종료 | 동일 입력 재전달로 회복되지 않는 것으로 처리 |
| `DataAccessException`이 `processRecord()` 밖으로 이탈 | 배치의 모든 future를 기다린 뒤 `ack.nack(0, 5초)` | DB에 결과를 기록하지 못했으므로 배치 전체 재전달 |
| 일반 `Exception`이 generic catch까지 도달 | 로그 후 삼키고 배치 처리 계속 | Job 획득 전 비-DB runtime, 실패 기록 자체의 비-DB 오류, 삭제 경로의 예기치 않은 runtime 등이 가능하다. 실패 상태가 DB에 남지 않고 ack될 수 있는 잔여 위험이다 |
| `Error` 등 `Exception` 밖의 치명 오류 | future/컨테이너로 전파, ack하지 않음 | `OutOfMemoryError`, `StackOverflowError`, `LinkageError` 등. 원인이 결정적이면 Kafka 재전달 후에도 같은 오류가 반복될 수 있다 |

Job `start()` 뒤 발생한 `Error`는 이미 `attempt_count`가 증가했으므로 재전달이 반복되면 Job 시도 상한에 도달할 수 있다. 반면 Job 생성 전, 삭제 경로, 역직렬화 경로에서 발생한 `Error`에는 Job 단위 시도 상한이 없다. 이런 치명 오류는 일반적인 retry로 회복된다고 가정하지 않고 프로세스 복구와 경보가 필요한 운영 장애로 본다.

DB health gate와 nack에는 선후 관계가 없다. Health gate는 애플리케이션 기동 후 독립 스케줄로 DB를 점검하므로 배치 처리 전·중·nack 후 어느 때든 pause를 요청할 수 있다. 실제 배치에서 `DataAccessException`이 이탈하면 health gate 실행 여부와 무관하게 nack한다.

### 0.3 Kafka record identity와 실행권 모델

`@KafkaListener`가 받은 `ConsumerRecord`에서 `topic()`, `partition()`, `offset()`을 꺼내 `KafkaRecordIdentity`를 만든다. Kafka record의 불변 위치는 세 값의 조합이며, `indexing_job`에도 세 값을 함께 저장한다.

`IndexingPipelineRunner.acquireJobId()`는 `insertIfAbsent()`로 `INSERT ... ON CONFLICT DO NOTHING`을 먼저 실행한 뒤, 삽입 성공 여부와 무관하게 `source_event_id`로 Job을 다시 조회한다. 유니크 제약은 충돌을 정의하고 `insertIfAbsent()`는 그 충돌을 예외 대신 0행 삽입으로 바꾸므로 둘은 대체 관계가 아니다. 재조회한 기존 행의 Kafka identity를 현재 record와 비교해야 아래 판정이 가능하다.

| 충돌 상황 | 판정 | 처리 |
|---|---|---|
| INSERT 성공 | 신규 Job | 정상 처리 |
| 같은 `eventId`, 같은 topic/partition/offset | 동일 Kafka record 재전달 | 미완료 Job 재처리 허용 |
| 같은 `eventId`, 다른 topic/partition/offset | 같은 논리 이벤트가 새 record로 중복 발행됨 | 멱등하게 무시 |
| 다른 `eventId`, 같은 활성 `documentVersionId` | 이미 다른 Job이 같은 버전을 처리 중 | 현재 record 종료, 기존 활성 Job이 작업을 대표 |

“다른 consumer가 죽어서 받은 이벤트”는 별도 종류가 아니다. 이전 consumer의 장애·리밸런스·nack 때문에 **같은 topic/partition/offset의 record가 재전달된 원인** 중 하나다. Worker는 장애 원인까지 구분하지 않고 record identity가 같은지만 확인한다.

이 설계는 중복 실행 가능성을 허용한다. `worker_id`는 마지막 획득 워커를 기록하는 관측 값이고 `attempt_count`는 시도 횟수일 뿐 fencing token이 아니다. 완료/실패 쓰기에도 두 값을 실행권 검증용으로 사용하지 않는다. 결과는 결정적 청킹, `(document_version_id, chunk_no)` UPSERT, 검색 버전 fencing으로 수렴시킨다. 그 대신 Lease 만료·갱신과 미완료 Job 회수 scheduler를 두지 않는다.

Kafka identity 확인은 `acquireJobId()`에서만 한다. 현재 production 경로에서 `start()`는 항상 `acquireJobId()` 성공 뒤 호출되며 우회 호출 경로가 없으므로 `start()`에서 identity를 다시 검증하지 않는다. `DOCUMENT_DELETED`는 `indexing_job`을 만들지 않아 이 판정 대상이 아니고 문서 전체 삭제를 멱등하게 반복한다.

---

## 1. 상태 전이 불변조건 (모든 항목의 전제)

> **설계 목표: `indexing_job`이 `PROCESSING` 또는 `RETRY_WAIT`인 동안에는, 그에 대응하는 커밋되지 않은 Kafka 메시지가 반드시 존재한다.**

- 이 불변조건을 지키는 유일한 규칙: **ack 이후에 Job 상태를 다시 `PROCESSING`/`RETRY_WAIT`으로 만드는 코드를 작성하지 않는다.**
- 위반 여부는 §8.4 카오스 테스트로 검증한다 — 실행 중인 record가 없는데 `PROCESSING`이 장시간 남아 있으면 위반이다.
- `RETRY_WAIT`은 관측 전용 상태가 아니다. Runner가 `next_retry_at`을 읽고 DB 시각으로 남은 시간을 계산하여 같은 executor thread에서 대기한 뒤 재획득하는 **인라인 재시도 제어 상태**다. 다만 이 상태를 스캔해 깨우는 별도 scheduler/poller는 없다.
- 동일 record가 due 전에 재전달되어 `start()`가 0을 반환해도 정상 종료·ack하지 않는다. `RETRY_WAIT.next_retry_at`까지 인라인 대기한 뒤 다시 `start()`한다.
- 현재 generic `Exception` catch가 예외를 삼키는 경로는 실패 상태를 기록하지 못한 채 ack하여 이 목표를 깨뜨릴 수 있는 잔여 위험이다. §0.2의 예외 경계와 §8의 장애 검증에서 별도로 확인한다.

---

## 2. 스키마 변경

아직 서버와 초기 schema를 실행하지 않은 전제이므로 별도 V2 호환 migration을 만들지 않고 V1 `indexing_job`에 다음 컬럼을 처음부터 포함한다.

```sql
kafka_topic       VARCHAR(255) NOT NULL,
kafka_partition   INTEGER NOT NULL,
kafka_offset      BIGINT NOT NULL,
phase             VARCHAR(30)
```

- topic은 공백일 수 없고 partition/offset은 0 이상이어야 한다.
- `source_event_id UNIQUE`와 활성 상태의 `document_version_id` 부분 유니크 인덱스는 유지한다.
- Kafka identity 컬럼은 `insertIfAbsent()`에서 최초 INSERT할 때 기록하고 이후 다른 record가 선점·갱신하지 않는다.
- `phase`는 현재 `DOWNLOADING`, `PARSING`, `CHUNKING`, `EMBEDDING` 진입 직전에 갱신한다. `PUBLISHING` 갱신은 아직 구현되지 않았다.
- `indexing_job.status` 허용값은 `PENDING/PROCESSING/RETRY_WAIT/COMPLETED/FAILED`를 유지한다.

삭제 스윕 chunk 삭제가 반복 실패하는 경우에 대한 별도 방어(무한 재시도 카운터)는 두지 않는다(2026-08-19 결정) — 실제로 발생하지 않는다고 보고 리스크를 감수한다. 필요성이 관측되면 그때 추가한다(부록 원칙 8).

Outbox 보정(발행 실패 감지·재발행)은 `doc-relay` 쪽에서 이미 구현돼 있어 Track B 스코프에서 제외한다(2026-08-19 확인) — §4에서 별도 P1-3 항목을 두지 않는다.

---

## 3. 설정 스키마

```yaml
spring:
  kafka:
    consumer:
      max-poll-records: 10
      properties:
        max.poll.interval.ms: 900000
        session.timeout.ms: 45000
    listener:
      ack-mode: manual
      type: batch

indexing:
  limits:
    max-file-size-bytes: 209715200      # 200MB — P0-2(a)
    parse-timeout: PT60S                # P0-2(b)
  storage:
    download-timeout: PT30S             # 현재 고정값. 환경변수 placeholder 없음
  retry:
    max-attempts: 5                     # P0-3, INDEXING_MAX_ATTEMPTS — 이미 구현·적용됨
    base-delay: PT30S                   # P0-3, INDEXING_RETRY_BASE_DELAY — 선형 백오프 기준 간격, 이미 구현·적용됨
  db-health-gate:
    check-interval-ms: 5000             # P0-5
    pause-nack-delay: PT5S
```

임베딩 전용 timeout 설정은 현재 없다. 따라서 다운로드 30초 + 파싱 60초 + 임베딩 30초라는 과거 120초 상한 가정은 현재 구현이 보장하지 않는다.

### 3.1 청킹 전략 선택 기준

청킹 전략은 문서 MIME·크기·tenant에 따라 자동 선택하지 않는다. Worker 기동 시 전역 설정 `INDEXING_CHUNKING_STRATEGY`를 읽어 모든 Job에 같은 전략을 적용하며 기본값은 `FIXED_TOKEN`이다.

| 설정값 | 동작 |
|---|---|
| `FIXED_TOKEN` | 전체 블록 텍스트를 합쳐 512 token 단위로 분할 |
| `PARAGRAPH` | 문단 경계를 우선 유지하고 긴 블록만 512 token 단위로 분할 |
| `PARAGRAPH_OVERLAP` | 긴 블록을 512 token, 기본 64 token overlap으로 분할 |

같은 입력·parser·tokenizer·전략·설정이면 결과는 결정적이다. 다만 실제 사용 전략을 Job이나 문서 버전에 저장하지 않으므로 rolling deployment 중 설정이 섞이거나 parser/tokenizer가 바뀌면 재처리 결과와 청크 수가 달라질 수 있다. 이 경우 publication의 trailing DELETE가 새 마지막 `chunk_no`보다 큰 과거 청크를 제거한다.

---

## 4. 컴포넌트 스펙

우선순위 기준: ①막지 않으면 데이터가 조용히 사라지는가 ②장애를 감지할 수 있는가 (`FAULT_TOLERANCE.md` §3 도입부).

### P0-1. 에러 분류 (Permanent / Transient)

**막는 장애**: 영구 실패에 재시도 낭비, 일시 실패의 재시도 부족.

**실제 구현 방식 — sealed class가 아니라 영구 실패 화이트리스트.** `isRetryable()`에 명시된 예외만 영구 실패로 보고, 그 밖의 `Exception`은 기본적으로 재시도 가능으로 처리한다. 이 방식을 현재 설계로 확정하며 sealed class 마이그레이션은 범위에 두지 않는다.

```kotlin
// IndexingPipelineRunner.kt — 실제 구현 형태
private fun isRetryable(e: Exception): Boolean = when (e) {
    is InvalidEventException,
    is ContentIntegrityException,
    is EmptyExtractionException,
    is ChunkLimitExceededException,
    is TotalTokenLimitExceededException,
    is UnsupportedMimeTypeException,
    is FileTooLargeException,
    is CorruptedFileException,
    is EmbeddingRequestRejectedException,
    is InvalidEmbeddingException -> false
    else -> true
}
```

**에러 코드 매핑** (판정 로직이 아니라 `last_error_code`에 기록되는 값 기준)

| 분류 | 코드 | 발생 지점 |
|---|---|---|
| Permanent | `EMPTY_EXTRACTION` | `ChunkGuard` |
| Permanent | `CHUNK_LIMIT_EXCEEDED` | `ChunkGuard` |
| Permanent | `TOTAL_TOKEN_LIMIT_EXCEEDED` | `ChunkGuard` |
| Permanent | `HASH_MISMATCH` | `ContentIntegrityException` |
| Permanent | `UNSUPPORTED_MIME_TYPE` | `DocumentParserRegistry` |
| Permanent | `CORRUPTED_FILE` | 파서가 열지 못함 |
| Permanent | `FILE_TOO_LARGE` | P0-2 |
| Permanent | `EMBEDDING_REQUEST_REJECTED` | 임베딩 공급자 HTTP 400 |
| Permanent | `INVALID_EMBEDDING` | 응답 개수·차원·유한값 검증 실패 |
| Permanent | `DOCUMENT_DELETED` | `failActiveJobsForDocument()` |
| Transient | 예외 클래스명 fallback | S3 timeout/server/network 오류, 임베딩 429/5xx/네트워크 오류 등 |
| Transient | `ParseTimeoutException` | P0-2. 예외 자체의 `code`는 `PARSE_TIMEOUT`이지만 현재 `errorCodeOf()` 매핑에 없어 DB에는 클래스명이 기록됨 |
| 별도 경계 | `DataAccessException` | 실패 상태까지 기록할 수 없고 리스너로 이탈한 경우 batch nack(P0-5) |

**동작 규칙**

- 429, 5xx, 네트워크 오류는 영구 실패 목록에 없으므로 `RETRY_WAIT`으로 전이하고 P0-3의 선형 backoff 뒤 같은 executor thread에서 재시도한다. `Retry-After` 기반 분기는 두지 않는다.
- HTTP 400은 `EmbeddingRequestRejectedException`, 잘못된 embedding 결과는 `InvalidEmbeddingException`으로 변환되어 즉시 `FAILED`가 된다.
- DB 작업 중 발생한 예외라도 실패 상태를 DB에 정상 기록할 수 있으면 일반 재시도 경로를 탈 수 있다. DB 시각 조회나 실패 기록 자체도 실패해 `DataAccessException`이 리스너까지 이탈한 경우에만 batch nack한다.
- 새 영구 예외를 추가할 때는 `isRetryable()`과 `errorCodeOf()`를 함께 수정해야 한다. 등록하지 않은 예외는 클래스 simpleName을 `last_error_code`로 사용하고 재시도한다.

**완료 조건**

- [x] 영구 실패 화이트리스트 방식으로 확정·구현됨
- [x] 파일·파싱·청킹·임베딩 결과 검증 예외가 현재 목록에 반영됨
- [x] 리스너까지 이탈한 `DataAccessException`은 별도 nack 경계에서 처리됨
- [ ] 0바이트 PDF·스캔 PDF·300MB 파일·429 반복·임베딩 반복 실패 케이스 테스트 통과(`FAULT_TOLERANCE.md` §5.4)

---

### P0-2. 리소스 가드 — 파일 크기 상한 + 파싱 타임아웃 + 메모리 경계

**막는 장애**: OOM으로 인한 fleet 붕괴, 재시도 예산 붕괴 (B-Gap-3, B-Gap-4).

**현재 상태**: 파일 크기 상한, 임시 파일 다운로드, 스트리밍 SHA-256, 파싱 timeout은 구현됐다. 임베딩 전용 timeout과 전체 파이프라인 1회 처리 시간 상한은 아직 없다.

`FileTooLargeException`은 영구 실패 목록에 포함되고 `ParseTimeoutException`은 기본 분기에 따라 재시도 가능하다.

**(a) 파일 크기 상한 — 다운로드 전 판정**

```kotlin
if (documentVersion.fileSize > maxFileSizeBytes) {
    throw FileTooLargeException(documentVersion.fileSize, maxFileSizeBytes)
}
```

**(b) 파싱 타임아웃**

```kotlin
private val parseExecutor = Executors.newFixedThreadPool(concurrency)

fun parse(parser: DocumentParser, source: Path): List<ParsedBlock> {
    val future = parseExecutor.submit<List<ParsedBlock>> {
        Files.newInputStream(source).use { parser.parse(it).toList() }
    }
    return try {
        future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS)
    } catch (e: TimeoutException) {
        future.cancel(true)
        throw ParseTimeoutException(mimeType, parseTimeout)
    }
}
```

타임아웃 처리는 워커를 살려두므로 `ParseTimeoutException`이 `isRetryable()`의 기본 분기(재시도 가능)를 타 인라인 재시도(P0-4)로 이어진다 — hang을 방치해 리밸런스가 나는 것과는 다른 경로임을 구분한다.

**(c) 메모리 사용 경계**

- 다운로드는 메모리 `ByteArray` 대신 임시 파일로 스풀한다.
- 해시는 `DigestInputStream`으로 스트리밍 계산한다(전체 바이트를 다시 메모리에 올리지 않는다).
- 임시 파일은 `finally` 블록에서 반드시 삭제한다.
- 파싱 결과는 timeout future 안에서 `toList()`로 물질화되므로 parser→chunker 전체가 완전한 스트리밍인 것은 아니다.

**완료 조건**

- [x] 선언된 파일 크기 200MiB 상한과 임시 파일 다운로드 적용
- [x] 파싱 60초 timeout과 `future.cancel(true)` 적용
- [x] SHA-256 스트리밍 계산과 `finally` 임시 파일 삭제 적용
- [ ] 임베딩 호출 timeout을 추가해 전체 시도 시간 상한을 정함
- [ ] 1만 페이지 PDF 처리 시 힙 사용량이 상한 내(`FAULT_TOLERANCE.md` §5.6)
- [ ] 순환 참조 PDF에서 60초 후 `ParseTimeoutException`이 발생해 인라인 재시도되고 다음 메시지도 정상 처리됨, `kafka_rebalance_total` 증가 없음
- [ ] `future.cancel(true)` 후 parser가 interrupt에 협조하는지 확인. 현재 `parse_thread_leaked`는 실제 생존 스레드 수가 아니라 timeout 취소 요청 누적치다

---

### P0-3. 재시도 예산 설계

**막는 장애**: 인라인 재시도가 `max.poll.interval.ms`를 넘겨 발생하는 리밸런스 폭풍.

**공식**: `처리시간 × 시도횟수 + 백오프 총합 + 마진 < max.poll.interval.ms`

**재시도 횟수·백오프는 이미 확정·구현되어 있다** — 초판 스펙의 "미결 사항"(지수 백오프 5s→40s)은 폐기됐다. 실제 구현(`IndexingFailureService.recordFailure()`, 5f52bd5)은 **선형** 백오프이고, `next_retry_at = failedAt + base_delay × attempt_count`다.

| 항목 | 값 | 근거 |
|---|---|---|
| 1회 처리 시간 상한 | 보장 없음 | 다운로드·파싱 timeout은 있으나 임베딩 전용 timeout이 없음 |
| 인라인 재시도 횟수 | **5회** | `INDEXING_MAX_ATTEMPTS` 기본값 5 — 이미 구현·적용됨 |
| 백오프 | **선형** `base-delay(기본 30s) × attempt_count` → 30/60/90/120초, 합 300초 | `IndexingFailureService.recordFailure()` — 이미 구현·적용됨 |
| `max.poll.interval.ms` | **900초** | `application.yml` 반영 완료 |
| 실제 poll 점유 시간 | 각 시도 처리 시간 합 + 최대 300초 + 같은 key record 처리/queue 대기 | 900초 이내라는 하드 보장은 없음 |

```kotlin
// IndexingFailureService.recordFailure() — 실제 구현 형태
val multiplier = attemptCount.coerceAtLeast(1)
val nextRetryAt = failedAt.plus(baseDelay.multipliedBy(multiplier.toLong()))
```

**주의**: `max.poll.interval.ms`를 올려도 진짜 크래시 감지 속도는 그대로다 — 워커 생사 판정은 `session.timeout.ms`(45초, heartbeat 기반)가 별개로 담당한다.

**흡수 범위**: 최대 다섯 번 시도하며 첫 네 번 실패 뒤 30/60/90/120초를 대기한다. 다섯 번째 실패는 추가 sleep 없이 `FAILED`로 종결한다. 따라서 backoff 합은 300초지만 처리 시간은 별도이며, 현재 코드만으로 “총 몇 분까지 장애를 흡수한다”고 고정할 수 없다. 장기 장애는 `FAILED` 조회와 재인덱싱 API로 복구한다(별도 DLQ 저장소 없음).

여기서 **인라인 재시도**는 실패 record를 ack한 뒤 새 Kafka 이벤트나 scheduler로 다시 처리한다는 뜻이 아니다. 같은 `IndexingPipelineRunner.run()` 호출과 같은 executor thread 안에서 `RETRY_WAIT`을 기록하고, 기본 `RetryWaiter`가 남은 시간만큼 `Thread.sleep()`한 뒤 루프를 다시 돈다. sleep 중에는 해당 thread와 배치가 점유되고 offset은 커밋되지 않는다.

`max.poll.interval.ms`를 늘리면 poll 복귀 여유는 커지지만 처리 hang 감지가 늦어지고, 재시도 횟수를 늘리면 일시 장애 흡수 범위와 함께 파티션 블로킹도 커진다. `session.timeout.ms` 45초는 process crash처럼 heartbeat가 끊긴 경우를 별도로 감지한다. 실제 값은 `kafka_rebalance_total`과 `indexing_job_duration_seconds`를 보고 조정한다.

**완료 조건**

- [x] `max.poll.interval.ms = 900000` 적용
- [x] 최대 5회, DB 시각 기준 30초 선형 backoff 구현
- [ ] 임베딩 timeout과 배치/queue 대기를 포함한 실제 최악 시간 예산을 다시 산정
- [ ] 마지막 시도 성공·최종 실패·poll interval 경계 통합 테스트

---

### P0-4. 인라인 재시도 전환 — 재시도 폴러 제거 ✅ 완료 (`5f52bd5`, `d4ff1e6`)

**막는 장애**: 재시도 경로 크래시로 인한 영구 유실(B-Gap-1).

**실제 구현 — 초판 스펙과 형태가 다르다.** 초판은 리스너 안 `for` 루프로 재시도 횟수를 세는 설계였지만, 실제 구현은 ① Kafka 리스너가 **배치**로 record를 받아 Kafka key로 그룹화하고, ② 그룹 task가 record를 순서대로 `processRecord()`에 전달하며 인덱싱 record마다 `IndexingPipelineRunner.run()`을 호출하고, ③ 재시도는 `run()` 내부의 `while` 루프가 전담하고, ④ ack은 배치 전체(모든 그룹)가 끝난 뒤 한 번만 호출된다.

```kotlin
// IndexingKafkaListener.kt — Kafka key(Producer 계약상 documentId) 그룹핑
@KafkaListener(topics = ["indexing"], id = "indexing")
fun onMessage(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment) {
    val futures = records
        .groupBy { it.key() }          // documentId
        .values
        .map { group -> executor.submit { group.forEach(::processRecord) } }
    futures.forEach { it.get() }
    ack.acknowledge()
}

// IndexingPipelineRunner.kt — 세부 로그/metric을 생략한 현재 제어 흐름
fun run(event: IndexingRequestedEvent, recordIdentity: KafkaRecordIdentity) {
    val jobId = acquireJobId(event, documentVersionId, recordIdentity) ?: return

    while (true) {
        val acquired = indexingJobRepository.start(jobId, workerId, maxAttempts)
        if (acquired != 1) {
            if (indexingJobRepository.failIfAttemptsExceeded(jobId, maxAttempts) == 1) return
            val job = indexingJobRepository.findById(jobId).orElse(null)
            if (job?.status == RETRY_WAIT && job.nextRetryAt != null) {
                waitUntilRetryDue(jobId, job.nextRetryAt) // DB CURRENT_TIMESTAMP 기준
                continue
            }
            return
        }
        try {
            val documentVersion = eventValidator.validate(event)
            processAcquiredJob(jobId, event, documentVersion)
            return
        } catch (e: Exception) {
            val failedAt = indexingJobRepository.currentDbTimestamp()
            val status = indexingFailureService.recordFailure(
                jobId, errorCode = errorCodeOf(e), permanent = !isRetryable(e),
                maxAttempts = maxAttempts, baseDelay = baseDelay, failedAt = failedAt,
            )
            if (status != RETRY_WAIT) return
            val nextRetryAt = indexingJobRepository.findById(jobId).orElseThrow().nextRetryAt!!
            waitUntilRetryDue(jobId, nextRetryAt, e)
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

- `RETRY_WAIT` 상태값 + `next_retry_at` — 인라인 대기 시각을 저장하고 관측에도 사용하는 실행 제어 정보다. 별도 poller가 이 상태를 조회해 깨우지는 않는다.
- `start()`의 재획득 조건에 `RETRY_WAIT AND next_retry_at <= CURRENT_TIMESTAMP` 포함 — due 전에는 0행이므로 Runner가 DB 시각으로 남은 시간을 기다리고 다시 호출한다.
- 동일 record가 `PROCESSING` 상태에서 재전달되면 `attempt_count` 상한 안에서 재획득한다. 원인이 consumer crash인지 nack인지 리밸런스인지는 구분하지 않는다.
- Kafka identity는 `acquireJobId()`에서 검증한다. `start()`는 현재 우회 production 경로가 없으므로 identity 조건을 중복해서 넣지 않는다.

**주의**: 임베딩 전용 timeout이 없어 인라인 재시도 전체가 900초 poll 예산 안에 든다는 보장은 아직 없다(P0-2/P0-3).

**완료 조건 — 코드 구현 완료, 실환경 회귀 검증 잔여**

- [x] `IndexingRetryScheduler`, `RetryEventSource`, `findRetryWaitDue()` 코드베이스에서 제거됨
- [x] 배치 리스너 + Kafka key 그룹화 + 배치 전체 완료 후 단일 ack 구현됨
- [x] 동일 record와 새 record 중복 발행을 topic/partition/offset으로 구분함
- [x] 동일 record의 due 전 `RETRY_WAIT`은 정상 종료하지 않고 대기 후 재획득함
- [ ] (회귀 확인 필요) 인라인 백오프 sleep 중 `docker kill` → 리밸런스 → 재전달 → 다른 워커가 처음부터 재처리 → 완료, `indexing_job`에 `PROCESSING`으로 남는 행 없음(`FAULT_TOLERANCE.md` §5.3) — 코드는 있으나 이 시나리오의 실측 검증 여부는 별도 확인 필요
- [ ] (회귀 확인 필요) 임베딩 2회 실패 후 성공 시 `attempt_count = 3`, `COMPLETED`, 추가 Kafka 메시지 발행 없음(§5.1)

---

### P0-5. 컨슈머 게이트 + `nack` — ✅ 구현 완료

**막는 장애**: DB 다운 중 메시지 소각(B-Gap-10).

`processRecord()`는 `DataAccessException`을 삼키지 않고 future 밖으로 전파한다. `onMessage()`는 DB 실패를 수집하면서 다른 그룹의 future도 모두 기다린 뒤 배치 전체를 nack한다. 이미 성공한 그룹도 재전달될 수 있으나 Kafka identity 판정과 DB 멱등 장치로 수렴한다.

**배치 리스너 기준 주의**: 리스너가 배치로 전환됐으므로(P0-4 완료) `nack`은 레코드 하나가 아니라 **배치 전체** 단위로 걸린다. 배치 안 한 레코드에서 DB 장애가 나면, 같은 배치의 다른 문서(정상 처리 가능했던 Job들)까지 통째로 되감기는 트레이드오프를 감수해야 한다.

**(a) DB 장애는 ack이 아니라 `nack`**

```kotlin
ack.nack(0, Duration.ofSeconds(5))
// 배치 index 0부터 되감고 5초 뒤 다시 poll한다.
```

**(b) 컨테이너 pause로 hot loop 방지**

```kotlin
@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
    private val meterRegistry: MeterRegistry,
) {
    @Scheduled(fixedDelay = 5_000)
    fun check() {
        val healthy = runCatching { jdbcTemplate.queryForObject("SELECT 1", Int::class.java) }.isSuccess
        val container = registry.getListenerContainer("indexing") ?: return
        when {
            !healthy && container.isRunning && !container.isPauseRequested -> {
                container.pause()
                meterRegistry.counter("db_health_gate_paused_total").increment()
                log.warn("DB down — consumer paused")
            }
            healthy && container.isPauseRequested -> { container.resume(); log.info("DB up — consumer resumed") }
        }
    }
}
```

`pause()`는 consumer의 파티션 fetch를 멈추되 컨테이너 poll/heartbeat는 유지한다. Health gate는 nack에서 호출되지 않고 5초 fixed delay 스케줄로 독립 실행된다.

**(c) DB 실패 정책**: DB에 아무것도 기록하지 못한 실패는 ack하지 않고 `nack`한다. `Error`처럼 `Exception` 밖의 치명 오류도 ack되지 않지만 이는 정상 복구 정책이 아니라 컨테이너로 전파되는 실패 경계다.

**완료 조건**

- [x] 리스너까지 이탈한 `DataAccessException`을 batch `nack(0, delay)`로 처리
- [x] nack 전 다른 documentId 그룹 future도 모두 기다림
- [x] DB health gate가 pause/resume하고 pause 전이를 metric으로 기록
- [ ] `docker stop postgres` → 30초 후 `docker start postgres` → `nack` → pause → 복구 후 resume → 전부 처리, Kafka lag 원복, 유령 `PROCESSING` 행 없음(§5.5)
- [ ] `db_health_gate_paused_total`과 실제 pause/resume 동작을 통합 환경에서 확인

---

### P1-1. 진행 상태 저장 + 폴링 API — Worker 일부 완료

**막는 장애**: 사용자가 실패한 최신 버전을 인지하지 못함(B-Gap-7), `FAILED` 종결을 아무도 모름(B-Gap-8).

**스키마**: §2의 `phase` 컬럼 하나만 둔다. `total_chunks`/`processed_chunks` 같은 청크 단위 진행률은 두지 않는다. 현재 Worker가 쓰는 값은 `DOWNLOADING`/`PARSING`/`CHUNKING`/`EMBEDDING` 네 가지이며 `PUBLISHING`은 아직 쓰지 않는다.

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
| `PUBLISHING` | 임베딩 완료 + UPSERT/fencing 비교 시작 직전 | 미구현. `EmbeddingIndexingProcessor` 또는 publication 진입점에서 추가 필요 |

인라인 재시도(P0-4)로 같은 Job이 여러 번 `start()`를 타면 실제 재실행이 시작될 때 `phase`를 `DOWNLOADING`으로 다시 쓴다. 다만 `RETRY_WAIT`으로 sleep하는 동안에는 실패 직전 phase가 남아 있으므로, 외부 API가 phase만 보고 “현재 그 단계를 실행 중”이라고 단정하면 안 된다.

**동작 규칙**

- 버전 목록 응답에 `searchable: boolean` + `indexingStatus` 노출 — "최신 버전을 올렸지만 실패해서 이전 버전이 검색된다"는 상태를 보이게 함.
- `FAILED` 상태는 재인덱싱 버튼과 함께 노출.

**완료 조건**

- [x] `IndexingPipelineRunner`가 `DOWNLOADING`/`PARSING`/`CHUNKING`/`EMBEDDING` 진입 직전 `phase` UPDATE
- [ ] publication 진입 직전 `PUBLISHING` UPDATE
- [x] 재시도 실제 실행 시작 시 `phase`를 `DOWNLOADING`으로 갱신
- [ ] v3 파싱 실패 후 버전 목록 API가 `searchable: false`(v3), 검색은 여전히 v2 콘텐츠를 반환함을 확인
- [ ] API 서버의 폴링 조회가 `phase`를 정확히 반영함

---

### P1-2. 메트릭 + 알림

| 메트릭 | 타입 | 알림 기준 | 의미 |
|---|---|---|---|
| `indexing_job_failed_total{errorCode}` | Counter | 급증 시 | DLQ 없이 실패를 감지하는 핵심 지표. `WHERE status='FAILED'`와 대응 |
| `kafka_rebalance_total` | Counter | 즉시 | 재시도 예산이 poll 간격을 넘고 있음(P0-3 실패 신호) |
| `indexing_inline_retry_total{attempt}` | Counter | 급증 시 | 어느 시도에서 성공/실패하는지, 예산 튜닝 근거 |
| `indexing_job_duration_seconds{phase=total}` | Timer | p99 감시 | Job 획득 뒤 최종 반환까지, retry sleep 포함 |
| `kafka_consumer_lag{partition}` | Gauge | 지속 증가 | 애플리케이션 custom metric은 없음. Kafka/Micrometer/운영 수집 계층에서 제공 필요 |
| `db_health_gate_paused_total` | Counter | 즉시 | DB가 흔들리고 있음 |
| `parse_thread_leaked` | Gauge | 임계 초과 | 실제 생존 thread 수가 아니라 parsing timeout 취소 요청 누적 |

**핵심 지표**: `kafka_rebalance_total`, `indexing_job_duration_seconds` p99, `indexing_job_failed_total`이다. custom metric 계측 코드는 들어가 있지만 이 저장소에는 Prometheus export, dashboard, alert rule이 없다.

**완료 조건**

- [x] 실패·인라인 재시도·전체 Job 시간·DB pause·rebalance·parse timeout custom metric 계측
- [ ] consumer lag 수집 경로 확인
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
페일오버 발생 → DataAccessResourceFailureException → batch nack(5초)
             └→ 독립 health gate가 DB 실패를 감지하면 consumer pause
promote·재연결 완료 → health gate resume → 되돌려둔 메시지부터 재처리
```

정상 DB 실패 경계에서는 별도 고아 Job 회수가 필요 없다. ack되지 않은 Kafka record가 복구 입력이 된다. §0.2의 generic 비-DB 예외 경계는 이 설명의 예외다.

**선행**: P0-4, P0-5(재활용) + 인프라 선행(OpenSQL 클러스터 구성).

**측정 지표**

| 지표 | 측정 방법 |
|---|---|
| 데이터 유실 건수 | 페일오버 중 업로드한 N건 중 최종 완료 수 → N/N |
| RTO | Primary kill → 첫 정상 완료 |
| 컨슈머 복구 시간 | pause → resume |

---

### P2-2. 재시도 시각 기준 통일 — 핵심 경로 완료, 타입 통일 잔여

**막는 장애**: `next_retry_at`(애플리케이션 시각) vs `start()` 비교(DB 시각) 불일치, `LocalDateTime`/`TIMESTAMPTZ` 혼재(B-Gap-5).

현재 Runner는 `SELECT CURRENT_TIMESTAMP`로 실패 기준 시각을 가져오고 그 값에 Java `Duration`을 더해 `next_retry_at`을 저장한다. due 비교는 SQL `CURRENT_TIMESTAMP`, sleep 잔여 시간 계산도 다시 조회한 DB `CURRENT_TIMESTAMP`를 사용한다. 즉 계산식 자체는 애플리케이션에 있지만 기준 시각은 모두 DB로 통일되어 JVM/DB 시계 오차 문제는 막는다.

잔여 과제는 `LocalDateTime`/`TIMESTAMPTZ` 매핑을 `Instant` 또는 `OffsetDateTime`으로 통일하는 것이다. Worker 컨테이너 `TZ=UTC` 고정도 운영 환경에서 확인한다.

**완료 조건**

- [x] 실패 시각, due 비교, sleep 잔여 시간의 기준을 DB `CURRENT_TIMESTAMP`로 통일
- [ ] 코드베이스에서 `LocalDateTime` 사용처가 인덱싱 도메인에서 제거됨

---

### P2-3. 파티션 / 처리량 튜닝

**막는 장애**: `documentId` 키의 hot spot(B-Gap-6), 인라인 재시도가 이를 악화한다. 배치 점유 시간은 처리 시간에 따라 달라져 현재 고정 상한이 없다.

| 문제 | 대응 |
|---|---|
| 재시도 중인 Job이 배치 커밋을 붙잡음 | `max-poll-records`를 작게 유지 |
| 대용량 문서가 파티션 점유 | 파일 크기별 토픽 분리 |
| 특정 테넌트 독점 | 테넌트별 동시 처리 상한 |

**선행**: P1-2(실측 없이 미리 튜닝하지 않는다).

---

### P2-5. 비동기 경계 트레이싱

- API 서버: `SET LOCAL app.trace_id = :traceId` (스펙 §6.3, API 담당 요청 필요)
- 워커: MDC 세팅과 `indexing_job.trace_id` 저장은 구현됨. 모든 외부/DB 비동기 경계에서 유지되는지는 추가 검증 필요

리스너가 executor 작업 안에서 MDC를 설정하며 인라인 재시도는 같은 스레드에서 돌아 MDC가 유지된다.

**완료 조건**

- [x] record 처리 스레드 MDC 설정/해제와 `indexing_job.trace_id` 저장
- [ ] `trace_id`가 외부 호출과 최종 상태 기록까지 모든 로그 라인에 포함되는지 통합 검증

---

## 5. 제거된 컴포넌트 ✅ 완료 (`5f52bd5`)

| 대상 | 위치(참고) | 대체 | 상태 |
|---|---|---|---|
| `IndexingRetryScheduler` | Track A plan Task 17 | P0-4 인라인 루프 | 제거됨 |
| `RetryEventSource` | Track A plan Task 17 | 원본 메시지 재사용(리스너가 이미 들고 있음) | 제거됨 |
| `findRetryWaitDue()` 쿼리 | — | 폴링 대상 없음 | 제거됨 |

---

## 6. 구현 상태와 다음 순서 — 2026-08-20

| 순서 | 항목 | 현재 상태/다음 작업 |
|---|---|---|---|
| 완료 | Kafka identity + `insertIfAbsent()` 충돌 판정 | 같은 record 재전달/다른 record 중복 발행 구분, Lease 미사용 |
| 완료 | P0-1 에러 분류 | 영구 실패 화이트리스트와 임베딩 400/응답 검증 반영 |
| 부분 완료 | P0-2 리소스 가드 | 파일/파싱 방어는 완료. 임베딩 전용 timeout과 대용량 실측 필요 |
| 부분 완료 | P0-3 재시도 예산 | 5회·선형 backoff·900초 poll interval 완료. 전체 처리 상한 재산정 필요 |
| 완료 | P0-4 인라인 재시도 | poller 제거, due 전 재전달도 인라인 대기 |
| 완료 | P0-5 DB 장애 경계 | batch nack과 독립 DB health gate 구현. 실제 DB 중단 통합 검증 필요 |
| 1 | 통합/E2E 장애 시나리오 | §8의 Kafka/PostgreSQL 기반 정상·재시도·중복·crash·DB 장애 검증 |
| 2 | 임베딩 timeout/요청 크기 경계 | poll 예산의 처리 시간 상한 확보 |
| 3 | 관측 인프라 | metric export, dashboard, alert, consumer lag 수집 |
| 4 | 진행률/시각 타입 잔여 | `PUBLISHING`, 조회 API, `LocalDateTime` 제거 |
| 5 | 페일오버·처리량 튜닝 | 실측과 인프라 구성이 준비된 뒤 진행 |

---

## 7. 다른 담당자에게 요청할 것

### 7.1 임베딩/Publication 경로

- [x] HTTP 400을 `EmbeddingRequestRejectedException`으로 변환해 영구 실패 처리
- [x] 응답 개수·차원·유한값 불일치를 `InvalidEmbeddingException`으로 영구 실패 처리
- [x] 429/5xx/네트워크 오류는 원 예외를 전파해 인라인 재시도
- [ ] 임베딩 호출 전용 timeout과 요청 크기/분할 정책 확정
- [ ] UPSERT/fencing 시작 직전 `phase = 'PUBLISHING'` 갱신

### 7.2 스키마 / API 서버 담당

| 대상 | 요청 |
|---|---|
| `indexing_job` | `phase`, Kafka identity 컬럼은 V1에 반영 완료. 운영 DB 적용 여부 확인 |
| 트랜잭션 | `SET LOCAL app.trace_id`(P2-5) |
| 재처리 API | 게이트 조건을 `status = 'FAILED'`로(스펙 §6.3, 이미 확정된 사항 재확인) |
| 진행률 API | `phase`와 `status`를 폴링 조회하도록 API 서버에서 구현 |

### 7.3 인프라 담당

- Kafka를 독립 배포 단위로(스펙 §6.5)
- OpenSQL 클러스터 + 페일오버 절차(P2-1)
- Toxiproxy를 개발 환경 compose에 포함
- 워커 컨테이너 `TZ=UTC` 고정(P2-2)
- custom metric export, dashboard, alert와 Kafka consumer lag 수집

---

## 8. 테스트 계획

테스트는 “같은 `eventId`인가”만 확인하지 않고 **Kafka record identity**, Job 최종 상태, attempt 수, ack/nack, chunk 수렴을 함께 검증해야 한다.

### 8.1 현재 자동화된 핵심 회귀 테스트

- 배치의 모든 record 처리 후 ack, 같은 Kafka key의 순차 처리, 삭제 handler 라우팅
- Jackson 역직렬화 실패·일반 예외는 다른 record를 계속 처리하고 최종 ack
- `DataAccessException`은 다른 그룹 future까지 기다린 뒤 `nack(0, delay)`
- listener가 `ConsumerRecord`의 topic/partition/offset을 Runner에 전달
- 같은 `eventId`/다른 offset은 중복 발행으로 무시
- 같은 topic/partition/offset의 `PROCESSING` Job은 재획득
- 같은 record의 terminal Job은 재실행하지 않음
- 같은 record의 due 전 `RETRY_WAIT`은 기다린 뒤 재획득
- 재시도 가능 오류가 성공하면 `COMPLETED`, 상한까지 실패하면 `FAILED`
- 영구 오류는 첫 시도에 `FAILED`

기본 `./gradlew test`는 `integration` tag를 제외한다. DB 상태 전이 통합 테스트는 `./gradlew integrationTest`로 별도 실행하며 로컬 PostgreSQL과 사전 schema가 필요하다. 현재 자동화 테스트는 실제 Kafka broker에서의 offset commit/rebalance와 worker process crash까지 검증하지 않는다.

### 8.2 추가할 전체 흐름/E2E 시나리오

| 시나리오 | 입력/장애 주입 | 필수 검증 |
|---|---|---|
| 정상 인덱싱 | 실제 Kafka에 `key=documentId`로 1건 발행 | Job `COMPLETED`, `attempt_count=1`, chunk 저장, searchable 승격, offset commit |
| 재시도 후 성공 | 임베딩 429/5xx/네트워크 오류 N회 후 성공 | `RETRY_WAIT`과 선형 대기, `attempt_count=N+1`, 최종 `COMPLETED`, sleep 중 offset 미커밋 |
| 재시도 상한 | 재시도 가능 오류를 5회 지속 | 최종 `FAILED`, 추가 sleep/재실행 없음, 배치는 최종 ack |
| 영구 실패 | hash 오류, 지원하지 않는 MIME, HTTP 400, 잘못된 vector | 첫 시도 `FAILED`, 인라인 retry 없음, 배치 ack |
| 동일 record 재전달 | 처리 중 worker kill 또는 강제 rebalance | 같은 topic/partition/offset으로 재전달, 기존 미완료 Job 재획득, 최종 결과 중복 없음 |
| 새 record 중복 발행 | 같은 `eventId`를 다른 offset에 다시 발행 | 기존 Job identity 불변, 두 번째 record에서 `start()`하지 않고 ack |
| consumer crash 원인 구분 | 이전 consumer 처리 중 process kill | 별도 플래그가 아니라 동일 record identity로 재전달 판정됨을 확인 |
| due 전 재전달 | `RETRY_WAIT.next_retry_at` 이전에 같은 record 전달 | 조기 ack 없이 남은 시간 대기 후 재획득 |
| DB 중단 | 배치 처리 중 PostgreSQL stop/start | batch nack, gate pause/resume, 복구 후 전건 수렴, 유령 `PROCESSING` 없음 |
| 치명 오류 반복 | 격리 환경에서 결정적 `Error` 주입 | ack되지 않으며 반복 재전달 가능성을 관측·경보. 운영 프로세스 전체를 불안정하게 하므로 일반 CI와 분리 |
| 삭제 | 인덱싱 뒤 `DOCUMENT_DELETED(documentVersionId=null)` | 특정 버전이 아니라 문서의 모든 chunk 삭제, 활성 Job 실패, 반복 삭제 멱등 |
| 동시 중복 실행 | 같은 record를 두 Worker가 경계 시점에 실행 | Lease 없이도 UPSERT/trailing DELETE/search fencing으로 최종 DB가 수렴 |

추가 테스트는 단위 테스트 대역만으로 끝내지 않고, 최소한 Kafka broker·PostgreSQL을 포함한 통합 환경에서 offset과 DB 상태를 함께 관찰해야 한다.

### 8.3 (회귀) 워커 크래시는 Kafka가 회수한다

```bash
docker kill -s SIGKILL worker-1   # 리스너가 처리 중일 때
```
**기대**: 리밸런스 → 다른 워커가 재처리 → 완료. 청크 중복 0건. (Track A 성과 유지 확인용 회귀 테스트)

### 8.4 카오스 테스트 (최종 성적표)

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
-- ★ 장애 주입과 retry due가 모두 끝났는데 PROCESSING/RETRY_WAIT이 남아 있으면 §1 목표 위반

-- ② 각 문서의 검색 버전에 청크가 정상적으로 남아있는가(참고용 — total_chunks 컬럼을 두지 않기로
--   했으므로 "저장돼야 할 개수"와의 자동 비교는 불가능하다. COMPLETED인데 청크가 0건인 문서가
--   있는지만 확인한다. 삭제되지 않은 문서의 COMPLETED 버전에 청크가 0건이면 버그다).
SELECT d.id, count(c.id) AS actual
FROM document d
JOIN document_version v ON v.id = d.searchable_version_id
JOIN indexing_job j ON j.document_version_id = v.id AND j.status = 'COMPLETED'
LEFT JOIN document_chunk c ON c.document_version_id = v.id
GROUP BY d.id;
```

**Definition of Done — 핵심 장애 복구 흐름**: 장애 주입과 모든 retry due가 끝난 뒤 `PROCESSING/RETRY_WAIT`이 0건이고, 입력한 각 record가 예상한 terminal 상태·offset commit 여부에 도달하며, 중복 실행 뒤에도 chunk unique key와 검색 버전 포인터가 기대값으로 수렴해야 한다. 운영 기능까지 포함한 Track B 전체 완료에는 §4의 잔여 체크박스도 별도로 닫아야 한다.

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
