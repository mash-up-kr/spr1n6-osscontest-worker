# Track A — 인덱싱 수집 파이프라인 설계 문서

> 이 문서는 "어떻게 여기까지 왔는지"가 아니라 **"지금 무엇이, 왜 이런 모양으로 존재하는지"**를
> 담는다. `IndexingProcessor`(임베딩 → 저장 → 검색 버전 승격, 이 문서 밖의 구현)를 만드는
> 쪽이 이 워커의 나머지 부분과 정확히 어떤 계약으로 연결되는지 이해하는 걸 목표로 쓴다.

## 1. 이 워커는 전체 시스템에서 어디에 있나

```
[사용자] --업로드--> [API 서버] --outbox_event--> [doc-relay] --Kafka produce--> [Kafka: "indexing" 토픽]
                                                                                        │
                                                                                        ▼
                                                                          [이 레포: 인덱싱 워커]
                                                                                        │
                                                                    consume → 파싱 → 청킹 → 임베딩 → 저장
                                                                                        │
                                                                                        ▼
                                                                          [PostgreSQL(pgvector): document_chunk]
```

**담당 경계는 정확히 Kafka 토픽이다.** `outbox_event` 폴링 → Kafka produce까지는
doc-relay(다른 담당자)가 하고, 이 워커는 Kafka에서 메시지를 consume하는 순간부터
시작한다.

**이 레포 안에도 다시 경계가 있다**: 다운로드 → 무결성 검증 → 파싱 → 청킹 →
`IndexingContext` 조립까지가 이 워커(Track A, 이 문서가 다루는 범위)고, 그 뒤
**임베딩 API 호출 → `document_chunk` 저장 → 검색 버전 승격은 `IndexingProcessor`**
(이 인터페이스의 실제 구현, 임베딩 담당)다. 이 경계는 코드 레벨에서 인터페이스
하나로 명확히 그어져 있다 — 아래 4장 참고.

## 2. 담당 경계 한눈에

| 항목 | 담당 |
|---|---|
| `document`/`document_version` 행 생성, `embedding_version_no` 예약 | API 서버 |
| `outbox_event` 폴링 → Kafka produce | doc-relay |
| Kafka consume → 파싱 → 청킹 → `IndexingContext` 조립 | **이 워커(Track A)** |
| 임베딩 API 호출, `document_chunk` UPSERT, `searchable_version_id` 승격 | **`IndexingProcessor`(임베딩 담당, 별도 구현)** |
| `document.deleted_at` 기록, 원본 파일 삭제, `document.purged_at` | API 서버 |
| 진행 중 Job 종결, `document_chunk` 삭제(삭제 이벤트 대응) | **이 워커(Track A)** |

## 3. 패키지 구조와 컴포넌트 책임

```
com.osscontest.worker.indexing
├── consumer/       Kafka 배치 수신, 이벤트 검증, eventType 라우팅
├── pipeline/        전체 처리 오케스트레이션(IndexingPipelineRunner) + 경계 타입
├── retrieval/       원문 다운로드(S3) + 무결성 검증
├── parsing/         원문 바이트 → 텍스트 블록(TXT/MD/PDF/DOCX/HWP)
├── chunking/        텍스트 블록 → 청크(3가지 전략) + 청크 유효성 가드
├── publication/     JPA 엔티티/리포지토리, 실패 기록 서비스
└── deletion/        DOCUMENT_DELETED 처리 + 정리 스윕
```

| 컴포넌트 | 역할 |
|---|---|
| `IndexingKafkaListener` | Kafka 배치 수신, `documentId`별 그룹핑·동시 처리, 배치 단위 ack |
| `IndexingEventValidator` | `INDEXING_REQUESTED` 스키마 버전 / `document_version` 존재·일치 검증 |
| `DocumentDeletionHandler` | `DOCUMENT_DELETED` 스키마 버전·테넌트 검증 후 위임 |
| `IndexingPipelineRunner` | Job 획득 → 검증 → 다운로드 → 파싱 → 청킹 → **`IndexingProcessor.process()` 호출** → 실패 시 인프로세스 재시도 |
| `DocumentDownloadClient`(S3 구현체) | 원문 다운로드 + SHA-256 무결성 검증(`content_hash`와 비교) |
| `DocumentParserRegistry` + 파서 4종 | mimeType별 파서 라우팅, 원문 → `ParsedBlock` 목록 |
| `ChunkingService` + 전략 3종(`FIXED_TOKEN`/`PARAGRAPH`/`PARAGRAPH_OVERLAP`) | 블록 → `Chunk` 목록 (설정으로 전략 전환 가능, 기본값 `FIXED_TOKEN`) |
| `ChunkGuard` | 빈 청크 차단, 청크 개수 상한, 토큰 총합 상한 검증 |
| `IndexingFailureService` | 실패를 `RETRY_WAIT`/`FAILED`로 분류해 DB에 기록, 선형 백오프 계산 |
| **`IndexingProcessor`** | **이 인터페이스만 이 레포에 있다 — 구현은 임베딩 담당** |
| `DocumentDeletionService` + `DocumentDeletionSweepScheduler` | 삭제된 문서의 Job 종결 + 청크 정리(이벤트 경로 + 60초 백업 스윕) |

## 4. `IndexingProcessor` — 이 문서에서 가장 중요한 부분

임베딩 담당이 실제로 구현해야 하는 계약은 이거 하나다:

```kotlin
interface IndexingProcessor {
    fun process(context: IndexingContext, chunks: List<Chunk>)
}
```

```kotlin
data class IndexingContext(
    val jobId: Long,
    val documentId: Long,
    val documentVersionId: Long,
    val versionNo: Long,
    val extractedMetadata: Map<String, Any>?,
)

data class Chunk(
    val chunkNo: Int,
    val content: String,
    val contentHash: String,
    val tokenCount: Int?,
    val pageFrom: Int?,
    val pageTo: Int?,
    val sectionPath: String?,
    val metadata: Map<String, Any>?,
)
```

### `process()`가 해야 하는 일 (Track A 쪽 코드가 그렇게 가정하고 짜여 있음)

1. `chunks`의 `content`를 임베딩 API에 넘겨 벡터를 받는다.
2. `document_chunk`에 `(document_version_id, chunk_no)` UNIQUE 기준으로 UPSERT 저장한다.
3. **trailing DELETE**: 이전 실행보다 청크 수가 줄었으면(재청킹 등), 이번에 안 쓴
   `chunk_no`의 잔존 행을 지운다 — 안 그러면 옛날 청크가 유령처럼 검색에 남는다.
4. `document_version.embedding_version_no`와 현재 `document.searchable_version_id`가
   가리키는 버전의 `embedding_version_no`를 비교해서, **더 큰 쪽만** 승격한다
   (`document.deleted_at IS NULL`인 경우에만 — 삭제된 문서는 절대 승격 금지).
5. 실패하면 그냥 예외를 던지면 된다 — 아래 "실패 시 계약" 참고.

### 실패 시 계약 — 이게 없으면 재시도 분류가 깨진다

`IndexingPipelineRunner`는 `process()`가 던진 예외를 받아서 **재시도 가능 여부를
화이트리스트로 판정**한다(`isRetryable`). 이 화이트리스트에 없는 예외는 전부
"재시도 가능"으로 기본 취급되므로, **임베딩 API의 일시적 오류(타임아웃, 429, 5xx
등)는 그냥 평범한 `Exception`/`RuntimeException`을 던지면 자동으로 재시도 대상이
된다** — Track A 쪽 코드를 고칠 필요가 없다.

반대로 "다시 시도해도 결과가 똑같은" 영구 실패(예: 임베딩 모델이 이 콘텐츠를 정책
위반으로 영구 거부하는 경우)가 있다면, 그건 Track A의 `IndexingPipelineRunner.
isRetryable()` 화이트리스트에 그 예외 타입을 추가해야 즉시 `FAILED`로 종결된다 —
이건 이 파일을 같이 고쳐야 하는 유일한 지점이니, 그런 예외 타입이 생기면 알려주면
좋겠다.

`indexing_job.status`를 직접 건드릴 필요는 없다 — 실패든 성공이든 호출자
(`IndexingPipelineRunner`)가 알아서 기록한다. `process()`가 자체적으로 뭔가
기록해도(예: 부분 성공 흔적), 호출자의 재기록 로직에 `status != PROCESSING이면
조용히 무시` 가드가 있어서 충돌 없이 안전하다.

## 5. 핵심 흐름 3가지 (요약 — 상세 diff/코드는 소스 참고)

### 5-1. 정상 처리

Kafka 배치 수신 → `documentId`로 그룹핑 → 그룹별 동시 처리(그룹 안은 순차) →
`IndexingPipelineRunner.run()`이 Job 획득 → 검증 → 다운로드/해시 검증 → 파싱 →
청킹 → 가드 → `IndexingProcessor.process()` 호출 → 배치 전체가 끝나야 한 번에 ack.

**"이미 최신 버전이 검색에 노출돼 있어도 끝까지 처리한다"** — 예전엔 조기
스킵 로직이 있었지만, "이전 버전으로 되돌리기" 기능이 그 버전의 청크가 실제로
저장돼 있어야 성립하기 때문에 없앴다. 검색 노출 여부는 `IndexingPipelineRunner`가
판단하지 않고, `IndexingProcessor`의 승격 UPDATE가 마지막에 판단한다.

### 5-2. 실패 / 재시도

`process()`가 예외를 던지면 → `isRetryable()`로 분류 → 재시도 가능이면
`RETRY_WAIT` 기록 후 `Thread.sleep(base-delay * attempt_count)`(선형 백오프) →
같은 스레드에서 `run()` 루프 재진입 → 상한(`max-attempts`, 기본 5) 도달 시
`FAILED`로 종결. 이 재시도는 완전히 인프로세스다 — 별도 스케줄러나 DB 폴링이
없다. 한 문서의 재시도 대기는 같은 배치 안 다른 문서 그룹을 막지 않는다(다른
스레드에서 독립 처리).

### 5-3. 문서 삭제

`DOCUMENT_DELETED` 이벤트(같은 토픽, 같은 파티션) → 스키마/테넌트 검증 →
진행 중 Job을 `FAILED(DOCUMENT_DELETED)`로 종결 → `document_chunk` 삭제.
이벤트가 유실돼도 60초마다 도는 스윕이 "삭제됐는데 청크가 남은 문서"를 찾아
같은 함수로 재실행한다 — 별도 완료 마커 없이 "청크가 남아있는가" 자체가 멱등한
"아직 정리 안 됨" 마커다.

## 6. 설정 값 총정리

```yaml
indexing:
  consumer:
    supported-schema-versions: 1        # INDEXING_SUPPORTED_SCHEMA_VERSIONS
    concurrency: 5                       # 배치 안 documentId 그룹 동시 처리 스레드 수
  retry:
    max-attempts: 5                      # INDEXING_MAX_ATTEMPTS
    base-delay: PT30S                    # 선형 백오프 기준 간격
  chunking:
    strategy: FIXED_TOKEN                # FIXED_TOKEN | PARAGRAPH | PARAGRAPH_OVERLAP
    max-chunks-per-document: 5000
    max-total-tokens: 2000000
  storage:
    bucket: (필수, 미설정 시 다운로드가 런타임에 실패)
    endpoint: ""                         # MinIO 등 커스텀 엔드포인트용
    region: us-east-1
  deletion:
    sweep-interval-ms: 60000
    batch-size: 50
    initial-delay-ms: 60000              # 앱 기동 직후 즉시 스윕이 도는 걸 방지

spring:
  kafka:
    consumer:
      max-poll-records: 10               # INDEXING_BATCH_SIZE, 배치 크기
      properties:
        max.poll.interval.ms: 600000     # 배치(재시도 포함) 처리가 이 안에 끝나야 함
    listener:
      ack-mode: manual
      type: batch
```

## 7. 관련 DB 테이블 (스키마 자체는 API 서버/마이그레이션 브랜치 소유, 참고용)

- `document`: `tenant_id`, `latest_version_no`, `searchable_version_id`, `deleted_at`, `purged_at`
- `document_version`: `document_id`, `version_no`, `source_object_key`, `mime_type`, `content_hash`, `embedding_version_no`
- `indexing_job`: `source_event_id`(UNIQUE), `document_id`, `document_version_id`, `status`, `attempt_count`, `next_retry_at`, `last_error_code`, `last_error_message`
- `document_chunk`: `document_version_id`, `chunk_no`, `content`, `embedding`(VECTOR), `(document_version_id, chunk_no)` UNIQUE

## 8. 통합 전 확인해야 할 것 (머지 시 체크리스트)

- `Chunk`, `IndexingContext`, `IndexingJobStatus`, `IndexingProcessor` — 이 4개
  타입이 양쪽 브랜치에 중복 존재한다. 이 레포 쪽 걸 지우고 실제 구현 쪽을 쓰면 된다
  (필드 모양은 동일하게 맞춰뒀다). `IndexingJobStatus`는 이 레포에 `STALE`이 없는데,
  이건 의도된 차이다(별도 상태 없이 fencing 비교로만 판정) — 지워도 되는지만 확인.
- `process()` 내부에서 `indexingJobRepository.start()`류의 Job 재획득을 별도로
  호출하지 않는지 확인 필요 — `IndexingPipelineRunner.run()`이 이미 Job을 획득한
  뒤에 `process()`를 부르므로, 내부에서 한 번 더 획득 시도를 하면 `attempt_count`가
  중복 증가할 수 있다.
- `indexing.retry.max-attempts`/`base-delay` 같은 설정 키 이름이 두 브랜치에서
  다르게 붙어 있을 수 있으니 병합 전에 하나로 맞추는 게 좋다.
- 로컬 검증은 Docker Postgres(pgvector)로 통합 테스트까지 했지만, 실제 배포용
  Kafka 토픽/파티션 설정과 S3 버킷은 아직 실환경에서 맞춰보지 않았다.
