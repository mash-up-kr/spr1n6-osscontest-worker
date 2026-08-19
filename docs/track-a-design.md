# Indexing 패키지 구조와 확장 규칙

## 1. 문서 목적

이 문서는 `com.osscontest.worker.indexing` 아래의 패키지 책임과 의존 방향을 정의한다.

새 기능을 추가하는 개발자나 AI는 파일을 만들기 전에 이 문서를 기준으로 위치를 결정한다.
특히 임베딩 관련 코드를 추가할 때 그 코드를 `pipeline`, `chunking`, `parsing`에 넣지
않도록 하는 것이 목적이다.

현재 Worker는 Kafka 수신부터 파싱·청킹까지는 전부 구현되어 있다. 임베딩 호출과
`document_chunk` 저장, 검색 버전 승격을 담당하는 `IndexingProcessor`는 계약(인터페이스)만
정의되어 있고, 실제 구현은 이 레포 밖(별도 담당자)에서 이루어진다.

---

## 2. 기본 원칙

최상위 패키지를 controller, service, repository 같은 기술 계층으로 나누지 않는다.
인덱싱 단계의 기능을 먼저 나누는 feature-first 구조를 사용한다.

```text
indexing
├── consumer
├── pipeline
├── retrieval
├── parsing
├── chunking
├── publication
└── deletion
```

각 기능 내부에서 필요한 경우에만 `domain`, `service`, `entity`, `repository`,
`config`로 나눈다.

`pipeline`은 기능 구현 패키지가 아니다. 여러 기능의 호출 순서와 실패 흐름을 조율하는
패키지다.

---

## 3. 현재 패키지 구조

```text
com.osscontest.worker.indexing
├── consumer
│   ├── IndexingKafkaListener.kt          # 배치 수신, documentId 그룹핑, 동시 처리, ack
│   ├── IndexingBatchExecutorConfig.kt    # 그룹 동시 처리용 고정 스레드풀
│   ├── IndexingEventValidator.kt         # INDEXING_REQUESTED 스키마/문서버전 검증
│   ├── DocumentDeletionHandler.kt        # DOCUMENT_DELETED 스키마/테넌트 검증 후 위임
│   ├── IndexingRequestedEvent.kt
│   ├── InvalidEventException.kt
│   └── DeserializationException.kt
│
├── pipeline
│   ├── domain
│   │   ├── IndexingContext.kt
│   │   └── IndexingJobStatus.kt
│   └── service
│       ├── IndexingPipelineRunner.kt     # 전체 흐름 조율 + 인프로세스 재시도 루프
│       └── IndexingProcessor.kt          # 인터페이스만 — 구현은 이 레포 밖
│
├── retrieval
│   ├── DocumentDownloadClient.kt
│   ├── S3DocumentDownloadClient.kt
│   ├── ContentIntegrityException.kt
│   └── config
│       ├── S3ClientConfiguration.kt
│       └── StorageProperties.kt
│
├── parsing
│   ├── domain
│   │   ├── BlockType.kt
│   │   └── ParsedBlock.kt
│   ├── DocumentParser.kt                 # SPI
│   ├── DocumentParserRegistry.kt
│   ├── TextDocumentParser.kt
│   ├── PdfDocumentParser.kt
│   ├── DocxDocumentParser.kt
│   ├── HwpDocumentParser.kt
│   └── UnsupportedMimeTypeException.kt
│
├── chunking
│   ├── domain
│   │   └── Chunk.kt
│   ├── config
│   │   └── ChunkingProperties.kt
│   └── service
│       ├── ChunkingService.kt            # 전략 dispatch
│       ├── ChunkingStrategy.kt           # FIXED_TOKEN | PARAGRAPH | PARAGRAPH_OVERLAP
│       ├── FixedTokenChunker.kt
│       ├── ParagraphChunker.kt
│       ├── ParagraphOverlapChunker.kt
│       ├── ChunkerTokenizer.kt           # 3개 전략이 공유하는 jtokkit 유틸
│       ├── ChunkGuard.kt
│       ├── EmptyExtractionException.kt
│       ├── ChunkLimitExceededException.kt
│       └── TotalTokenLimitExceededException.kt
│
├── publication
│   ├── entity
│   │   ├── DocumentEntity.kt
│   │   ├── DocumentVersionEntity.kt
│   │   ├── IndexingJobEntity.kt
│   │   └── DocumentChunkEntity.kt        # id만 있는 최소 매핑, 삭제 전용
│   ├── repository
│   │   ├── DocumentRepository.kt
│   │   ├── DocumentVersionRepository.kt
│   │   ├── IndexingJobRepository.kt
│   │   └── DocumentChunkRepository.kt
│   └── service
│       └── IndexingFailureService.kt     # 실패 분류 결과를 RETRY_WAIT/FAILED로 기록
│
└── deletion
    ├── DocumentDeletionSweepScheduler.kt # 삭제 이벤트 유실 대비 백업 스윕
    └── service
        └── DocumentDeletionService.kt
```

`chunking.domain`에는 `Chunk` 하나만 있는 게 아니라 `ChunkingService`와 3개 구현
전략이 이미 다 존재한다 — 청킹은 이 Worker 안에서 완결되는 기능이고, 다른 Worker나
상위 단계로부터 전달받는 계약이 아니다. `publication.entity.DocumentChunkEntity`는
반대로 최소 매핑이다 — `document_chunk` 저장은 이 Worker의 책임이 아니라
`IndexingProcessor`(레포 밖) 책임이므로, 이 Entity는 삭제 쿼리(`DELETE FROM
document_chunk WHERE document_id = :id`) 하나를 위해서만 존재한다.

---

## 4. 패키지별 책임

### `consumer`

Kafka 배치 수신, 이벤트 파싱/검증, `eventType`에 따른 라우팅을 담당한다.

`IndexingKafkaListener`의 책임은 다음과 같다.

1. 배치 레코드를 `documentId`(메시지 key)로 그룹핑
2. 그룹마다 스레드풀에 제출해 동시 처리, 그룹 안은 원래 순서 그대로 순차 처리
3. `eventType == "INDEXING_REQUESTED"` → `IndexingPipelineRunner.run()`
4. `eventType == "DOCUMENT_DELETED"` → `DocumentDeletionHandler.handle()`
5. 모든 그룹의 처리가 끝나야 배치 전체를 한 번만 ack

`consumer`에 들어가면 안 되는 코드는 다음과 같다.

- 다운로드/파싱/청킹 알고리즘
- `indexing_job` 상태 전이 로직(획득, 재시도 판단)
- 임베딩 호출 관련 코드

### `pipeline`

전체 인덱싱 처리 순서를 조율한다. 유일하게 여러 기능 패키지(`retrieval`, `parsing`,
`chunking`, `publication`)를 직접 호출하는 곳이다.

`IndexingPipelineRunner.run()`의 책임은 다음과 같다.

1. `indexing_job` 행 확보(`acquireJobId`) 및 처리 권한 획득(`start`)
2. 이벤트 검증(`IndexingEventValidator`) + 테넌트 일치 확인
3. 원문 다운로드 + 해시 검증(`retrieval`)
4. 파싱(`parsing`) → 청킹(`chunking`) → 청크 유효성 검증(`ChunkGuard`)
5. `IndexingContext` 조립 후 `IndexingProcessor.process()` 호출
6. 예외 발생 시 영구/재시도 분류 → `IndexingFailureService.recordFailure()` 호출 →
   재시도 가능이면 `Thread.sleep` 후 같은 함수 안에서 재시도, 상한 도달 시 종결

`pipeline`에 들어가면 안 되는 코드는 다음과 같다.

- 문서 형식별 파싱 코드
- chunk size, overlap, token 분할 알고리즘
- 임베딩 API 호출 구현
- `document_chunk`에 대한 JPA Entity와 저장 쿼리

### `retrieval`

원문 바이트를 가져오고 무결성을 검증하는 기능의 소유자다.

- `DocumentDownloadClient`: S3에서 원문 다운로드
- SHA-256 계산 후 `document_version.content_hash`와 비교, 불일치 시
  `ContentIntegrityException`(영구 실패로 분류됨)

`retrieval`은 원본 파일을 **읽기만** 한다. 삭제 메서드는 의도적으로 없다 — 원본 파일
삭제는 API 서버 책임이다(§6 참고).

### `parsing`

원문 바이트를 텍스트 블록(`ParsedBlock`) 목록으로 변환하는 기능의 소유자다.
mimeType별로 파서를 등록하고(`DocumentParserRegistry`), 등록되지 않은 형식은
`UnsupportedMimeTypeException`(영구 실패)을 던진다.

`ParsedBlock`은 다음 데이터를 표현한다.

```text
order
type (HEADING/PARAGRAPH/TABLE/LIST)
text
pageNo
headingPath
```

`parsing`은 청킹 알고리즘이나 JPA Entity를 알지 않는다. 텍스트와 구조 메타데이터를
생성하는 것까지만 담당한다.

### `chunking`

파싱된 블록을 임베딩에 사용할 청크 목록으로 변환하는 기능의 소유자다.

```text
chunkNo
content
contentHash
tokenCount
pageFrom / pageTo
sectionPath
metadata
```

`ChunkingService`가 전략(`FIXED_TOKEN`/`PARAGRAPH`/`PARAGRAPH_OVERLAP`, 설정
`indexing.chunking.strategy`)에 따라 3개 구현 중 하나로 dispatch한다. `ChunkGuard`가
빈 청크·청크 개수 상한·토큰 총합 상한을 검증해 초과 시 예외(전부 영구 실패로 분류)를
던진다.

`Chunk`는 임베딩 호출 메서드나 JPA 매핑을 갖지 않는다. 청킹 기능은 문자열과 청크
메타데이터를 생성하는 것까지만 담당한다.

### `publication`

`indexing_job`/`document`/`document_version` 상태를 관리하고, 실패를 DB에 기록한다.

```text
IndexingFailureService.recordFailure(jobId, errorCode, permanent, ...)
→ permanent=true 또는 attempt_count >= max-attempts  → FAILED
→ 그 외                                                → RETRY_WAIT (선형 백오프로 next_retry_at 계산)
```

`document_chunk` 자체의 UPSERT/저장 로직은 여기 없다 — 그건 `IndexingProcessor`
(레포 밖) 책임이다. 이 패키지의 `DocumentChunkRepository`는 **삭제** 전용이다.

### `deletion`

`DOCUMENT_DELETED` 이벤트에 대응해 이 Worker가 진행 중이던 작업을 정리한다. 담당
범위는 좁다 — 원본 파일 삭제와 `document.purged_at`은 API 서버 책임이라 여기서
다루지 않는다(§6).

```text
DocumentDeletionService.handleDocumentDeleted(documentId)
→ 활성 indexing_job(PENDING/PROCESSING/RETRY_WAIT)을 FAILED로 종결
→ document_chunk 삭제
```

이벤트 경로가 유실돼도 `DocumentDeletionSweepScheduler`가 주기적으로 "삭제됐는데
chunk가 남은 문서"를 찾아 같은 함수를 재실행한다 — 별도 완료 마커 없이 멱등하게
수렴한다.

---

## 5. 현재 실행 흐름

### 5-1. 정상 처리

```text
Kafka 배치 수신 (documentId 키)
        ↓
IndexingKafkaListener — documentId로 groupBy, 그룹별 동시 처리
        ↓
IndexingPipelineRunner.run(event)
        ├─ acquireJobId + start() — indexing_job 확보, attempt_count 증가
        ├─ IndexingEventValidator.validate() + 테넌트 검증
        ├─ DocumentDownloadClient.download() + SHA-256 검증
        ├─ DocumentParserRegistry → parser.parse() → List<ParsedBlock>
        ├─ ChunkingService.chunk(blocks, strategy) → List<Chunk>
        ├─ ChunkGuard.assertValid(chunks)
        └─ IndexingProcessor.process(IndexingContext, chunks)  ← 레포 밖 구현
        ↓
배치 내 모든 그룹 완료 → 배치 전체 ack (1회)
```

### 5-2. 실패 / 재시도

`IndexingProcessor.process()`를 포함해 어느 단계에서든 예외가 발생하면
`IndexingPipelineRunner`가 잡아서 영구/재시도 여부를 화이트리스트로 판정한다.
재시도 가능이면 같은 함수 안에서 `Thread.sleep` 후 재진입한다(별도 스케줄러 없음).

```text
예외 발생
   ↓
isRetryable(e)?
   ├─ false(영구) ────────────────────────────→ FAILED 즉시 종결
   └─ true(재시도 가능, 기본값)
        ↓
      attempt_count >= max-attempts?
        ├─ 예 ──────────────────────────────→ FAILED 종결
        └─ 아니오 → RETRY_WAIT 기록 → next_retry_at까지 Thread.sleep
                     → run() 루프 재진입 (같은 스레드, 같은 documentId 그룹)
```

### 5-3. 문서 삭제

```text
DOCUMENT_DELETED 이벤트 (같은 토픽·파티션, documentId 키)
        ↓
DocumentDeletionHandler — 스키마 버전 + 테넌트 검증
        ↓
DocumentDeletionService.handleDocumentDeleted(documentId)
        ├─ 활성 indexing_job → FAILED
        └─ document_chunk 삭제
```

원본 파일 삭제·`document.purged_at`은 API 서버가 별도로 처리한다.

---

## 6. `IndexingProcessor`를 구현하는 방법

`IndexingProcessor`는 이 레포에 인터페이스만 존재한다. 실제 구현은 별도 브랜치/담당자가
작성하며, `pipeline.service` 패키지 **밖**에 있어야 한다(구현체가 이 레포에 들어온다면
`embedding`/`publication` 같은 별도 최상위 패키지를 새로 만든다).

```kotlin
interface IndexingProcessor {
    fun process(context: IndexingContext, chunks: List<Chunk>)
}

data class IndexingContext(
    val jobId: Long,
    val documentId: Long,
    val documentVersionId: Long,
    val versionNo: Long,
    val extractedMetadata: Map<String, Any>?,
)
```

`process()` 구현체가 담당해야 하는 일:

1. `chunks[].content`를 임베딩 API에 전달해 벡터를 받는다.
2. `document_chunk`에 `(document_version_id, chunk_no)` 기준 UPSERT로 저장한다.
3. `document_version.embedding_version_no`와 현재 `searchable_version_id`가 가리키는
   버전의 값을 비교해, **더 큰 쪽만** `document.searchable_version_id`로 승격한다
   (`document.deleted_at IS NULL`인 경우에만).
4. 실패하면 예외를 던지기만 하면 된다 — `indexing_job` 상태를 직접 갱신할 필요 없다.
   호출자(`IndexingPipelineRunner`)가 모든 예외에 대해 재시도 분류와 DB 기록을 대신
   한다.

`IndexingPipelineRunner.isRetryable()`은 화이트리스트 방식이다 — 여기 나열되지 않은
예외(임베딩 API 타임아웃, 5xx, 429 등)는 기본적으로 재시도 가능으로 자동 분류되므로,
일시적 오류는 그냥 평범한 예외를 던지면 된다. "다시 시도해도 결과가 항상 같은" 영구
실패 타입이 새로 필요해지면 그때 `isRetryable()`에 한 줄 추가한다 — 이 파일을 고치는
게 임베딩 쪽에서 `pipeline`을 침범하는 유일하게 정당한 경우다.

예상되는 연결 형태:

```kotlin
val chunks: List<Chunk> = chunkingService.chunk(blocks, strategy)
chunkGuard.assertValid(chunks)

indexingProcessor.process(
    context = indexingContext,
    chunks = chunks,
)
```

---

## 7. 의존 방향

의존은 조율자인 `pipeline`에서 각 기능으로 향한다.

```text
pipeline
├── consumer.IndexingRequestedEvent / InvalidEventException
├── retrieval
├── parsing
├── chunking
├── publication
└── pipeline.service.IndexingProcessor (인터페이스, 구현은 레포 밖)
```

기능 패키지끼리는 불필요하게 서로 의존하지 않는다.

```text
retrieval  -X→ parsing
parsing    -X→ chunking
chunking   -X→ publication
publication -X→ chunking
deletion   -X→ pipeline (deletion은 publication.repository만 직접 씀)
```

구체적인 규칙은 다음과 같다.

- `Chunk`에 임베딩 호출 메서드를 추가하지 않는다.
- `Chunk`나 `ParsedBlock`에 JPA 매핑 책임을 추가하지 않는다.
- `DocumentDownloadClient`에 삭제/쓰기 메서드를 추가하지 않는다(§6, 읽기 전용 유지).
- `IndexingFailureService`에서 청킹이나 파싱을 호출하지 않는다.
- 단계 간 데이터 결합과 호출 순서는 `pipeline`에서 처리한다.
- `IndexingProcessor` 구현체 안에서 `indexing_job` 재획득(`start()` 재호출)을 하지
  않는다 — `IndexingPipelineRunner`가 이미 획득한 뒤 호출하므로, 내부에서 한 번 더
  획득하면 `attempt_count`가 중복 증가한다.

---

## 8. 새 코드 위치 판단표

| 추가하려는 코드 | 위치 |
|---|---|
| Kafka 배치 수신/그룹핑/ack | `indexing.consumer` |
| 이벤트 스키마/테넌트 검증 | `indexing.consumer` |
| 여러 단계의 호출 순서, 재시도 루프 | `indexing.pipeline.service` |
| 작업 문맥 및 Job 상태 enum | `indexing.pipeline.domain` |
| 원문 다운로드, 무결성 검증 | `indexing.retrieval` |
| 문서 형식별 파싱 | `indexing.parsing` |
| chunk size, overlap, token 분할 | `indexing.chunking.service` |
| 청킹 결과 데이터 | `indexing.chunking.domain` |
| `indexing_job`/`document` JPA Entity, Repository | `indexing.publication.entity` / `.repository` |
| 실패 기록, 백오프 계산 | `indexing.publication.service` |
| `DOCUMENT_DELETED` 처리, 정리 스윕 | `indexing.deletion` |
| 임베딩 API 호출, `document_chunk` 저장, 검색 버전 승격 | **이 레포 밖** — `IndexingProcessor` 구현체 |

분류가 애매한 코드는 먼저 "한 단계 내부의 기능인가, 여러 단계를 연결하는 코드인가"를
판단한다. 한 단계 내부의 기능이면 해당 feature에 두고, 여러 단계를 연결하는 코드일
때만 `pipeline`에 둔다. `IndexingProcessor`의 실제 구현이 이 레포로 들어오는
시점(머지)에는, `Chunk`/`IndexingContext`/`IndexingJobStatus`/`IndexingProcessor`
네 타입의 중복을 정리하고 이 레포 쪽을 지운다.
