# 임베딩 및 DB 반영 처리 흐름

## 1. 문서 목적

이 문서는 이미 청킹된 문서 데이터를 입력받아 임베딩을 생성하고, 임베딩 결과를 PostgreSQL에 저장한 뒤 해당 문서 버전을 검색 대상으로 전환하는 과정을 설명한다.

프로젝트를 처음 보는 개발자가 다음 내용을 파악할 수 있도록 작성되었다.

- 처리 시작 시 어떤 데이터가 필요한가
- 청크별 임베딩이 어떻게 생성되고 결합되는가
- 어떤 테이블을 어떤 순서로 변경하는가
- 정상 완료와 `STALE`은 어떻게 구분되는가
- 처리 실패 시 Job 상태와 재시도 정보가 어떻게 기록되는가
- 어느 구간이 하나의 트랜잭션으로 처리되는가
- 현재 구현에 포함되지 않은 기능은 무엇인가

현재 구현의 진입점은 다음 클래스다.

```text
com.osscontest.worker.indexing.pipeline.service.IndexingProcessor
```

---

## 2. 처리 범위

현재 Worker가 구현한 범위는 다음과 같다.

```text
이미 생성된 List<Chunk>
        ↓
Spring AI를 통한 청크별 임베딩
        ↓
DocumentChunk 변환
        ↓
document_chunk 저장
        ↓
document_version 완료 처리
        ↓
document 검색 버전 전환
        ↓
indexing_job 완료 또는 stale 처리
```

다음 기능은 현재 구현 범위가 아니다.

- 문서 수집
- 문서 파싱
- 청킹 알고리즘
- Kafka Consumer 및 이벤트 역직렬화
- 재시도 Job을 다시 가져오는 스케줄러
- DB 스키마 생성 또는 마이그레이션
- `outbox_event` 상태 변경

`outbox_event`는 Relay의 책임이며 이 Worker는 접근하지 않는다.

---

## 3. 전체 처리 흐름 요약

```text
IndexingProcessor.process(context, chunks)
        │
        ├─ 1. indexing_job을 PROCESSING으로 조건부 전환
        │
        ├─ 2. Chunk 데이터 검증
        │
        ├─ 3. List<String>을 Spring AI EmbeddingModel에 전달
        │
        ├─ 4. List<FloatArray> 결과 검증
        │
        ├─ 5. Chunk + embedding + context → DocumentChunk
        │
        └─ 6. IndexingPublicationService.publish()
                 │
                 ├─ 현재 검색 중인 버전인지 확인
                 ├─ 기존 document_chunk 삭제
                 ├─ 새 document_chunk 일괄 저장
                 ├─ 저장된 청크 수 검증
                 ├─ document_version 완료 처리
                 ├─ document 검색 버전 조건부 전환
                 └─ indexing_job을 COMPLETED 또는 STALE로 전환
```

2번부터 6번 사이에서 예외가 발생하면 다음 흐름으로 이동한다.

```text
예외 발생
    ↓
publication 트랜잭션이 진행 중이면 전체 롤백
    ↓
indexing_job을 비관적 잠금으로 조회
    ↓
attempt_count 증가
    ↓
재시도 가능 → RETRY_WAIT
최대 시도 횟수 도달 → FAILED
    ↓
원래 예외를 호출자에게 다시 전달
```

---

## 4. 입력 데이터

`IndexingProcessor.process()`는 `IndexingContext` 하나와 `List<Chunk>`를 입력받는다.

```kotlin
fun process(
    context: IndexingContext,
    chunks: List<Chunk>,
)
```

### 4.1 `IndexingContext`

`IndexingContext`는 이번 인덱싱 작업과 문서 버전을 식별한다.

```kotlin
data class IndexingContext(
    val jobId: Long,
    val documentId: Long,
    val documentVersionId: Long,
    val versionNo: Long,
    val extractedMetadata: Map<String, Any>?,
)
```

| 필드 | 의미 | 사용 위치 |
|---|---|---|
| `jobId` | 현재 인덱싱 Job ID | `indexing_job` 상태 변경 |
| `documentId` | 원본 문서 ID | `document_chunk`, `document_version`, `document` 변경 |
| `documentVersionId` | 처리 중인 문서 버전 ID | 청크 저장 및 문서 버전 완료 처리 |
| `versionNo` | 처리 중인 문서의 버전 번호 | 최신 버전인지 조건부 확인 |
| `extractedMetadata` | 파싱 단계에서 추출된 문서 메타데이터 | `document_version.extracted_metadata` 저장 |

### 4.2 `Chunk`

`Chunk`는 다른 단계에서 이미 생성된 청크 하나를 나타낸다.

```kotlin
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

| 필드 | 의미 |
|---|---|
| `chunkNo` | 문서 버전 안에서의 청크 순서 및 식별 번호 |
| `content` | 임베딩할 청크 원문 |
| `contentHash` | 청크 내용의 해시 |
| `tokenCount` | 청크 토큰 수. 없으면 `null` |
| `pageFrom`, `pageTo` | 청크가 위치한 문서 페이지 범위 |
| `sectionPath` | 문서 내 섹션 경로 |
| `metadata` | 청크별 부가정보. DB에는 JSONB로 저장 |

현재 Processor는 다음 조건을 먼저 검사한다.

- 동일 요청의 `chunkNo`는 중복될 수 없다.
- 모든 `content`는 공백 문자열이 아니어야 한다.
- 모든 `contentHash`는 공백 문자열이 아니어야 한다.

빈 `List<Chunk>`는 현재 허용된다. 이 경우 OpenAI를 호출하지 않고 빈 청크 목록으로 publication 단계를 수행하며 최종 `chunk_count`는 `0`이 된다.

---

## 5. Job 처리 권한 획득

가장 먼저 `indexing_job`을 `PROCESSING`으로 전환한다.

```text
PENDING → PROCESSING
RETRY_WAIT → PROCESSING
```

`RETRY_WAIT`인 경우 `next_retry_at`이 현재 시각 이전이거나 `null`일 때만 전환할 수 있다.

실제 조건은 다음과 같다.

```sql
UPDATE indexing_job
SET status = 'PROCESSING',
    next_retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :jobId
  AND (
      status = 'PENDING'
      OR (
          status = 'RETRY_WAIT'
          AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
      )
  );
```

UPDATE 결과가 `1`이면 현재 Worker가 Job 처리를 시작한다.

UPDATE 결과가 `0`이면 다음 중 하나이므로 `process()`를 조용히 종료한다.

- Job이 존재하지 않음
- 이미 다른 Worker가 처리 중임
- 이미 `COMPLETED`, `FAILED`, `STALE` 상태임
- 아직 재시도 시각이 되지 않음

이 조건부 UPDATE가 동일 Job의 중복 실행을 막는 1차 보호 장치다.

---

## 6. 청크 임베딩

### 6.1 Spring AI 호출

청크의 `content`만 뽑아 `EmbeddingUseCase`에 전달한다.

```kotlin
val contents: List<String> = chunks.map(Chunk::content)
val embeddings: List<FloatArray> = embeddingUseCase.embed(contents)
```

호출 구조는 다음과 같다.

```text
IndexingProcessor
    ↓
EmbeddingUseCase
    ↓
EmbeddingService
    ↓
Spring AI EmbeddingModel
    ↓
OpenAI text-embedding-3-small
```

여러 청크를 한 번에 전달하더라도 하나의 합쳐진 벡터가 만들어지는 것은 아니다. 각 문자열마다 독립적인 벡터가 생성되고 입력 순서와 같은 순서로 반환된다.

```text
chunks[0].content → embeddings[0]
chunks[1].content → embeddings[1]
chunks[2].content → embeddings[2]
```

### 6.2 임베딩 결과 검증

임베딩 결과는 DB 저장 전에 다음 조건을 만족해야 한다.

1. 입력 청크 수와 반환 벡터 수가 같아야 한다.
2. 각 벡터의 길이가 설정된 차원과 같아야 한다.
3. 벡터의 모든 값은 유한한 숫자여야 한다. `NaN`, 양의 무한대, 음의 무한대는 허용하지 않는다.

기본 설정은 다음과 같다.

```yaml
spring:
  ai:
    openai:
      embedding:
        model: text-embedding-3-small
        dimensions: 1536
```

모델 차원과 `document_chunk.embedding` 컬럼의 `vector(1536)` 차원은 반드시 같아야 한다.

---

## 7. `DocumentChunk` 변환

검증된 embedding을 같은 인덱스의 `Chunk`와 결합한다.

```text
Chunk
+ FloatArray
+ IndexingContext.documentId
+ IndexingContext.documentVersionId
+ embeddedAt
        ↓
DocumentChunk
```

생성되는 객체는 다음 정보를 가진다.

```kotlin
data class DocumentChunk(
    val documentVersionId: Long,
    val documentId: Long,
    val chunkNo: Int,
    val content: String,
    val contentHash: String,
    val tokenCount: Int?,
    val pageFrom: Int?,
    val pageTo: Int?,
    val sectionPath: String?,
    val metadata: Map<String, Any>?,
    val embedding: FloatArray,
    val embeddedAt: LocalDateTime,
)
```

한 처리 요청에 속한 모든 청크는 동일한 `embeddedAt`을 사용한다. 애플리케이션의 `Clock`은 UTC 시스템 시계를 사용한다.

`DocumentChunk`는 저장 직전에 `DocumentChunkEntity`로 변환된다.

- `metadata`는 Hibernate의 `SqlTypes.JSON`을 통해 PostgreSQL JSONB에 저장된다.
- `embedding`은 Hibernate Vector의 `SqlTypes.VECTOR`를 통해 PostgreSQL pgvector에 저장된다.

---

## 8. DB publication 트랜잭션

`IndexingPublicationService.publish()`는 `@Transactional`로 실행된다.

아래 작업은 모두 하나의 DB 트랜잭션에 포함된다.

```text
검색 중 버전 여부 확인
→ 기존 청크 삭제
→ 새 청크 저장
→ 청크 수 검증
→ document_version 완료
→ document 검색 버전 전환
→ indexing_job COMPLETED 또는 STALE
```

중간 단계에서 예외가 발생하면 이 트랜잭션에서 수행한 변경은 모두 롤백된다.

OpenAI 임베딩 호출은 이 트랜잭션에 포함되지 않는다. 외부 API 응답을 기다리는 동안 DB 연결이나 잠금을 유지하지 않기 위해서다.

### 8.1 이미 검색 중인 버전 보호

청크를 삭제하기 전에 대상 버전이 이미 검색 버전인지 확인한다.

```text
document.id = context.documentId
AND document.searchable_version_id = context.documentVersionId
```

이미 검색 중인 버전이면 기존 검색 데이터를 삭제하지 않고 예외를 발생시킨다.

### 8.2 기존 청크 삭제

현재 구현은 SQL `ON CONFLICT UPSERT` 방식이 아니다.

재처리 시 과거 청크가 남지 않도록 해당 문서 버전의 기존 청크를 모두 삭제한 후 새 목록을 다시 저장한다.

논리적으로 실행되는 쿼리는 다음과 같다.

```sql
DELETE FROM document_chunk
WHERE document_version_id = :documentVersionId;
```

### 8.3 새 청크 저장

`DocumentChunk`를 `DocumentChunkEntity`로 변환하고 Spring Data JPA로 일괄 저장한다.

```kotlin
documentChunkRepository.saveAll(
    documentChunks.map(DocumentChunkEntity::from),
)
documentChunkRepository.flush()
```

`flush()`를 호출하므로 INSERT가 즉시 DB에 반영되고 이후 저장 개수 검증에서 확인할 수 있다.

현재 Entity의 ID 생성 전략은 다음과 같다.

```kotlin
@GeneratedValue(strategy = GenerationType.IDENTITY)
```

따라서 `document_chunk.id`는 DB의 identity 또는 이에 호환되는 자동 증가 컬럼이어야 한다.

### 8.4 저장 개수 검증

저장 이후 해당 문서 버전의 실제 청크 개수를 조회한다.

```text
SELECT COUNT(*)
FROM document_chunk
WHERE document_version_id = :documentVersionId
```

다음 조건을 만족해야 이후 단계로 진행한다.

```text
전달받은 DocumentChunk 개수 == DB에 저장된 청크 개수
```

개수가 다르면 예외를 발생시키고 청크 삭제 및 저장을 포함한 publication 트랜잭션 전체를 롤백한다.

---

## 9. `document_version` 완료 처리

청크 저장과 개수 검증이 성공하면 처리 중인 문서 버전을 인덱싱 완료 상태로 만든다.

변경하는 컬럼은 다음과 같다.

| 컬럼 | 저장값 |
|---|---|
| `chunk_count` | DB에서 다시 확인한 실제 청크 수 |
| `extracted_metadata` | `IndexingContext.extractedMetadata` |
| `indexed_at` | DB `CURRENT_TIMESTAMP` |

조건부 UPDATE의 의미는 다음과 같다.

```text
id = documentVersionId
AND document_id = documentId
AND indexed_at IS NULL
```

즉, 요청의 문서와 문서 버전 관계가 일치하고 아직 완료되지 않은 버전만 완료 처리할 수 있다.

UPDATE 결과는 반드시 `1`이어야 한다. `0`이면 다음 가능성이 있으므로 예외로 처리하고 publication 트랜잭션을 롤백한다.

- 문서 버전이 존재하지 않음
- `documentId`와 `documentVersionId` 관계가 잘못됨
- 이미 `indexed_at`이 기록된 버전을 중복 처리함

`indexed_at`은 해당 버전의 청크 생성과 임베딩 저장이 끝났다는 뜻이다. 검색 노출 여부는 다음 단계의 `document.searchable_version_id`가 결정한다.

---

## 10. 검색 버전 조건부 전환

완료된 문서 버전이 현재 문서의 최신 버전이고 문서가 삭제되지 않았을 때만 검색 버전으로 전환한다.

변경 내용:

```text
document.searchable_version_id = context.documentVersionId
document.updated_at = CURRENT_TIMESTAMP
```

UPDATE 조건:

```text
document.id = context.documentId
AND document.latest_version_no = context.versionNo
AND document.deleted_at IS NULL
```

### UPDATE 결과가 1인 경우

현재 작업의 문서 버전이 최신이며 문서가 삭제되지 않았다. 검색 포인터 전환이 성공했으므로 Job을 `COMPLETED`로 변경한다.

```text
indexing_job.status = COMPLETED
indexing_job.completed_at = CURRENT_TIMESTAMP
indexing_job.next_retry_at = NULL
indexing_job.updated_at = CURRENT_TIMESTAMP
```

### UPDATE 결과가 0인 경우

다음 중 하나다.

- 처리 도중 더 최신 버전이 등록됨
- 처리 도중 문서가 삭제됨
- 대상 문서가 존재하지 않음

청크와 임베딩 저장 자체는 성공했으므로 실패로 보지 않고 Job을 `STALE`로 변경한다.

```text
indexing_job.status = STALE
indexing_job.completed_at = CURRENT_TIMESTAMP
indexing_job.next_retry_at = NULL
indexing_job.updated_at = CURRENT_TIMESTAMP
```

`STALE` 버전의 `document_version.indexed_at`과 `document_chunk` 데이터는 남는다. 단, `document.searchable_version_id`는 더 최신 버전을 계속 가리키므로 검색에 노출되지 않는다.

예시:

```text
V1 처리 시작
→ V2 등록
→ V2 인덱싱 완료 및 검색 버전 전환
→ V1 처리 완료
→ V1의 latest_version_no 조건 불일치
→ 검색 포인터는 V2 유지
→ V1 Job은 STALE
```

---

## 11. 실패 및 재시도 처리

임베딩, 입력 검증 또는 DB publication 과정에서 예외가 발생하면 `IndexingProcessor`가 실패 정보를 기록한다.

실패 기록은 `IndexingFailureService`에서 다음 설정으로 실행된다.

```kotlin
@Transactional(propagation = Propagation.REQUIRES_NEW)
```

publication 트랜잭션이 롤백되더라도 실패 상태는 별도의 새 트랜잭션으로 DB에 남는다.

### 11.1 동시성 보호

실패 상태를 변경할 때 `indexing_job`을 비관적 쓰기 잠금으로 조회한다.

```text
PESSIMISTIC_WRITE
```

조회한 Job의 현재 상태가 `PROCESSING`일 때만 실패 정보를 기록할 수 있다.

### 11.2 공통 변경 필드

실패할 때마다 다음 필드를 변경한다.

| 컬럼 | 변경 내용 |
|---|---|
| `attempt_count` | 기존 값에서 1 증가 |
| `last_error_code` | 예외 클래스 이름, 최대 100자 |
| `last_error_message` | 예외 메시지, 최대 1,000자 |
| `updated_at` | 애플리케이션 시각 |

### 11.3 재시도 가능한 경우

증가한 `attempt_count`가 최대 시도 횟수보다 작으면 다음과 같이 변경한다.

```text
status = RETRY_WAIT
next_retry_at = 실패 시각 + retry delay
completed_at = NULL
```

기본 재시도 설정은 다음과 같다.

```yaml
indexing:
  retry:
    max-attempts: 3
    delay: 30s
```

현재 프로젝트는 `RETRY_WAIT` Job을 다시 조회하여 실행하는 스케줄러나 Consumer를 구현하지 않았다. 외부 실행 주체가 `next_retry_at` 이후 Processor를 다시 호출해야 한다.

### 11.4 재시도 한도를 초과한 경우

증가한 `attempt_count`가 최대 시도 횟수 이상이면 다음과 같이 변경한다.

```text
status = FAILED
next_retry_at = NULL
completed_at = 실패 시각
```

실패 상태를 기록한 후에도 원래 발생한 예외는 호출자에게 다시 전달된다.

---

## 12. 테이블별 변경 요약

| 테이블 | 변경 시점 | 변경 내용 |
|---|---|---|
| `document_chunk` | 임베딩 성공 후 | 대상 버전 기존 행 삭제 후 새 청크 및 vector 저장 |
| `document_version` | 청크 수 검증 후 | `chunk_count`, `extracted_metadata`, `indexed_at` 변경 |
| `document` | 문서 버전 완료 후 | 최신·미삭제 조건을 만족하면 `searchable_version_id` 변경 |
| `indexing_job` | 시작, 완료, stale, 실패 시 | 상태, 완료 시각, 재시도 및 오류 정보 변경 |
| `outbox_event` | 변경하지 않음 | Relay 책임 |

### `document_chunk`에 저장되는 데이터

| DB 컬럼 | 데이터 출처 |
|---|---|
| `document_version_id` | `IndexingContext.documentVersionId` |
| `document_id` | `IndexingContext.documentId` |
| `chunk_no` | `Chunk.chunkNo` |
| `content` | `Chunk.content` |
| `content_hash` | `Chunk.contentHash` |
| `token_count` | `Chunk.tokenCount` |
| `page_from`, `page_to` | `Chunk.pageFrom`, `Chunk.pageTo` |
| `section_path` | `Chunk.sectionPath` |
| `metadata` | `Chunk.metadata`, JSONB |
| `embedding` | Spring AI 반환 `FloatArray`, vector(1536) |
| `embedded_at` | 한 처리 요청에서 공유하는 애플리케이션 시각 |

---

## 13. 트랜잭션 경계

처리 구간별 트랜잭션 경계는 다음과 같다.

### 별도 조건부 UPDATE

```text
indexing_job PENDING/RETRY_WAIT → PROCESSING
```

### DB 트랜잭션 밖

```text
Chunk 검증
→ OpenAI 임베딩 호출
→ embedding 검증
→ DocumentChunk 변환
```

### publication 트랜잭션

```text
검색 중 버전 보호 확인
→ 기존 청크 삭제
→ 새 청크 저장
→ 저장 개수 검증
→ document_version 완료
→ document 검색 버전 전환
→ Job COMPLETED 또는 STALE
```

### 별도 신규 트랜잭션

```text
실패 정보 기록
→ RETRY_WAIT 또는 FAILED
```

이 구조는 다음을 보장하기 위한 것이다.

- OpenAI 응답을 기다리는 동안 DB 트랜잭션을 열어두지 않는다.
- publication 중 일부 DB 변경만 커밋되지 않는다.
- publication이 롤백되어도 실패 상태는 기록된다.

---

## 14. 상태 전환 표

| 시작 상태 | 조건 | 종료 상태 |
|---|---|---|
| `PENDING` | Job 처리 권한 획득 | `PROCESSING` |
| `RETRY_WAIT` | `next_retry_at` 도달 후 처리 권한 획득 | `PROCESSING` |
| `PROCESSING` | 최신 문서 버전으로 검색 전환 성공 | `COMPLETED` |
| `PROCESSING` | 더 최신 버전이 있거나 문서가 삭제됨 | `STALE` |
| `PROCESSING` | 실패했고 재시도 횟수가 남음 | `RETRY_WAIT` |
| `PROCESSING` | 실패했고 최대 시도 횟수 도달 | `FAILED` |

`COMPLETED`, `STALE`, `FAILED` 상태는 현재 처리 흐름에서 다시 `PROCESSING`으로 변경되지 않는다.

---

## 15. 주요 클래스와 책임

| 클래스 | 책임 |
|---|---|
| `IndexingProcessor` | 전체 흐름 조정, 검증, 임베딩 호출, 실패 처리 연결 |
| `EmbeddingUseCase` | 임베딩 기능의 호출 계약 |
| `EmbeddingService` | Spring AI `EmbeddingModel` 호출 |
| `IndexingPublicationService` | 임베딩 이후 DB 변경을 하나의 트랜잭션으로 처리 |
| `IndexingFailureService` | publication 롤백과 별개로 실패 상태 기록 |
| `DocumentChunkRepository` | 청크 삭제, 저장, 개수 조회 |
| `DocumentVersionRepository` | 문서 버전 완료 정보 조건부 변경 |
| `DocumentRepository` | 최신 문서 검색 포인터 조건부 변경 |
| `IndexingJobRepository` | Job 시작·완료·stale 전환 및 잠금 조회 |
| `DocumentChunkEntity` | `document_chunk` 및 pgvector/JSONB JPA 매핑 |
| `DocumentVersionEntity` | `document_version` JPA 매핑 |
| `DocumentEntity` | `document` JPA 매핑 |
| `IndexingJobEntity` | `indexing_job` JPA 매핑 |

---

## 16. 실행에 필요한 설정

애플리케이션 시작 시 다음 환경변수가 필요하다.

```text
DB_URL
DB_USERNAME
DB_PASSWORD
OPENAI_API_KEY
```

선택 설정:

```text
EMBEDDING_DIMENSIONS       기본값 1536
INDEXING_MAX_ATTEMPTS      기본값 3
INDEXING_RETRY_DELAY       기본값 30s
```

예시:

```bash
export DB_URL="jdbc:postgresql://localhost:5432/osscontest"
export DB_USERNAME="postgres"
export DB_PASSWORD="postgres"
export OPENAI_API_KEY="..."
```

JPA 설정은 다음과 같다.

```yaml
spring:
  jpa:
    open-in-view: false
    hibernate:
      ddl-auto: none
```

`ddl-auto: none`이므로 애플리케이션은 테이블이나 pgvector extension을 생성하지 않는다. DB 스키마는 미리 준비되어 있어야 한다.

---

## 17. DB 스키마 전제 조건

현재 Entity와 쿼리는 최소한 다음 스키마를 전제로 한다.

- PostgreSQL에 pgvector extension이 설치되어 있음
- `document_chunk.embedding`은 `vector(1536)`임
- `document_chunk.metadata`는 JSONB임
- `document_version.extracted_metadata`는 JSONB임
- `document_chunk.id`는 identity 또는 호환 자동 증가 컬럼임
- `indexing_job.status`는 enum 이름을 문자열로 저장할 수 있음
- 코드에서 참조하는 모든 timestamp와 상태 컬럼이 존재함

Entity는 테이블의 일부 컬럼만 매핑한다. DB의 나머지 필수 컬럼에 기본값이 없으면 INSERT 또는 UPDATE 시 실패할 수 있으므로 실제 스키마와 반드시 대조해야 한다.

---

## 18. 현재 구현의 한계와 후속 작업

현재 코드는 컴파일과 Gradle 빌드가 성공하지만 실제 DB 및 OpenAI를 연결한 전체 흐름 검증은 별도 작업이다.

특히 다음 사항을 확인해야 한다.

1. 실제 DB 컬럼 타입과 Entity 필드 타입이 일치하는가
2. `document_chunk.id`의 생성 전략이 `IDENTITY`와 호환되는가
3. pgvector의 실제 컬럼 차원이 설정값과 같은가
4. JSONB에 포함될 metadata 값들을 Jackson이 모두 직렬화할 수 있는가
5. 상태 컬럼이 PostgreSQL enum이면 native query 문자열 대입에 명시적 cast가 필요한가
6. `LocalDateTime`과 DB timezone 정책이 일치하는가
7. 빈 청크 목록을 완료 가능한 문서 버전으로 취급할 것인가
8. `RETRY_WAIT` Job을 다시 실행시킬 주체는 무엇인가
9. Kafka 또는 다른 트리거가 `IndexingProcessor`에 `IndexingContext`와 `List<Chunk>`를 어떻게 전달할 것인가

현재 청크 저장은 “삭제 후 재생성” 방식이다. 따라서 `UNIQUE (document_version_id, chunk_no)`를 이용한 `ON CONFLICT UPSERT`와는 동작 방식이 다르다. 검색 중인 버전은 삭제 전에 차단하지만, 실제 운영 환경의 동시성 수준에 맞는 추가 잠금 정책이 필요한지는 DB 통합 검증 후 결정해야 한다.

---

## 19. 호출 예시

상위 단계가 이미 청크와 작업 컨텍스트를 확보했다면 다음처럼 호출한다.

```kotlin
indexingProcessor.process(
    context =
        IndexingContext(
            jobId = 1001L,
            documentId = 10L,
            documentVersionId = 25L,
            versionNo = 3L,
            extractedMetadata = mapOf("title" to "pgvector 소개"),
        ),
    chunks =
        listOf(
            Chunk(
                chunkNo = 0,
                content = "PostgreSQL은 오픈소스 관계형 데이터베이스이다.",
                contentHash = "hash-0",
                tokenCount = 20,
                pageFrom = 1,
                pageTo = 1,
                sectionPath = "1. PostgreSQL",
                metadata = null,
            ),
            Chunk(
                chunkNo = 1,
                content = "pgvector는 PostgreSQL에서 벡터 검색을 지원한다.",
                contentHash = "hash-1",
                tokenCount = 22,
                pageFrom = 1,
                pageTo = 2,
                sectionPath = "2. pgvector",
                metadata = null,
            ),
        ),
)
```

호출이 성공적으로 끝났다면 다음 중 하나다.

- 최신 문서 버전으로 전환되어 Job이 `COMPLETED`
- 저장은 완료됐지만 더 최신 버전이 존재하거나 문서가 삭제되어 Job이 `STALE`

예외가 반환됐다면 publication 변경은 롤백되고 Job은 재시도 횟수에 따라 `RETRY_WAIT` 또는 `FAILED` 상태가 된다.
