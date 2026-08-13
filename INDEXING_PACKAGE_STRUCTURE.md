# Indexing 패키지 구조와 확장 규칙

## 1. 문서 목적

이 문서는 `com.osscontest.worker.indexing` 아래의 패키지 책임과 의존 방향을 정의한다.

새 기능을 추가하는 개발자나 AI는 파일을 만들기 전에 이 문서를 기준으로 위치를 결정한다. 특히 청킹 기능을 추가할 때 청킹 알고리즘을 `pipeline`, `embedding`, `publication`에 넣지 않도록 하는 것이 목적이다.

현재 Worker는 이미 생성된 `List<Chunk>`를 입력받아 임베딩하고 DB에 반영하는 단계만 구현되어 있다. 청킹 알고리즘은 아직 구현되어 있지 않다.

---

## 2. 기본 원칙

최상위 패키지를 controller, service, repository 같은 기술 계층으로 나누지 않는다. 인덱싱 단계의 기능을 먼저 나누는 feature-first 구조를 사용한다.

```text
indexing
├── pipeline
├── chunking
├── embedding
└── publication
```

각 기능 내부에서 필요한 경우에만 `domain`, `service`, `usecase`, `entity`, `repository`로 나눈다.

`pipeline`은 기능 구현 패키지가 아니다. 여러 기능의 호출 순서와 실패 흐름을 조율하는 패키지다.

---

## 3. 현재 패키지 구조

```text
com.osscontest.worker.indexing
├── config
│   └── IndexingConfiguration.kt
│
├── pipeline
│   ├── domain
│   │   ├── IndexingContext.kt
│   │   └── IndexingJobStatus.kt
│   └── service
│       └── IndexingProcessor.kt
│
├── chunking
│   └── domain
│       └── Chunk.kt
│
├── embedding
│   ├── usecase
│   │   └── EmbeddingUseCase.kt
│   └── service
│       └── EmbeddingService.kt
│
└── publication
    ├── domain
    │   └── DocumentChunk.kt
    ├── entity
    │   ├── DocumentChunkEntity.kt
    │   ├── DocumentEntity.kt
    │   ├── DocumentVersionEntity.kt
    │   └── IndexingJobEntity.kt
    ├── repository
    │   ├── DocumentChunkRepository.kt
    │   ├── DocumentRepository.kt
    │   ├── DocumentVersionRepository.kt
    │   └── IndexingJobRepository.kt
    └── service
        ├── IndexingFailureService.kt
        └── IndexingPublicationService.kt
```

`chunking`에는 현재 출력 모델인 `Chunk`만 존재한다. 이것은 청킹 기능이 구현되었다는 의미가 아니다. 다른 Worker나 상위 단계에서 만들어진 청크를 현재 파이프라인이 전달받기 위한 계약이다.

---

## 4. 패키지별 책임

### `pipeline`

전체 인덱싱 처리 순서를 조율한다.

현재 `IndexingProcessor`의 책임은 다음과 같다.

1. `indexing_job` 처리 권한 획득
2. 전달받은 `Chunk` 목록 검증
3. 청크의 문자열을 `EmbeddingUseCase`에 전달
4. 임베딩 결과 개수와 차원 검증
5. `Chunk`, embedding, `IndexingContext`를 `DocumentChunk`로 결합
6. `IndexingPublicationService` 호출
7. 실패 시 `IndexingFailureService` 호출 후 원래 예외 재전파

`pipeline`에 들어가면 안 되는 코드는 다음과 같다.

- 문서 형식별 파싱 코드
- chunk size, overlap, token 분할 알고리즘
- Spring AI provider 호출 구현
- JPA Entity와 DB 저장 쿼리

### `chunking`

파싱된 문서를 검색과 임베딩에 사용할 청크 목록으로 변환하는 기능의 소유자다.

현재는 `Chunk` 데이터 모델만 존재한다. `Chunk`는 청킹 결과에 필요한 다음 데이터를 표현한다.

```text
chunkNo
content
contentHash
tokenCount
pageFrom / pageTo
sectionPath
metadata
```

`Chunk`는 Spring AI, JPA Entity, `DocumentChunk`를 알지 않는다. 청킹 기능은 문자열과 청크 메타데이터를 생성하는 것까지만 담당한다.

### `embedding`

문자열 목록을 임베딩 벡터 목록으로 변환한다.

```text
List<String>
→ EmbeddingUseCase
→ EmbeddingService
→ Spring AI EmbeddingModel
→ List<FloatArray>
```

`embedding`은 `Chunk`를 입력받지 않는다. 청크에서 `content`를 꺼내는 작업은 `pipeline` 책임이다. 이 경계를 유지하면 임베딩 기능을 청킹 방식과 독립적으로 사용할 수 있다.

### `publication`

완성된 임베딩 결과를 DB에 저장하고 검색 가능한 버전으로 확정한다.

```text
List<DocumentChunk>
→ 기존 미공개 버전 청크 삭제
→ document_chunk 저장
→ 청크 수 검증
→ document_version 완료
→ document.searchable_version_id 조건부 전환
→ indexing_job COMPLETED 또는 STALE
```

위 DB 변경은 `IndexingPublicationService`의 트랜잭션 안에서 실행한다. `publication`은 Spring AI 호출이나 청킹 알고리즘을 포함하지 않는다.

---

## 5. 현재 실행 흐름

현재 파이프라인은 청킹이 끝난 지점에서 시작한다.

```text
외부 Worker 또는 상위 단계
        ↓
IndexingContext + List<Chunk>
        ↓
IndexingProcessor
        ├─ EmbeddingUseCase.embed(chunks.map { it.content })
        ├─ Chunk와 embedding을 DocumentChunk로 결합
        └─ IndexingPublicationService.publish(...)
```

외부 임베딩 API 호출은 DB publication 트랜잭션 밖에서 수행한다. DB 저장과 검색 버전 전환만 하나의 트랜잭션으로 묶는다.

---

## 6. 청킹 기능을 추가하는 방법

청킹 구현이 필요해지면 다음 위치에 추가한다.

```text
com.osscontest.worker.indexing.chunking
├── domain
│   └── Chunk.kt                 # 이미 존재
└── service
    └── ChunkingService.kt       # 새로 추가할 청킹 구현
```

`ChunkingService`가 담당할 수 있는 내용은 다음과 같다.

- chunk size 정책
- overlap 정책
- 문자 또는 token 기준 분할
- `chunkNo` 부여
- `contentHash` 생성
- `tokenCount` 계산
- 페이지, 섹션, 청크 metadata 유지

입력 타입은 parsing 기능의 실제 출력이 정해진 뒤 결정한다. 아직 존재하지 않는 `ParsedDocument` 같은 모델을 청킹 구현 전에 임의로 만들지 않는다.

예상되는 연결 형태는 다음과 같다.

```kotlin
val chunks: List<Chunk> = chunkingService.chunk(parsedInput)

indexingProcessor.process(
    context = indexingContext,
    chunks = chunks,
)
```

전체 파이프라인을 이 Worker 안에서 처리하게 되면 상위 `pipeline` 조율 코드가 `ChunkingService`를 호출하도록 변경한다. 이때도 실제 분할 로직은 `IndexingProcessor`에 작성하지 않는다.

청킹 구현체가 하나뿐이라면 우선 구체 `ChunkingService`만 만든다. 구현 교체가 필요하거나 명확한 호출 계약이 필요한 시점에만 `chunking.usecase` 인터페이스를 추가한다.

---

## 7. 의존 방향

의존은 조율자인 `pipeline`에서 각 기능으로 향한다.

```text
pipeline
├── chunking.domain
├── embedding.usecase
└── publication.service / publication.domain
```

기능 패키지끼리는 불필요하게 서로 의존하지 않는다.

```text
chunking  -X→ embedding
chunking  -X→ publication
embedding -X→ publication
publication -X→ embedding
```

구체적인 규칙은 다음과 같다.

- `Chunk`에 임베딩 호출 메서드를 추가하지 않는다.
- `Chunk`에 JPA 매핑이나 `DocumentChunk` 변환 책임을 추가하지 않는다.
- `EmbeddingService`에 DB 저장 책임을 추가하지 않는다.
- `IndexingPublicationService`에서 청킹이나 임베딩을 호출하지 않는다.
- 단계 간 데이터 결합과 호출 순서는 `pipeline`에서 처리한다.

---

## 8. 새 코드 위치 판단표

| 추가하려는 코드 | 위치 |
|---|---|
| chunk size, overlap, token 분할 | `indexing.chunking.service` |
| 청킹 결과 데이터 | `indexing.chunking.domain` |
| 여러 단계의 호출 순서 | `indexing.pipeline.service` |
| 작업 문맥 및 파이프라인 상태 | `indexing.pipeline.domain` |
| Spring AI `EmbeddingModel` 호출 | `indexing.embedding.service` |
| 임베딩 호출 계약 | `indexing.embedding.usecase` |
| DB에 저장할 최종 청크 | `indexing.publication.domain` |
| JPA Entity | `indexing.publication.entity` |
| Spring Data JPA Repository | `indexing.publication.repository` |
| DB 트랜잭션 및 검색 버전 전환 | `indexing.publication.service` |

분류가 애매한 코드는 먼저 “한 단계 내부의 기능인가, 여러 단계를 연결하는 코드인가”를 판단한다. 한 단계 내부의 기능이면 해당 feature에 두고, 여러 단계를 연결하는 코드일 때만 `pipeline`에 둔다.
