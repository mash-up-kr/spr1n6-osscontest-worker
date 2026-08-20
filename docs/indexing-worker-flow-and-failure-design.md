# 인덱싱 워커 전체 흐름 및 장애 대응 설계

> 구현 기준: `2026-08-20` 현재 작업 트리
>
> 검증 범위: `src/main`, `application.yml`, DB migration, 단위 테스트 및 비외부 연동 테스트
>
> 문서 성격: 현재 구현을 설명하는 운영·개발 기준 문서. 과거 계획 문서의 미구현/예정 설명보다 이 문서와 실제 코드를 우선한다. 단, 아직 구현하지 않은 변경안은 본문에서 반드시 **변경 예정(미구현)**으로 표시한다.

## 1. 목적과 범위

이 워커는 Kafka의 `indexing` 토픽에서 문서 이벤트를 배치로 받아 다음 두 경로를 처리한다.

- `INDEXING_REQUESTED`: 원문 다운로드 → 무결성 검증 → 파싱 → 청킹 → OpenAI 임베딩 → PostgreSQL/pgvector 반영 → 검색 버전 승격
- `DOCUMENT_DELETED`: 활성 인덱싱 Job 종결 → 해당 문서의 모든 청크 삭제

이 문서는 다음을 설명한다.

1. 정상 처리의 실제 호출 순서와 DB 상태 변화
2. 각 단계에서 가능한 장애와 현재 구현된 방어 장치
3. Kafka ack/nack, 재시도, 트랜잭션, 멱등성의 경계
4. 현재 구현이 아직 완전히 막지 못하는 장애와 운영 전제

다음은 이 워커의 책임 밖이다.

- 업로드 API와 `document`/`document_version` 생성
- Outbox 생성 및 Kafka 발행을 담당하는 Relay
- 검색 API
- S3 원본 삭제와 `document.purged_at` 기록
- 실패 Job 재인덱싱 API와 운영 대시보드/알림 구성

## 2. 한눈에 보는 전체 구조

```text
업로드/API 서버
  ├─ document, document_version 생성
  ├─ 원문을 S3 호환 스토리지에 저장
  └─ Outbox/Relay를 통해 indexing 토픽에 발행
                    │
                    │ key = documentId
                    ▼
IndexingKafkaListener (batch, manual ack)
  ├─ 같은 key: 배치 내 순차 처리
  └─ 다른 key: 고정 스레드풀에서 병렬 처리
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
INDEXING_REQUESTED      DOCUMENT_DELETED
IndexingPipelineRunner  DocumentDeletionHandler
  │                       │
  ├─ Job 확보/시작         ├─ schema/tenant 검증
  ├─ 이벤트/tenant 검증    └─ DocumentDeletionService
  ├─ S3 임시 파일 다운로드      ├─ 활성 Job FAILED
  ├─ SHA-256 검증                └─ 모든 chunk DELETE
  ├─ MIME별 파싱
  ├─ 전략별 청킹                  ▲
  ├─ 청크 상한 검증               │
  └─ EmbeddingIndexingProcessor   └─ 삭제 이벤트 유실 시
       ├─ OpenAI 임베딩              주기적 sweep이 보정
       ├─ 결과 검증
       └─ IndexingPublicationService (@Transactional)
            ├─ chunk UPSERT + trailing DELETE
            ├─ 저장 개수 검증
            ├─ document_version 완료
            ├─ searchable_version_id 조건부 승격
            └─ indexing_job COMPLETED
                    │
                    ▼
       배치의 모든 그룹 처리 종료
          ├─ DB 기록 불가: batch nack
          └─ 그 외: batch ack
```

핵심 구현 진입점은 [IndexingKafkaListener](../src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListener.kt), [IndexingPipelineRunner](../src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt), [EmbeddingIndexingProcessor](../src/main/kotlin/com/osscontest/worker/indexing/embedding/service/EmbeddingIndexingProcessor.kt), [IndexingPublicationService](../src/main/kotlin/com/osscontest/worker/indexing/publication/service/IndexingPublicationService.kt)다.

## 3. 설계 불변조건

### 3.1 문서 단위 순서는 Kafka key로 보장한다

`INDEXING_REQUESTED`와 `DOCUMENT_DELETED`를 같은 `indexing` 토픽에 싣고 `documentId`를 key로 사용한다. Kafka의 같은 파티션 내 순서와 리스너의 key별 순차 처리로 “업로드 후 삭제” 같은 문서 단위 순서를 유지한다.

이 보장은 생산자가 항상 동일한 문서에 동일한 key를 사용한다는 전제에 의존한다. 현재 소비자는 Kafka key와 payload의 `documentId`가 일치하는지 검증하지 않는다.

### 3.2 Kafka 전달은 중복될 수 있고 DB 결과가 수렴해야 한다

정상 인덱싱 경로는 exactly-once가 아니라 Kafka 재전달에 따른 at-least-once 가능성을 전제로 한다. 중복 실행을 완전히 배제하기보다 다음 장치로 같은 최종 상태에 수렴시킨다. 역직렬화/검증 실패처럼 의도적으로 ack 후 폐기하는 예외는 이 보장에 포함되지 않는다.

- `indexing_job.source_event_id UNIQUE`: 같은 이벤트의 Job 중복 생성 방지
- 활성 상태에만 적용되는 `uq_indexing_job_active_version`: 같은 문서 버전의 동시 활성 Job 제한
- `UNIQUE(document_version_id, chunk_no)` + UPSERT: 같은 청크 재저장 수렴
- 결정적인 청킹 결과: 같은 입력과 설정이면 같은 `chunk_no`, content, hash 생성
- 검색 버전 fencing: 더 낮거나 같은 `embedding_version_no`가 검색 포인터를 덮어쓰지 못함

DB 제약은 [V1__create_indexing_schema.sql](../src/main/resources/db/migration/V1__create_indexing_schema.sql), 청크 UPSERT는 [DocumentChunkWriter](../src/main/kotlin/com/osscontest/worker/indexing/publication/repository/DocumentChunkWriter.kt), 검색 포인터 fencing은 [DocumentRepository](../src/main/kotlin/com/osscontest/worker/indexing/publication/repository/DocumentRepository.kt)에 있다.

### 3.3 외부 API 호출과 DB publication을 분리한다

다운로드, 파싱, 청킹, OpenAI 호출은 publication 트랜잭션 밖에서 수행한다. 외부 응답을 기다리는 동안 DB 트랜잭션과 잠금을 오래 유지하지 않기 위해서다.

임베딩이 준비된 뒤의 DB 변경만 하나의 publication 트랜잭션으로 묶는다. 이 트랜잭션 안에서 하나라도 실패하면 청크 저장, 문서 버전 완료, 검색 포인터 변경, Job 완료가 함께 롤백된다.

### 3.4 최신 검색 버전과 인덱싱 완료를 구분한다

이전 버전도 끝까지 파싱·청킹·임베딩하여 청크를 보관한다. 다만 `document.searchable_version_id`는 현재 검색 버전보다 `embedding_version_no`가 **큰** 후보만 승격한다. 따라서 느린 구버전 Job이 먼저 완료된 신버전을 덮어쓸 수 없다.

승격되지 않은 이전 버전도 Job 상태는 `COMPLETED`다. 현재 상태 enum과 DB 제약에는 `STALE`이 없다.

## 4. 이벤트 수신, 병렬성, ack/nack

### 4.1 이벤트 계약

공통 DTO는 [IndexingRequestedEvent](../src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingRequestedEvent.kt)다.

| 필드 | 계약 |
|---|---|
| `eventId` | Job 멱등 키로 사용하는 UUID |
| `eventType` | `INDEXING_REQUESTED` 또는 `DOCUMENT_DELETED` |
| `eventSchemaVersion` | 기본 지원 버전 `1` |
| `tenantId` | DB의 문서 tenant와 일치해야 함 |
| `documentId` | 대상 문서 ID이자 Kafka key로 사용해야 하는 값 |
| `documentVersionId` | 인덱싱 이벤트에는 필수, 삭제 이벤트에는 `null` |
| `occurredAt` | 이벤트 발생 시각. 현재 처리 순서나 DB 갱신 조건에는 사용하지 않음 |
| `traceId` | `indexing_job.trace_id`와 로그 MDC에 저장 |

`DOCUMENT_DELETED`의 `documentVersionId=null`은 “특정 버전이 없는 이벤트”라는 표현이며, 삭제 범위를 한 버전으로 제한한다는 뜻이 아니다. 현재 삭제 핸들러는 이 필드를 읽거나 `null`인지 검증하지 않고 `documentId`만 사용한다. 따라서 삭제 이벤트에 실수로 non-null 값이 들어와도 해당 값을 무시하고 문서 전체 범위의 삭제를 수행한다.

### 4.2 배치 내부 실행 방식

리스너는 `spring.kafka.listener.type=batch`, `ack-mode=manual`, 기본 `max.poll.records=10`으로 동작한다. 한 배치를 Kafka key로 그룹화한 뒤 그룹별 작업을 executor에 제출한다. 이때 Kafka key는 payload의 `documentId`를 일관된 문자열로 표현한 값이어야 한다. 그래야 같은 문서의 인덱싱·삭제 이벤트가 같은 파티션에 기록되고, 배치 안에서도 같은 그룹으로 묶여 순서대로 처리된다. Worker는 `record.key()`와 payload의 `documentId`가 일치하는지 별도로 검증하지 않으므로, Producer가 key를 누락하거나 다른 값을 넣으면 이 순서 보장은 성립하지 않는다.

- 같은 key의 레코드: 수신 순서대로 `forEach` 처리
- 다른 key의 그룹: 기본 5개 스레드에서 병렬 처리
- ack 시점: 모든 그룹의 future가 끝난 뒤 배치당 한 번

`INDEXING_CONSUMER_CONCURRENCY`는 이름과 달리 현재 Kafka listener container의 consumer concurrency를 설정하지 않는다. [IndexingBatchExecutorConfig](../src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingBatchExecutorConfig.kt)의 배치 내부 작업 풀 크기와 파싱 전용 풀 크기를 함께 설정한다.

### 4.3 ack/nack 결정표

여기서 “역직렬화”는 이미 `String`으로 전달된 `ConsumerRecord.value()` JSON을 Jackson으로 `IndexingRequestedEvent`에 매핑하는 단계다. JSON 문법 오류, UUID/시각/숫자 타입 변환 실패, non-null 필드 누락 등이 여기에 해당하며 PDF/HWP 원문 파싱 실패와는 별개다. Kafka wire payload를 `String`으로 만드는 consumer deserializer에서 발생한 실패는 이 `processRecord()`의 catch 범위에 들어오지 않는다.

| 결과 | 현재 동작 | 이유와 영향 |
|---|---|---|
| 전체 정상 또는 Job이 `COMPLETED`/`FAILED`로 수렴 | 배치 ack | Kafka 재전달이 더 필요하지 않음 |
| 역직렬화 실패 | 로그 후 해당 레코드 폐기, 최종 배치 ack | poison message가 파티션을 영구 차단하지 않게 함. DB/DLQ 기록은 없음 |
| 지원하지 않는 이벤트/검증 실패 | 로그 또는 Job `FAILED`, 최종 배치 ack | 동일 입력 재시도가 의미 없다고 판단 |
| Runner/DeletionHandler의 일반 `Exception`이 `processRecord()`의 generic catch에 도달 | 로그 후 예외를 삼키고 최종 배치 ack | 배치 진행성 우선. 단, DB에 실패 상태를 못 남긴 예외까지 이 경로에 들어갈 수 있는 잔여 위험이 있음 |
| `DataAccessException`이 이탈 | 모든 future를 기다린 뒤 `ack.nack(0, delay)` | DB에 처리 결과를 기록하지 못했으므로 배치 전체 재전달 |
| `Error` 등 `Exception` 밖의 치명 오류 | 컨테이너로 전파, ack 없음 | `OutOfMemoryError`, `StackOverflowError`, `LinkageError` 등이 해당. 재전달로 원인이 없어지지 않으면 같은 오류가 반복될 수 있음 |

배치 nack은 특정 레코드가 아니라 인덱스 0부터 배치 전체를 되감는다. 이미 성공한 다른 문서도 재전달되지만 멱등 장치로 수렴한다.

일반 `Exception`은 원칙적으로 `processRecord()` 밖으로 이탈하지 않는다. 다만 `DataAccessException`은 명시적으로 다시 던진다. 파이프라인 본 처리 예외는 대부분 Runner가 `RETRY_WAIT`/`FAILED`로 기록하지만, Job 획득 전의 비-DB RuntimeException, 실패 기록 자체의 비-DB 예외, 삭제 경로의 예상하지 못한 RuntimeException 등은 generic catch가 로그만 남기고 ack하는 경로에 들어갈 수 있다. 반면 `Error`는 의도적으로 잡지 않는다. Job `start()`가 커밋된 뒤 발생한 `Error`는 재전달 때 `attempt_count` 상한의 영향을 받을 수 있지만, Job 생성 전·삭제 경로·DTO 매핑 경로의 `Error`에는 Job 단위 상한이 없다.

## 5. `INDEXING_REQUESTED` 상세 흐름과 단계별 장애 대응

### 5.1 Job 행 확보

`IndexingPipelineRunner.acquireJobId()`가 먼저 `INSERT ... ON CONFLICT DO NOTHING`을 실행하고 `source_event_id`로 Job을 다시 조회한다.

`UNIQUE` 제약과 `insertIfAbsent()`는 대체 관계가 아니다. `source_event_id UNIQUE`와 활성 버전 부분 유니크 인덱스가 충돌을 판정하고, `INSERT ... ON CONFLICT DO NOTHING`은 그 충돌을 예외 대신 0행 삽입으로 바꾸는 현재 구현 방식이다. 조회는 “신규 삽입 성공”과 “기존 행 충돌” 양쪽에서 실제 Job을 확인하기 위해 항상 수행한다.

| 발생 가능한 장애/경합 | 방어 설계 | 결과 |
|---|---|---|
| 같은 이벤트 중복 전달 | `source_event_id UNIQUE` + Kafka record identity 비교 | 같은 record 재전달이면 미완료 Job을 재처리하고, 다른 record 중복 발행이면 무시 |
| 다른 이벤트가 같은 문서 버전을 이미 활성 처리 | 활성 버전 부분 유니크 인덱스 | 새 행은 생성되지 않고 현재 이벤트 처리는 종료. 기존 활성 Job이 작업을 대표한다는 전제 |
| `documentVersionId == null` | FK/NOT NULL Job을 억지로 만들지 않고 조기 종료 | 로그 후 ack. Job/DLQ 기록 없음 |
| DB 연결/쓰기 실패 | `DataAccessException`을 리스너까지 전달 | 배치 nack 및 재전달 |

#### Kafka record identity로 충돌 원인 구분

리스너는 `@KafkaListener`가 받은 `ConsumerRecord`의 `topic()`, `partition()`, `offset()`으로 `KafkaRecordIdentity`를 만들고 Runner에 이벤트와 함께 전달한다. `indexing_job`은 초기 V1 schema부터 `kafka_topic`, `kafka_partition`, `kafka_offset`을 필수 컬럼으로 가진다. Kafka record의 완전한 식별자는 `topic + partition + offset`이므로 세 값을 항상 함께 비교한다.

신규 Job은 `insertIfAbsent()`에서 세 값을 함께 저장한다. DB 컬럼과 Entity 모두 non-null이며 topic은 비어 있지 않고 partition/offset은 음수가 아니어야 한다.

`insertIfAbsent()`는 이 식별자를 함께 INSERT하고, 충돌 뒤 `source_event_id`로 조회한 결과를 다음처럼 판정한다.

| 판정 | 조건 | 처리 |
|---|---|---|
| 신규 Job | INSERT 1행 | 생성된 Job 처리 |
| 동일 record 재전달 | 기존 Job의 topic/partition/offset이 현재 record와 같음 | 미완료 Job 재처리 허용 |
| 같은 이벤트의 중복 발행 | `source_event_id`는 같지만 topic/partition/offset이 다름 | 새 record를 멱등하게 무시 |
| 다른 이벤트의 활성 버전 충돌 | INSERT는 0행이고 현재 `source_event_id` 조회 결과도 없음 | 기존 활성 Job이 해당 버전을 대표한다고 보고 현재 record 종료 |

`source_event_id UNIQUE`와 활성 버전 부분 유니크 인덱스는 그대로 유지한다. record identity 비교는 유니크 제약을 제거하는 변경이 아니라, 충돌 후 “재처리할 record인가”를 `acquireJobId()`에서 결정하는 추가 조건이다. 현재 `start()`의 production 호출은 이 획득 경로 뒤 한 곳뿐이므로 identity를 다시 비교하지 않는다.

동일 record 재전달로 판정한 뒤에는 기존 Job 상태에 따라 처리한다.

| 기존 상태 | 목표 동작 |
|---|---|
| `PENDING`, `PROCESSING` | `attempt_count` 상한 안에서 `PROCESSING`으로 재획득 |
| `RETRY_WAIT`, `next_retry_at` 도달 | 즉시 `PROCESSING`으로 재획득 |
| `RETRY_WAIT`, `next_retry_at` 미도달 | DB 시각으로 남은 시간을 계산해 현재 호출에서 인라인 대기한 뒤 재획득. 정상 반환·ack하지 않음 |
| `COMPLETED`, `FAILED` | 이미 종결된 Job이므로 재실행하지 않고 record 처리 종료 |

특히 due 전 `RETRY_WAIT`에서 단순히 `start()==0`을 정상 종료로 취급하면 offset은 ack되고 Job은 다시 집힐 수 없으므로 허용하지 않는다. 별도 Job 회수 scheduler를 두지 않는다는 설계는 이 record를 잡고 있는 호출이 due 시각까지 책임지고 대기한다는 뜻이다.

이 방식은 중복 실행 자체를 완전히 배제하지 않는다. 리밸런스 경계에서 같은 record를 구 워커와 새 워커가 동시에 실행할 수 있으며, 결과는 결정적 청킹과 chunk UPSERT, 검색 버전 fencing으로 수렴시킨다. 대신 Lease 만료·갱신과 미완료 Job 회수 스케줄러는 두지 않는다. 단, Relay가 논리적으로 같은 이벤트를 중복 발행하면서 매번 새로운 `eventId`까지 생성하면 이 방식만으로는 같은 이벤트임을 알 수 없으므로, 중복 발행은 동일 `eventId`를 유지한다는 생산자 계약이 필요하다.

구현 회귀 테스트는 다음을 확인한다.

- 같은 topic/partition/offset의 `PROCESSING` Job은 크래시 재전달로 보고 재획득한다.
- 같은 `eventId`이지만 offset이 다른 record는 중복 발행으로 보고 `start()`하지 않는다.
- 같은 record의 `COMPLETED`/`FAILED` Job은 다시 실행하지 않는다.
- 같은 record의 due 전 `RETRY_WAIT` Job은 ack하지 않고 due 이후 재획득한다.
- 다른 `eventId`가 같은 활성 `documentVersionId`와 충돌하면 기존과 같이 현재 record를 종료한다.

`DOCUMENT_DELETED`는 `indexing_job`을 생성하지 않으므로 이 record identity 판정 대상이 아니다. 삭제 처리는 `documentId` 전체에 대해 멱등하게 반복 실행되는 현재 구조를 유지한다.

### 5.2 Job 처리 권한 획득

[IndexingJobRepository.start](../src/main/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepository.kt)는 한 번의 조건부 UPDATE로 다음을 수행한다.

```text
PENDING 또는 PROCESSING
  └─ attempt_count < maxAttempts
      → PROCESSING, attempt_count + 1

RETRY_WAIT
  └─ next_retry_at <= DB CURRENT_TIMESTAMP
     그리고 attempt_count < maxAttempts
      → PROCESSING, attempt_count + 1
```

`PROCESSING`도 다시 획득할 수 있어 워커 크래시 후 Kafka 재전달 시 별도 고아 Job 스위퍼 없이 재처리할 수 있다. `attempt_count` 상한에 도달한 `PENDING`/`PROCESSING`은 `MAX_ATTEMPTS_EXCEEDED`로 `FAILED` 처리한다.

동시에 두 워커가 같은 Job을 실행하는 것을 이 UPDATE가 완전히 배제하지는 않는다. 두 호출이 순차로 UPDATE에 성공할 수 있으며, 설계는 결정적 청킹·UPSERT·검색 fencing으로 결과를 수렴시키는 쪽을 택한다.

현재 `worker_id`는 마지막 획득 워커를 기록할 뿐 완료/실패 쓰기의 권한을 검증하지 않으며, `attempt_count`도 fencing token으로 전달·검증되지 않는다. 즉 Lease나 실행권 fencing은 없고, `PROCESSING` 재획득과 중복 실행을 허용하는 모델이다. Kafka record identity는 “어떤 Kafka record가 이 Job을 다시 시작할 수 있는가”를 제한하지만, 동일 record를 처리하는 두 실행 중 하나를 배제하는 Lease나 fencing token은 아니다.

### 5.3 이벤트와 tenant 검증

[IndexingEventValidator](../src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingEventValidator.kt)가 schema version, 문서 버전 존재 여부, `documentVersion.documentId == event.documentId`를 확인한다. Runner가 `document`를 추가 조회하여 tenant 일치도 확인한다.

검증은 Job 시작 뒤 수행한다. 그래야 검증 실패도 시도 횟수와 최종 실패 상태를 DB에 남길 수 있다.

| 장애 | 에러 코드 | 분류/대응 |
|---|---|---|
| 미지원 schema version | `UNSUPPORTED_SCHEMA_VERSION` | 영구 실패, 즉시 `FAILED` |
| 문서 버전 없음 | `DOCUMENT_VERSION_NOT_FOUND` | 영구 실패, 즉시 `FAILED` |
| 문서와 버전 관계 불일치 | `DOCUMENT_MISMATCH` | 영구 실패, 즉시 `FAILED` |
| 문서 없음 | `DOCUMENT_NOT_FOUND` | 영구 실패, 즉시 `FAILED` |
| tenant 불일치 | `TENANT_MISMATCH` | 영구 실패, 즉시 `FAILED` |

현재 Runner는 `document.deleted_at`을 이 단계에서 거부하지 않는다. 삭제 문서에 늦게 도착한 인덱싱 이벤트도 계산과 청크 저장까지 갈 수 있고, 검색 포인터 승격만 차단된다. 남은 청크는 삭제 sweep이 최종 정리한다.

### 5.4 파일 크기 확인과 다운로드

다운로드 전에 `document_version.file_size`가 기본 200 MiB 상한을 넘는지 확인한다. 초과하면 S3 호출 없이 `FILE_TOO_LARGE` 영구 실패로 끝낸다.

[S3DocumentDownloadClient](../src/main/kotlin/com/osscontest/worker/indexing/retrieval/S3DocumentDownloadClient.kt)는 S3 응답을 JVM `ByteArray`로 반환하지 않고 OS 임시 파일로 스트리밍한다. 전체 API call timeout 기본값은 30초다. 반환된 파일은 파이프라인의 `finally`에서 삭제한다.

| 장애 | 방어 설계 | 분류/결과 |
|---|---|---|
| 메타데이터상 대용량 파일 | 다운로드 전 크기 상한 | `FILE_TOO_LARGE`, 즉시 `FAILED` |
| S3 무응답 | AWS SDK `apiCallTimeout=30s` | SDK 예외가 그대로 전파되어 기본적으로 재시도 |
| key 없음, 네트워크/자격 증명 오류 | SDK 예외를 숨기지 않음 | 기본적으로 재시도, 최종적으로 예외 클래스명이 에러 코드가 됨 |
| 힙 메모리 급증 | 임시 파일 다운로드 | 다운로드 단계의 원문 전체 `ByteArray` 보유를 제거 |

현재 상한은 DB의 `file_size`를 신뢰하며 실제 다운로드 바이트 수를 다시 제한하지 않는다. 또 `getObject`가 반환 전에 실패하면 Runner가 임시 경로를 전달받지 못하므로 SDK가 남긴 부분 파일을 정리하지 못할 가능성이 있다.

### 5.5 원문 SHA-256 무결성 검증

다운로드한 파일을 `DigestInputStream`으로 스트리밍하며 SHA-256을 계산하고 `document_version.content_hash`와 정확히 비교한다. 실제 값은 `sha256:<lowercase hex>` 형식이다.

불일치는 동일한 입력으로 재시도해도 해결되지 않는다고 보고 `HASH_MISMATCH` 영구 실패로 즉시 종결한다. 정상 반환 이후의 오류이므로 Runner의 `finally`가 임시 파일을 삭제한다.

### 5.6 MIME별 파싱과 타임아웃

[DocumentParserRegistry](../src/main/kotlin/com/osscontest/worker/indexing/parsing/DocumentParserRegistry.kt)가 MIME type별 파서를 선택한다.

| MIME type | 구현 | 출력 특징 |
|---|---|---|
| `text/plain`, `text/markdown` | `TextDocumentParser` | Markdown heading과 문단, heading path |
| `application/pdf` | `PdfDocumentParser` | 페이지별 문단, page number |
| DOCX MIME | `DocxDocumentParser` | heading/paragraph, 표를 pipe 형식 text로 변환 |
| `application/x-hwp`, `application/haansofthwp` | `HwpDocumentParser` | 전체 텍스트를 문단으로 분리 |

[ParsingTimeoutGuard](../src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt)는 파싱을 별도 고정 스레드풀에서 실행하고 기본 60초 안에 `Sequence`를 끝까지 소비한다.

| 장애 | 방어 설계 | 분류/결과 |
|---|---|---|
| 미지원 MIME | Registry에서 명시적 거부 | `UNSUPPORTED_MIME_TYPE`, 즉시 `FAILED` |
| 파싱 중 `IOException` | `CorruptedFileException`으로 변환 | `CORRUPTED_FILE`, 즉시 `FAILED` |
| 파서 hang/장시간 처리 | timeout 후 `Future.cancel(true)` | 재시도. 현재 DB 에러 코드는 `PARSE_TIMEOUT`이 아니라 클래스명 `ParseTimeoutException`으로 기록됨 |
| 파서가 interrupt를 무시 | timeout 횟수를 `parse_thread_leaked`에 누적 | 관측만 가능하며 스레드 강제 종료는 보장하지 못함 |

다운로드는 파일 스트리밍이지만 PDF와 HWP 파서는 내부에서 `readAllBytes()`를 사용한다. 따라서 200 MiB 상한 안에서도 파서/라이브러리의 추가 객체와 함께 큰 힙 사용량이 발생할 수 있다. 파싱 timeout도 협조적 interruption에 의존하므로 네이티브/라이브러리 코드가 멈춘 경우 풀 고갈 가능성이 남는다.

### 5.7 청킹과 리소스 가드

[ChunkingService](../src/main/kotlin/com/osscontest/worker/indexing/chunking/service/ChunkingService.kt)는 설정에 따라 다음 전략 중 하나를 선택한다. 선택 기준은 문서 MIME, 크기, tenant 같은 입력 속성이 아니라 Worker 기동 시 읽는 전역 설정 `INDEXING_CHUNKING_STRATEGY`이며 기본값은 `FIXED_TOKEN`이다. 한 Job이나 문서 버전별로 전략을 자동 선택하지 않고, 실제 사용 전략을 `indexing_job`/`document_version`에도 저장하지 않는다.

- `FIXED_TOKEN`: 전체 블록 텍스트를 합쳐 512 token 단위로 분할
- `PARAGRAPH`: 블록 경계를 유지하고 긴 블록만 512 token 단위로 분할
- `PARAGRAPH_OVERLAP`: 긴 블록을 512 token, 기본 64 token overlap으로 분할

모든 전략은 `cl100k_base` tokenizer를 사용하고, 0부터 연속된 `chunkNo`와 SHA-256 content hash를 만든다. [ChunkGuard](../src/main/kotlin/com/osscontest/worker/indexing/chunking/service/ChunkGuard.kt)가 다음 상한을 적용한다.

| 장애 | 기본 상한 | 에러 코드 | 분류 |
|---|---:|---|---|
| 파싱 결과/청크가 비어 있음 | 최소 1개 | `EMPTY_EXTRACTION` | 영구 실패 |
| 청크 수 폭증 | 5,000개 | `CHUNK_LIMIT_EXCEEDED` | 영구 실패 |
| 총 토큰 수 폭증 | 2,000,000 token | `TOTAL_TOKEN_LIMIT_EXCEEDED` | 영구 실패 |

결정적 청킹은 중복 실행 시 UPSERT 수렴의 전제다. 배포된 워커 사이에 전략, tokenizer 버전, chunk size가 다르면 이 전제가 약해지므로 rolling deployment 시 설정을 동일하게 유지해야 한다.

현재 `tokenCount`는 heading prefix를 제외한 본문 window 기준이고, `null`이면 총합 계산에서 0으로 취급한다. 따라서 guard의 합계가 실제 공급자 입력 token보다 작을 수 있다.

### 5.8 임베딩 호출과 결과 검증

[EmbeddingService](../src/main/kotlin/com/osscontest/worker/indexing/embedding/service/EmbeddingService.kt)는 모든 청크 content를 한 번의 `EmbeddingModel.embed(List<String>)` 호출로 전달한다. 모델은 `text-embedding-3-small`, 차원은 코드·설정·DB 모두 1536으로 고정되어 있다.

호출 전후에 다음을 검증한다.

- 청크가 비어 있지 않음
- `chunkNo == 0..n-1`
- content와 content hash가 공백이 아님
- 응답 vector 개수가 청크 개수와 같음
- 모든 vector가 1536차원
- 모든 값이 finite이며 `NaN`/무한대가 아님

| 장애 | 방어 설계 | 분류/결과 |
|---|---|---|
| 공급자 HTTP 400 | `EmbeddingRequestRejectedException`으로 변환 | `EMBEDDING_REQUEST_REJECTED`, 즉시 `FAILED` |
| 응답 개수/차원/값 이상 | 저장 전 결과 검증 | `INVALID_EMBEDDING`, 즉시 `FAILED` |
| 429, 5xx, 네트워크 오류 | 원 예외 전파 | 기본적으로 인라인 재시도 |
| DB vector 차원 불일치 | 코드/설정/migration 1536 일치 + 저장 전 검증 | 설정 drift를 조기에 차단 |

현재 임베딩 호출 전용 timeout과 Worker 차원의 요청 분할은 없다. 청크가 최대 5,000개여도 코드는 한 번의 `EmbeddingModel.embed` 호출에 모두 전달하므로 공급자 제한, 응답 지연, `max.poll.interval.ms` 초과 위험이 남는다.

### 5.9 publication 트랜잭션

임베딩 성공 뒤 [IndexingPublicationService](../src/main/kotlin/com/osscontest/worker/indexing/publication/service/IndexingPublicationService.kt)가 아래 전체를 하나의 `@Transactional` 범위에서 실행한다.

1. `(document_version_id, chunk_no)` 기준으로 청크를 한 행씩 UPSERT
2. 새 마지막 `chunk_no`보다 큰 과거 trailing chunk 삭제
3. 해당 문서 버전의 실제 저장 개수와 요청 개수 비교
4. `document_version.chunk_count`, `extracted_metadata`, `indexed_at` 갱신
5. 문서가 삭제되지 않았고 후보의 `embedding_version_no`가 현재 검색 버전보다 클 때만 `searchable_version_id`를 승격하고 `latest_embedding_version_no`를 증가 방향으로 갱신
6. `indexing_job`을 `COMPLETED`로 갱신

| 장애/경합 | 방어 설계 | 결과 |
|---|---|---|
| 재실행/동시 저장 | unique key 기반 UPSERT | 같은 청크 번호의 최종 상태로 수렴 |
| 재청킹 후 청크 수 감소 | trailing chunk DELETE | 과거 꼬리 청크 제거 |
| 일부 청크만 저장 | 저장 후 count 검증 | 불일치 시 전체 publication 롤백 |
| 문서/버전 관계 오류 | 복합 FK, UPDATE 조건, 입력 check | 전체 publication 롤백 |
| 느린 구버전 완료 | `embedding_version_no` 비교 fencing | 청크는 보관하되 검색 포인터는 유지 |
| 처리 중 문서 삭제 | `deleted_at IS NULL` 승격 조건 | 검색 노출 차단. Job은 `COMPLETED`이며 청크는 sweep 대상 |
| 중간 DB 장애 | 단일 transaction | 청크/버전/포인터/Job 일부만 커밋되는 상태 방지 |

같은 원문을 같은 파서·tokenizer·청킹 전략·설정으로 처리하면 청크 수와 번호가 결정적이므로 정상적인 동일 실행 사이에는 trailing chunk가 생기지 않는다. 이 삭제는 이전 실행과 재시도 사이의 배포 버전, 파서/tokenizer, 청킹 전략 또는 chunk size가 달라졌거나 과거 구현이 더 많은 청크를 저장한 경우를 방어한다. 특히 청킹 전략이 Job에 저장되지 않으므로 재기동·rolling deployment 중 설정이 달라지면 실제로 청크 수가 바뀔 수 있다. 따라서 trailing DELETE는 정상 경로의 비결정성을 전제한 로직이 아니라 구성·버전 drift와 과거 데이터에 대한 방어 로직이다.

OpenAI 호출은 이 트랜잭션 전에 끝나므로 DB 오류로 publication이 실패하면 재시도 시 임베딩 비용을 다시 지불한다. 임베딩 결과 캐시는 없다.

## 6. 실패 분류와 인라인 재시도

### 6.1 상태 전이

```text
신규 이벤트
   │
   ▼
PENDING ── start() ──▶ PROCESSING ── 성공 ──▶ COMPLETED
                         │
                         ├─ 영구 실패 ─────────▶ FAILED
                         │
                         └─ 일시 실패, 예산 남음
                                  ▼
                              RETRY_WAIT
                                  │ next_retry_at 도달
                                  └──────────────▶ PROCESSING

PENDING/PROCESSING + attempt_count 상한 도달
   └────────────────────────────────────────────▶ FAILED
```

`attempt_count`는 `start()`가 `PROCESSING`으로 전환할 때 증가한다. 실패 기록은 별도 `REQUIRES_NEW` 트랜잭션에서 Job을 `PESSIMISTIC_WRITE`로 잠그고, 현재 상태가 `PROCESSING`일 때만 적용한다. 더 빠른 성공이 이미 `COMPLETED`로 바꿨다면 늦은 실패 쓰기는 무시된다.

### 6.2 영구 실패 목록

다음 예외만 명시적으로 영구 실패다.

| 분류 | DB `last_error_code` |
|---|---|
| 이벤트/tenant 검증 | `InvalidEventException.code` |
| 원문 hash 불일치 | `HASH_MISMATCH` |
| 빈 추출 | `EMPTY_EXTRACTION` |
| 청크/총 token 상한 | `CHUNK_LIMIT_EXCEEDED`, `TOTAL_TOKEN_LIMIT_EXCEEDED` |
| 미지원 MIME | `UNSUPPORTED_MIME_TYPE` |
| 선언된 파일 크기 초과 | `FILE_TOO_LARGE` |
| `IOException`으로 식별된 손상 파일 | `CORRUPTED_FILE` |
| 공급자 HTTP 400 | `EMBEDDING_REQUEST_REJECTED` |
| 임베딩 결과 불변조건 위반 | `INVALID_EMBEDDING` |

그 외 `Exception`은 기본적으로 재시도 가능하다. 새 예외를 추가할 때 “동일 입력으로 다시 실행하면 성공할 가능성이 있는가”를 판단하고, 영구 실패라면 `isRetryable()`과 `errorCodeOf()`를 함께 수정해야 한다.

### 6.3 재시도 계산

기본값은 최대 5회 시도와 30초 선형 backoff다.

“인라인 재시도”는 실패 record를 ack한 뒤 별도 scheduler나 새 Kafka 전달로 처리한다는 뜻이 아니다. 현재 `IndexingPipelineRunner.run()` 호출과 같은 executor thread 안에서 Job을 `RETRY_WAIT`으로 기록하고 `next_retry_at`까지 `Thread.sleep()`한 뒤, 같은 이벤트로 루프를 다시 도는 방식이다. sleep 중에는 해당 executor thread와 Kafka 배치가 점유되고 offset은 커밋되지 않는다. 임베딩 429, 5xx, 네트워크 오류가 `EmbeddingModel` 밖으로 예외로 전파되면 영구 실패 목록에 없으므로 이 경로를 탄다.

```text
next_retry_at = 실패 시각(DB CURRENT_TIMESTAMP)
              + base_delay × 현재 attempt_count

1회 실패 후 30초
2회 실패 후 60초
3회 실패 후 90초
4회 실패 후 120초
5회 실패 시 FAILED
```

앱 서버와 DB의 시계 오차로 너무 일찍 재획득하는 일을 막기 위해 실패 시각, `next_retry_at` 비교, 남은 sleep 계산에 모두 DB 시계를 사용한다. 대기 중에는 같은 executor thread와 Kafka 배치가 점유되며 offset은 아직 ack되지 않는다.

기본 backoff 합은 300초다. 전체 poll 점유 시간은 대략 `각 시도의 실제 처리 시간 합 + 300초`이며, 900초 `max.poll.interval.ms` 안에 들어간다는 하드 보장은 없다. 특히 임베딩 timeout 부재, 한 key에 여러 레코드가 있는 배치, executor queue 대기가 예산을 초과시킬 수 있다.

## 7. DB 장애 대응

DB 장애 대응은 독립적으로 실행되는 주기적 health gate와 실제 처리 실패에 반응하는 nack 두 겹이다. nack이 health gate를 호출하거나, health gate가 반드시 nack 이후에 실행되는 순서 관계는 없다.

1. [DbHealthGate](../src/main/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGate.kt)가 기본 5초마다 `SELECT 1`을 수행한다.
2. 실패하면 listener container에 pause를 요청하고 `db_health_gate_paused_total`을 증가시킨다.
3. pause 중에도 consumer heartbeat/poll은 유지되어 불필요한 rebalance를 줄인다.
4. DB가 회복되면 container를 resume한다.
5. 이미 받은 배치에서 DB 결과를 기록하지 못해 `DataAccessException`이 이탈하면 5초 지연으로 배치 전체를 nack한다.

Health gate는 애플리케이션 기동 뒤부터 스케줄에 따라 계속 실행되므로 DB 장애를 배치 처리 전·처리 중·nack 후 어느 시점에든 감지할 수 있다. 짧은 장애가 다음 health check 전에 회복되면 nack만 발생하고 pause는 발생하지 않을 수도 있다. 반대로 먼저 pause가 요청돼도 이미 리스너에 전달되어 실행 중인 배치는 끝까지 처리한 뒤 필요하면 nack한다.

Runner 안의 DB 예외가 항상 즉시 nack되는 것은 아니다. 실패 상태를 DB에 정상적으로 `RETRY_WAIT`으로 기록할 수 있으면 일반 일시 실패처럼 인라인 재시도를 한다. DB 시각 조회나 실패 기록 자체도 실패해 `DataAccessException`이 리스너까지 올라왔을 때 nack한다.

## 8. `DOCUMENT_DELETED`와 삭제 보정

### 8.1 이벤트 경로

[DocumentDeletionHandler](../src/main/kotlin/com/osscontest/worker/indexing/consumer/DocumentDeletionHandler.kt)는 삭제 전에 다음을 확인한다.

- 지원하는 schema version인가
- 문서가 존재하는가
- 이벤트 tenant와 문서 tenant가 일치하는가

검증 후 [DocumentDeletionService](../src/main/kotlin/com/osscontest/worker/indexing/deletion/service/DocumentDeletionService.kt)가 순서대로 처리한다.

1. 해당 문서의 `PENDING`, `PROCESSING`, `RETRY_WAIT` Job을 `DOCUMENT_DELETED` 사유의 `FAILED`로 변경
2. 해당 `document_id`의 모든 `document_chunk` 삭제

삭제 기준은 `documentVersionId`가 아니라 `documentId`다. 따라서 한 버전의 청크만 지우는 것이 아니라 그 문서에 속한 모든 버전의 청크를 삭제하고, 모든 활성 Job을 종결한다. Worker는 `document`/`document_version` 행과 원본 S3 object를 물리 삭제하지 않으며, 원본 삭제와 `document.purged_at` 기록은 API 서버 책임이다.

두 연산은 서비스 전체 단일 트랜잭션이 아니라 각각의 repository 트랜잭션이다. 첫 단계만 성공하고 청크 삭제가 실패할 수 있지만, 삭제 sweep이 청크 삭제를 다시 시도하도록 설계되어 있다.

### 8.2 유실/경합 보정 sweep

[DocumentDeletionSweepScheduler](../src/main/kotlin/com/osscontest/worker/indexing/deletion/DocumentDeletionSweepScheduler.kt)는 기본적으로 기동 60초 뒤 시작하고, 이후 60초 fixed delay로 실행한다.

- `document.deleted_at IS NOT NULL`인데 청크가 남은 문서를 오래된 삭제 순으로 조회
- 한 번에 기본 50개 처리
- 이벤트 경로와 동일한 멱등 삭제 함수를 재호출
- 한 문서 실패를 로그로 남기고 다음 문서를 계속 처리

이 설계는 삭제 이벤트 유실, 삭제 도중 DB 장애, 삭제와 인덱싱 완료의 경합으로 다시 생긴 청크를 최종적으로 제거한다. 단, 계속 실패하는 문서의 최대 재시도 횟수나 격리 큐는 없으며 매 sweep마다 무기한 재시도한다.

원본 S3 object 삭제와 `purged_at` 기록은 이 워커가 하지 않는다.

## 9. 트랜잭션 경계

| 구간 | 트랜잭션 |
|---|---|
| Job `insertIfAbsent` | repository 단위 독립 트랜잭션 |
| Job `start`, `failIfAttemptsExceeded`, phase 갱신 | 각 repository 호출 단위 트랜잭션 |
| 다운로드, hash, 파싱, 청킹, OpenAI 호출 | DB 트랜잭션 없음 |
| chunk UPSERT → trailing DELETE → count → version 완료 → 검색 승격 → Job 완료 | `IndexingPublicationService.publish()` 단일 트랜잭션 |
| 실패 기록 | `REQUIRES_NEW`, Job row 비관적 잠금 |
| 삭제의 활성 Job 실패 처리와 chunk 삭제 | 각각 별도 repository 트랜잭션 |

Runner 전체를 한 트랜잭션으로 묶지 않는 이유는 외부 I/O 중 DB connection/lock을 오래 잡지 않고, Kafka 재전달과 조건부 UPDATE로 단계별 수렴을 만들기 위해서다.

## 10. 관측 가능성

### 10.1 DB 진행 상태

`indexing_job`이 실행 이력과 재시도 상태의 기준이다.

| 필드 | 의미 |
|---|---|
| `status` | `PENDING`, `PROCESSING`, `RETRY_WAIT`, `COMPLETED`, `FAILED` |
| `attempt_count` | `start()` 성공 횟수 |
| `next_retry_at` | 인라인 재시도 가능 시각 |
| `worker_id` | 마지막으로 획득한 워커 식별자. 기본은 인스턴스별 랜덤 UUID |
| `last_error_code/message` | 마지막 실패 정보. 각각 최대 100/1,000자로 절단 |
| `trace_id` | 원 이벤트 trace ID |
| `kafka_topic/partition/offset` | 이 Job을 생성한 Kafka record의 불변 위치 |
| `phase` | 마지막으로 진입한 단계 |
| `started_at/completed_at` | 최초 시작/종료 시각 |

phase는 단계 진입 직전에 `DOWNLOADING`, `PARSING`, `CHUNKING`, `EMBEDDING`으로 갱신된다. 재시도 시작 시 `DOWNLOADING`으로 돌아가지만 `RETRY_WAIT` 중에는 실패한 단계 값이 남는다. 따라서 phase만으로 현재 실행 중이라고 판단하면 안 된다. `PUBLISHING` phase와 상태 조회 API는 이 저장소에 없다.

### 10.2 로그와 trace

레코드 처리 스레드에서 이벤트의 `traceId`를 MDC에 넣고 `finally`에서 제거한다. 콘솔 로그 패턴에도 trace ID가 포함된다. 비동기 그룹 처리에 사용하는 각 작업 내부에서 MDC를 설정하므로 listener thread의 MDC 상속에 의존하지 않는다.

### 10.3 커스텀 metric

| metric | 타입/태그 | 의미 |
|---|---|---|
| `indexing_job_duration_seconds{phase=total}` | Timer | Job 획득 뒤 최종 반환까지, 재시도 sleep 포함 |
| `indexing_inline_retry_total{attempt}` | Counter | 실제 `RETRY_WAIT` 인라인 재시도 진입 횟수 |
| `indexing_job_failed_total{errorCode}` | Counter | 최종 `FAILED` 횟수 |
| `parse_thread_leaked` | Gauge | timeout으로 취소 요청한 파싱 작업 누적 수의 근사치 |
| `db_health_gate_paused_total` | Counter | DB 장애로 pause 전이한 횟수 |
| `kafka_rebalance_total` | Counter | non-empty partition revoke 횟수 |

`parse_thread_leaked`는 현재 살아 있는 thread 수가 아니라 timeout 누적치이며 감소하지 않는다. 또한 이 저장소에는 Prometheus registry, metric export endpoint 설정, dashboard, alert rule이 없다. 계측값을 실제 운영에서 수집·경보하려면 배포 인프라 구성이 추가로 필요하다.

## 11. 설정과 배포 전제

현재 설정의 기준은 [application.yml](../src/main/resources/application.yml)이다.

| 환경변수/설정 | 기본값 | 용도 |
|---|---:|---|
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` | 필수 | PostgreSQL 연결 |
| `KAFKA_BOOTSTRAP_SERVERS` | 필수 | Kafka bootstrap |
| `OPENAI_API_KEY` | 필수 | OpenAI 임베딩 |
| `INDEXING_STORAGE_BUCKET` | 필수 | 원문 bucket |
| `INDEXING_STORAGE_ENDPOINT` | 빈 값 | S3 호환 endpoint override |
| `INDEXING_STORAGE_REGION` | `us-east-1` | S3 region |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | provider chain | S3 자격 증명 |
| `INDEXING_WORKER_ID` (`indexing.worker-id`) | 시작 시 랜덤 UUID | 마지막 처리 워커 식별자 |
| `INDEXING_BATCH_SIZE` | `10` | `max.poll.records` |
| `INDEXING_CONSUMER_CONCURRENCY` | `5` | 배치 그룹 executor와 파싱 executor 각각의 pool size |
| `INDEXING_SUPPORTED_SCHEMA_VERSIONS` | `1` | 허용 이벤트 schema |
| `INDEXING_MAX_ATTEMPTS` | `5` | Job 총 시도 상한 |
| `INDEXING_RETRY_BASE_DELAY` | `PT30S` | 선형 backoff 기준 |
| `INDEXING_DB_HEALTH_CHECK_INTERVAL_MS` | `5000` | DB gate 주기 |
| `INDEXING_DB_HEALTH_PAUSE_NACK_DELAY` | `PT5S` | batch nack 지연 |
| `INDEXING_MAX_FILE_SIZE_BYTES` | `209715200` | DB file size 상한, 200 MiB |
| `INDEXING_PARSE_TIMEOUT` | `PT60S` | 파싱 timeout |
| `INDEXING_CHUNKING_STRATEGY` | `FIXED_TOKEN` | 청킹 전략 |
| `INDEXING_MAX_CHUNKS` | `5000` | 문서당 청크 상한 |
| `INDEXING_MAX_TOTAL_TOKENS` | `2000000` | 문서당 집계 token 상한 |
| `INDEXING_DELETION_INITIAL_DELAY_MS` | `60000` | 첫 삭제 sweep 지연 |
| `INDEXING_DELETION_SWEEP_INTERVAL_MS` | `60000` | 삭제 sweep fixed delay |
| `INDEXING_DELETION_BATCH_SIZE` | `50` | sweep당 문서 수 |

S3 전체 API call timeout은 현재 `indexing.storage.download-timeout=PT30S`로 고정되어 있고 별도 환경변수 placeholder가 없다. 임베딩 모델(`text-embedding-3-small`)과 차원(1536)도 현재 YAML과 코드에 고정되어 있다.

Kafka 설정은 다음과 같다.

- YAML의 consumer group 설정: `indexing-worker`
- 현재 listener의 실효 group ID: `indexing` (`@KafkaListener(id = "indexing")`와 Spring Kafka 4.1의 기본 `idIsGroup=true` 때문)
- auto commit: off
- offset reset: earliest
- listener: batch/manual ack
- `max.poll.interval.ms`: 900,000ms
- `session.timeout.ms`: 45,000ms
- `heartbeat.interval.ms`: 3,000ms

따라서 consumer group을 바꾸려면 YAML만 수정해서는 안 된다. listener에 `groupId`를 명시하거나 `idIsGroup=false`로 바꾸지 않는 한 annotation의 `id`가 group ID로 사용된다. `id="indexing"`은 `DbHealthGate`가 컨테이너를 찾는 ID이기도 하므로 함께 검토해야 한다.

JPA `ddl-auto`와 Flyway가 모두 비활성화되어 있으므로 애플리케이션이 운영 DB 스키마를 만들거나 갱신하지 않는다. 제공된 V1 migration도 독립 실행 가능한 전체 서비스 스키마가 아니며 기존 `tenant(id)` 테이블을 전제로 한다. 배포 전 pgvector extension, `vector(1536)`, JSONB, 복합 FK/unique index와 Kafka identity 필수 컬럼·check constraint가 실제 DB에 반영됐는지 확인해야 한다.

## 12. 현재 남아 있는 장애 경계

아래 항목은 현재 코드가 완전히 방어한다고 말하면 안 되는 영역이다.

### 12.1 처리 시간 예산이 하드하게 닫혀 있지 않음

- 임베딩 전용 timeout이 없음
- 최대 5,000개 청크를 한 provider 요청으로 전달
- 한 key의 여러 배치 레코드는 순차 처리
- 파싱 thread가 interrupt를 무시할 수 있음
- `max.poll.interval.ms=900s`와 기본 재시도 예산 사이에 강제 검증이 없음

따라서 900초 설정은 여유를 늘린 값이지 rebalance 방지 보장이 아니다.

### 12.2 메모리·디스크 상한의 빈틈

- 파일 크기는 실제 S3 응답이 아니라 DB metadata만 검사
- PDF/HWP는 파일 전체를 다시 메모리에 올림
- 실패한 S3 다운로드의 부분 임시 파일 정리가 보장되지 않음
- timeout된 HWP 파서가 멈추면 내부 임시 파일도 남을 수 있음

### 12.3 ack 후 복구 수단이 없는 입력

역직렬화 실패, 알 수 없는 이벤트 타입, `documentVersionId`가 없는 인덱싱 이벤트는 Job이나 DLQ 없이 로그만 남기고 ack된다. 또한 Runner/DeletionHandler에서 `processRecord()`의 generic catch까지 도달한 일반 `Exception`도 리스너가 로그 후 삼키고 ack한다. 로그 수집 실패나 알림 부재 시 운영자가 놓칠 수 있다.

### 12.4 삭제와 실행 중 Job의 경합

삭제는 실행 중 작업을 interrupt하지 않는다. 삭제 서비스가 Job을 `FAILED(DOCUMENT_DELETED)`로 바꾼 뒤 기존 작업이 publication에 도달하면 `complete()`는 상태 가드 없이 `COMPLETED`를 쓸 수 있다. 검색 승격은 `deleted_at IS NULL` 조건으로 막히고 sweep이 청크를 지우지만, Job 상태가 삭제 사실과 다르게 보일 수 있다.

삭제 뒤 새 인덱싱 이벤트도 Runner 입구에서 차단하지 않으므로 청크가 다시 생겼다가 sweep에서 제거될 수 있다. 정상 생산자가 삭제 뒤 인덱싱 이벤트를 만들지 않고 동일 key 순서를 지킨다는 전제가 중요하다.

### 12.5 관측과 복구는 아직 애플리케이션 내부에만 있음

metric 계측은 있지만 exporter/dashboard/alert가 없고, 실패 Job 조회·재인덱싱 API도 이 저장소에 없다. DLQ를 두지 않는 설계이므로 `FAILED` 조회와 `indexing_job_failed_total` 경보가 실제 인프라에 연결되지 않으면 복구 루프가 완성되지 않는다.

### 12.6 Kafka group 설정 이름과 실효값이 다름

`application.yml`에는 `group-id=indexing-worker`가 있지만 현재 annotation 기본 규칙상 실효 group ID는 `indexing`이다. 배포 설정이나 대시보드가 `indexing-worker`를 기준으로 lag를 조회하면 실제 consumer를 놓칠 수 있다.

### 12.7 timestamp 타입과 시계가 완전히 통일되지는 않음

재시도 계산과 대부분의 상태 갱신은 DB `CURRENT_TIMESTAMP`를 사용하지만 `document_chunk.embedded_at`은 `Clock.systemUTC()`로 만든 애플리케이션 `LocalDateTime`이다. Entity는 DB의 `TIMESTAMPTZ`를 `LocalDateTime`으로 매핑하므로 JVM/DB session timezone 설정이 다를 때 시각 해석을 통합 검증해야 한다. Runner가 만드는 `extractedMetadata`는 현재 항상 `null`이므로 파싱 결과에서 문서 metadata를 채우는 흐름도 아직 없다.

### 12.8 치명 오류는 재전달만으로 해결되지 않을 수 있음

`Error`는 `processRecord()`에서 잡지 않으므로 ack되지 않고 컨테이너로 전파된다. 일시적인 프로세스 불안정이면 재시작과 재전달로 회복할 수 있지만, 특정 입력이 항상 유발하는 `StackOverflowError`, 배포 자체의 `LinkageError`, 해소되지 않은 메모리 부족은 같은 record에서 반복될 수 있다. Job 시작 이후의 오류는 재전달 과정에서 `attempt_count` 상한에 도달할 수 있지만, Job 생성 전·삭제·DTO 매핑 경로에는 그 상한이 적용되지 않는다. 현재 별도 DLT나 listener 수준의 delivery-attempt 제한은 없다.

## 13. 변경 시 개발자 체크리스트

- 이벤트 생산자는 반드시 `documentId`를 Kafka key로 유지하는가?
- 중복 발행은 같은 논리 이벤트에 동일한 `eventId`를 유지하는가?
- topic/partition/offset을 신규 Job 생성 시 항상 함께 저장하고 `acquireJobId()`에서 비교하는가?
- 새 이벤트 schema를 두 핸들러 모두에서 같은 정책으로 허용하는가?
- 새 예외가 영구/일시 중 어디에 속하며 `errorCodeOf()`까지 반영됐는가?
- 청킹 변경이 동일 입력에 결정적이며 rolling deployment 중 버전 혼용이 안전한가?
- OpenAI model/dimensions 변경을 코드의 1536 검증, `application.yml`, pgvector column과 함께 바꿨는가?
- retry 횟수·delay·각 단계 timeout 변경 후 최악 배치 시간이 `max.poll.interval.ms` 안에 드는지 다시 계산했는가?
- `RETRY_WAIT` record가 `next_retry_at` 전에 재전달돼도 ack 후 고아 Job으로 남지 않는가?
- 외부 I/O를 publication 트랜잭션 안으로 넣지 않았는가?
- 검색 포인터 갱신에 `deleted_at`과 `embedding_version_no` fencing이 유지되는가?
- 삭제 뒤 청크가 다시 생기는 경합과 sweep의 수렴성을 테스트했는가?
- invalid/drop, retry, terminal failure, DB nack이 각각 로그·Job·metric 중 어디에 남는지 확인했는가?

## 14. 구현 검증 근거

주요 동작은 다음 테스트에서 확인한다.

- 배치 그룹 순서, ack/nack, MDC, `ConsumerRecord` identity 전달: [IndexingKafkaListenerTest](../src/test/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListenerTest.kt)
- Kafka 재전달/중복 발행 구분, due 전 `RETRY_WAIT`, 전체 단계 호출, 영구 실패 분류, file/hash guard, metric: [IndexingPipelineRunnerTest](../src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt)
- 실제 인라인 재시도 상태 전이: [IndexingPipelineRunnerIntegrationTest](../src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerIntegrationTest.kt)
- Job identity 저장, 획득/재획득/상한/삭제 상태 전이: [IndexingJobRepositoryIntegrationTest](../src/test/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepositoryIntegrationTest.kt)
- 실패 backoff와 비관적 잠금: [IndexingFailureServiceIntegrationTest](../src/test/kotlin/com/osscontest/worker/indexing/publication/service/IndexingFailureServiceIntegrationTest.kt)
- publication 승격 skip 시 완료: [IndexingPublicationServiceTest](../src/test/kotlin/com/osscontest/worker/indexing/publication/service/IndexingPublicationServiceTest.kt)
- 파싱 timeout/손상 파일: [ParsingTimeoutGuardTest](../src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt)
- 청킹 결정성과 guard: [chunking tests](../src/test/kotlin/com/osscontest/worker/indexing/chunking/service)
- 삭제 멱등성과 sweep: [deletion tests](../src/test/kotlin/com/osscontest/worker/indexing/deletion)
- DB gate와 rebalance metric: [consumer tests](../src/test/kotlin/com/osscontest/worker/indexing/consumer)

작성 시점에 `./gradlew test`는 성공했다. `integrationTest` 태그의 실제 PostgreSQL/OpenAI 연동 테스트는 기본 `test` 작업에서 제외되며, 외부 서비스와 자격 증명을 준비해 별도로 실행해야 한다.
