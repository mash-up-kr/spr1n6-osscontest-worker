# Worker DB 예약 재시도 전환 계획

- 상태: Worker 구현 반영. PostgreSQL 통합 검증·Core 스키마 적용·생산자 계약 확인은 남아 있음
- 작성일: 2026-10-10
- 검토 기준: Worker `feat/db-job-queue` (`0a958eb`), Core 저장소의 현재 `main`
- 구현 기준: DB 큐 코드가 머지된 Worker `main` (`0ea222d`). 아래 확인 결과는 구현 전 검토 기록이다.
- 범위: 인라인 Job 재시도 제거, DB 실패 상태·재시도 시각 기록, 실행권 해제와 슬롯 반환

## 확인 결과

- `IndexingPipelineRunner`의 일반 실패와 `ExternalApiOperations`의 429 처리 모두 인라인 대기하면서 슬롯·heartbeat를 유지한다. 실패 서비스는 Worker·lease를 해제하지 않는다.
- `decideFailure`는 아직 없다. 현재 정책은 `IndexingFailureService.recordFailure()`에 들어 있다.
- 실제 실행 경로인 `JobQueueStore.claim()`은 Job ID 순서, 재시도 우선 선택, 신규 작업 차단 정책을 사용한다. 별도의 `IndexingJobQueueRepository`는 `enqueue_seq`를 사용하지만 실행 루프에 연결되지 않았다.
- `recover_expired_indexing_jobs()`는 만료된 `PROCESSING`을 `PENDING`으로 되돌려 backoff를 건너뛴다. 시도 한도 처리는 함수 밖의 `JobQueueStore.recover()`에 있어 함수만 호출하는 DB 스케줄러와 정책이 달라질 수 있다.
- 계약 SQL과 기존 문서는 Job ID 순서·회수 시 epoch 증가를 전제로 한다. 엔티티와 별도 큐 저장소가 요구하는 `enqueue_seq`·문서 epoch 계약도 정렬해야 한다.

## 목표 처리 흐름

1. claim이 같은 Job에 새 실행권을 발급하고 `attempt_count`·epoch를 증가시킨다.
2. Worker가 한 번의 파이프라인 시도를 수행한다. DB 연결 장애는 1번 작업의 별도 대기·재개 흐름으로 처리한다.
3. 일반 처리 오류를 분류하고 `decideFailure`에서 다음 상태를 결정한다.
   - 일시 오류이고 `attempt_count < maxAttempts`: `RETRY_WAIT`과 `next_retry_at`.
   - 영구 오류 또는 시도 한도 소진: `FAILED`, `next_retry_at = NULL`.
4. 실패 반영 트랜잭션에서 현재 실행권을 검증하고 상태 변경과 Worker·lease 해제를 원자적으로 처리한다.
5. runner가 반환하고 활성 실행·heartbeat 대상을 정리한 뒤 슬롯을 반환한다. 다른 문서의 Job을 처리할 수 있어야 한다.
6. 저장된 재시도 시각 이후 다음 claim이 같은 Job을 다시 획득한다. 메모리 결과를 보존하는 인라인 재시도 대신 전체 파이프라인을 새로 수행한다.

## 실패 정책과 저장 계약

### 실패 결정

- `decideFailure`는 오류 분류, 현재 시도 횟수, 시도 한도, DB 시각을 받아 상태와 재시도 시각을 결정한다.
- 재시도 지연: `baseDelay × max(attempt_count, 1) × U(0.8, 1.2)`.
- 지터는 검증 가능한 난수 공급자로 분리한다. 최초 실패부터 기존 선형 backoff에 ±20%를 적용한다.
- `next_retry_at`은 짧은 실패 반영 트랜잭션에서 얻은 DB 시각을 기준으로 계산한다. JVM 시각을 사용하지 않는다.
- 시각 타입은 공용 스키마의 `TIMESTAMPTZ`와 애플리케이션의 `Instant`로 맞춘다.
- `next_retry_at`은 실행 예약의 보장 시각이 아니라 재시도가 가능해지는 최초 시각이다.

### 원자적 실패 반영

- 실제 쓰기 조건: Job ID, `status = PROCESSING`, Worker ID, epoch 일치, `lease_until > DB 시각`.
- 상태·오류·재시도/완료 시각 기록과 `worker_id = NULL`, `lease_until = NULL`을 같은 트랜잭션에서 반영한다.
- `RETRY_WAIT`이면 `completed_at = NULL`, `FAILED`이면 DB 시각으로 완료 시각을 기록한다.
- 실패 반영은 Job ID·`source_event_id`·`enqueue_seq`·시도 횟수·epoch를 변경하지 않는다.
- 반영 0행이면 실행권을 잃은 것으로 처리하고 종료한다. 다른 실행의 상태를 덮어쓰지 않는다.
- 커밋 응답 유실은 1번 작업의 DB 복구 흐름에 연결한다. Worker가 이미 해제된 실패 상태를 재조회할 수 있도록 기존 소유자 검증과 구분하며, 이미 반영된 deadline을 다시 계산하거나 미루지 않는다.

## Worker 변경 파일

경로는 `src/main/kotlin/com/osscontest/worker/indexing/` 아래를 기준으로 한다.

| 파일 | 변경 내용 |
|---|---|
| `publication/service/IndexingFailureService.kt` | `decideFailure` 분리, 선형 지터 적용, 실행권 해제를 포함한 실패 반영 호출 |
| `publication/repository/IndexingJobRepository.kt` 또는 통합된 큐 저장소 | 유효 lease를 포함한 조건부 실패 반영과 반영 결과 반환. 저장소 선택은 claim 담당과 먼저 합의 |
| `pipeline/service/IndexingPipelineRunner.kt` | 반복 실행·`awaitRetry()` 제거. 실패 반영 후 반환 |
| `queue/ExecutionOperations.kt` | Job 재시도 대기 제거. DB 장애 후 실행권 확인·대기·재개 유지 |
| `queue/ExternalApiOperations.kt` | 429를 공통 실패 경로로 전달. 인라인 재호출과 재시도 때문에 다른 작업을 막는 표식·대기 제거 |
| `queue/DbJobWorker.kt` | 실패 반영 후 활성 실행 정리·슬롯 반환. 자발적 해제와 heartbeat 경쟁 처리 |
| `pipeline/service/IndexingErrorClassifier.kt` | 생산자 계약 확인 후 명시적인 `NoSuchKey`만 영구 오류로 추가 |
| `pipeline/service/RetryWaiter.kt` | Job 재시도 사용처 제거. `DatabaseOperations`의 장애 대기에 필요한 사용처는 유지 |

관련 테스트는 실패 서비스, runner, 큐 저장소, Worker, 실행 경계, 외부 API 테스트를 갱신한다. 오류 분류 테스트에는 `NoSuchKey`와 일반 스토리지 오류의 구분을 추가한다.

`application.yml`, README 한/영, `docs/PROCESSING_MODEL*.md`, `docs/FAILURE_HANDLING*.md`, `docs/ARCHITECTURE*.md`의 인라인 재시도 설명·상태 전이·설정 주석을 갱신한다. `indexing_inline_retry_total`은 DB 재시도 예약을 나타내는 메트릭으로 변경하고, 최종 실패와 구분한다.

## 다른 담당 영역과 협의할 사항

| 담당 | 필요한 계약·변경 |
|---|---|
| claim | 두 저장소의 실제 사용 경로 통합. `PENDING` 또는 due인 `RETRY_WAIT`을 `enqueue_seq` 순서로 선택. 선행 미종결 Job 검사에는 미래의 `RETRY_WAIT`도 포함. 다른 문서까지 막는 전역 재시도 차단·재시도 우선 정렬 제거. 횟수·epoch는 다음 claim에서 증가 |
| Core·스키마 | `sql/20261012_worker_queue_and_circuit.sql`과 실제 Core 마이그레이션 정렬. `enqueue_seq`·문서 epoch·시각 타입 계약 보완. 정상 실패와 만료 복구에 같은 시도 한도·backoff·지터 설정 적용 |
| Core 만료 복구 | 만료 `PROCESSING`을 일시 실패로 보고 잔여 횟수에 따라 `RETRY_WAIT`/`FAILED` 결정. Worker·lease를 함께 해제. 기존 `RETRY_WAIT`의 저장된 deadline 보존. 시도 횟수를 추가 소비하지 않고, 현재 회수 시 epoch 증가도 다음 claim에서 증가하는 계약과 정렬 |
| Core 생산자 | 객체 업로드 성공 후 Job 등록, 문서 변경과 Job 등록의 트랜잭션 계약, 문서별 등록 순서·`enqueue_seq` 발급 확인 |
| 1번 작업 | 소유권 해제 후 실패 커밋 응답 유실 확인 방식 연결. 기존 DB 대기·조건부 lease 갱신·회수 경쟁·재개 흐름 회귀 방지 |
| 외부 API | 기존 429 `Retry-After`와 선형 지터의 우선순위 확정. 인라인 대기 제거에 따른 성공 임베딩 배치의 메모리 보존 설명 정리 |

### NoSuchKey 계약 확인

Core의 기존 `DocumentService`는 동기 `objectStorage.put()` 성공 후 `document_version`을 저장하고, INSERT 트리거가 outbox를 등록한다. 새 DB Job 생산 경로는 같은 순서를 유지하는지 추가 확인해야 한다.

이 계약을 확인한 뒤 `NoSuchKeyException` 또는 명시적인 S3 오류 코드 `NoSuchKey`만 영구 오류로 분류한다. HTTP 404나 스토리지 예외 전체를 영구 오류로 분류하지 않는다.

## 검증 시나리오

| 시나리오 | 기대 결과 |
|---|---|
| 일시 오류·영구 오류·마지막 시도 | 잔여 횟수와 영구성에 따른 `RETRY_WAIT`/`FAILED`, ±20% 지터 범위 |
| JVM·DB 시각 차이 | DB 기준 deadline과 claim 조건 유지 |
| 정상 실패 반영 | 상태·Worker·lease 함께 변경. 식별자·등록 순서·횟수·epoch 유지 |
| 실패 후 빈 슬롯 | 인라인 대기 없이 다른 문서 처리. 다음 claim에서 횟수·epoch 증가 |
| 미래 재시도 시각의 선행 Job | 같은 문서 후속 Job 차단, 다른 문서는 실행 가능, due 이후 같은 Job 재claim |
| Worker·epoch 불일치 또는 만료 lease | 실패 쓰기 거절, 다른 실행 상태 유지 |
| heartbeat·회수·실패 반영 경쟁 | 유효한 실행만 쓰기 성공. 자발적 해제 후 정상 정리와 슬롯 1회 반환 |
| 실패 커밋 응답 유실·재호출 | 이미 반영된 실패 확인, deadline·횟수 중복 변경 없음 |
| 정상 실패와 만료 복구 | 동일 한도·backoff·지터 정책. 기존 재시도 deadline 보존 |
| 429 | DB 상태를 남기고 슬롯 반환. 다른 문서의 신규 Job 처리 가능 |
| NoSuchKey와 기타 스토리지 오류 | 계약 확인 후 NoSuchKey만 영구 분류. 일반 404·타임아웃·5xx 일괄 영구 처리 없음 |
| DB 장애 | 1번 작업의 대기·재개 유지, 불필요한 Job 시도 횟수 소비 없음 |

정책·분류·runner 종료는 단위 테스트로, SQL 원자성·claim 순서·경쟁·롤백은 전용 DB 통합 테스트로 검증한다. 현재 문서는 테스트 실행 결과가 아니다.

## 적용 순서

- [x] Worker의 두 claim 경로를 `JobQueueStore`로 통합하고 등록 순서·문서 epoch 계약을 반영한다.
- [ ] Core와 생산자·만료 복구 정책 및 공통 설정 전달 방식을 맞춘다.
- [x] 실패 결정·조건부 반영을 구현하고 정책·실행권 단위 테스트를 검증한다.
- [x] 일반 실패·429 인라인 대기를 제거하고 슬롯 반환·heartbeat 경쟁 단위 테스트를 검증한다.
- [x] 한/영 문서와 재시도 예약 메트릭을 갱신한다.
- [ ] 전용 PostgreSQL에서 claim·만료 복구·실패 반영의 통합 회귀 검증을 실행한다.

외부 API circuit breaker는 이번 범위에 포함하지 않는다.

## 구현 시 결정과 배포 의존성

- 429의 `Retry-After`는 선형 지터 deadline의 하한으로 처리한다.
- 실패 커밋 응답 유실은 같은 epoch의 해제 상태를 확인하는 별도 실패 경계로 처리한다.
- Worker가 요구하는 스키마는 `sql/20261012_worker_queue_and_circuit.sql`에 정리했다. Server 마이그레이션은 이 파일을 기준으로 작성하며 Worker는 적용하지 않는다.
- Core 생산자 계약은 새 Job 경로에서 추가 확인이 필요하다. `INDEXING_JOB_AFTER_UPLOAD_CONFIRMED`는 기본 false이며 확인 후 활성화한다.
- Docker 실행 승인 요청이 중단되어 실제 PostgreSQL 통합 검증은 실행하지 못했다.
