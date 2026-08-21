# Track B(장애 복구 확장) 구현 계획

**Goal:** [`2026-08-19-track-b-fault-tolerance-spec.md`](../specs/2026-08-19-track-b-fault-tolerance-spec.md)의 남은 P0~P2 항목(P0-4/배치 리스너 전환은 이미 완료됨, `5f52bd5`/`d4ff1e6`)을 순서대로 구현해, 인덱싱 워커가 DB 장애·손상 파일·대용량 파일·파싱 행(hang)·재시도 예산 초과를 스스로 방어하고 상태를 관측 가능하게 만든다.

**Architecture:** `com.osscontest.worker.indexing` 패키지(`consumer`/`pipeline`/`parsing`/`retrieval`/`publication`) 안에서 기존 `IndexingKafkaListener` → `IndexingPipelineRunner.run()`(인라인 재시도 루프) → `IndexingProcessor`(별도 저장소, 계약만 이 repo에 존재) 흐름을 그대로 유지한 채, 각 실패 지점에 방어 로직과 메트릭을 끼워 넣는다. 새 컴포넌트는 기존 컨벤션(생성자 주입 `@Component`/`@Service`, `@Value`로 설정 주입, 네이티브 `@Modifying @Query`로 `indexing_job` 상태 전이)을 그대로 따른다.

**Tech Stack:** Kotlin 2.3 + Spring Boot 4.1 + Spring Kafka(배치 리스너) + Spring Data JPA(OpenSQL/PostgreSQL 호환, `ddl-auto: none`) + Micrometer(신규 추가) + JUnit5 + mockito-kotlin + AssertJ.

---

## 스펙 대비 커버리지 (Self-Review 겸용)

| 스펙 항목 | 이 계획의 Task | 비고 |
|---|---|---|
| P0-5(a) DB 장애 시 nack | Task 1 | |
| P0-5(b) DbHealthGate pause/resume | Task 2 | |
| P0-2(a) 파일 크기 상한 + P0-1 `FILE_TOO_LARGE` 분류 | Task 3 | |
| P0-1 `CORRUPTED_FILE` 분류 + P0-2(b) 파싱 타임아웃 | Task 4 | 스펙은 별도 항목이지만 같은 호출부(`parser.parse().toList()`)를 건드려 한 Task로 묶음 |
| P0-2(c) 다운로드 스트리밍 | Task 5 | 아래 "P0-2(c) 범위 조정" 참고 — 청커까지의 완전한 스트리밍은 이번 계획에서 제외 |
| P0-3 `max.poll.interval.ms` 900초 상향 | Task 6 | |
| P1-2 메트릭 7종 | Task 7 | `kafka_consumer_lag`는 자동 계측(코드 불필요), `kafka_rebalance_total`은 리밸런스 리스너 신설 |
| P1-1 진행률(`phase`) | Task 8 | DOWNLOADING~EMBEDDING만 — PUBLISHING은 `IndexingProcessor`(이 repo 밖) 몫 |
| P2-2 시각 통일 | Task 9 | 아래 "P2-2 범위 조정" 참고 — `LocalDateTime`→`Instant` 전면 마이그레이션은 제외 |
| P2-5 트레이싱 | Task 10 | `trace_id` DB 저장은 이미 구현돼 있음(`insertIfAbsent`) — MDC 전파만 추가 |
| P0-1 미결 사항(화이트리스트 유지 vs sealed class) | — | 화이트리스트 유지로 확정(대화 중 결정). 코드 변경 없음 — Task 3/4에서 새 예외를 기존 `isRetryable()`의 `when`에 추가하는 것으로 충분 |

**이 계획에서 제외한 항목과 이유**

- **P1-3(Outbox 보정 배치), P2-4(삭제 스윕 무한 재시도 방어)**: 스펙에서 이미 제외 확정(2026-08-19) — P1-3은 `doc-relay` 쪽에 이미 구현됨, P2-4는 발생 가능성이 낮다고 보고 방어하지 않기로 결정.
- **P2-1(DB 페일오버 대응)**: 스펙 자체가 "2차 과제"로 명시. OpenSQL 멀티 노드 클러스터가 실제로 떠 있어야 검증 가능한데, 이 repo에는 그 인프라가 없다. 코드만 먼저 짜면 검증 못 하는 플레이스홀더가 된다 — 인프라가 준비된 뒤 별도 계획으로 분리한다.
- **P2-3(파티션/처리량 튜닝)**: 스펙 본문이 "실측(P1-2) 없이 미리 튜닝하지 않는다"고 명시. Task 7의 메트릭이 실제로 돌기 전까지는 튜닝할 근거 자체가 없다.
- **DB 스키마 변경(phase 컬럼 등)**: 이 repo는 `ddl-auto: none`이고 마이그레이션 파일이 없다(Flyway/Liquibase 디렉토리, `.sql` 파일 전부 없음 — 스키마는 API 서버 쪽 저장소에서 관리). Task 8에서 `phase` 컬럼을 참조하는 코드는 작성하지만, **컬럼 자체는 스펙 §7.2 요청대로 외부에서 먼저 추가돼 있어야 한다.** 이 전제가 없으면 Task 8의 통합 테스트는 실패한다.

### P0-2(c) 범위 조정 — 왜 청커까지는 스트리밍하지 않는가

스펙은 "`Sequence`를 청커까지 그대로 흘린다"고 적었지만, 실제 `FixedTokenChunker.chunk()`(`src/main/kotlin/com/osscontest/worker/indexing/chunking/service/FixedTokenChunker.kt:16`)는 `blocks.joinToString("\n\n") { it.text }`로 전체 블록을 한 번에 이어붙인 뒤 토크나이저에 통째로 넘긴다 — 알고리즘 자체가 전체 텍스트를 동시에 봐야 하므로, 블록 단위 스트리밍은 청커를 처음부터 다시 설계하지 않는 한 불가능하다(청킹 전략 중 기본값이자 유일하게 도달 가능한 게 이 청커다 — PR #4). 이번 계획은 실제로 메모리 절감 효과가 있는 부분(다운로드를 힙 `ByteArray` 대신 임시 파일로 스풀 + 해시를 스트리밍 계산)만 Task 5에서 구현하고, 청커 재설계는 범위 밖으로 둔다.

### P2-2 범위 조정 — 왜 `LocalDateTime`→`Instant` 전면 전환은 제외했는가

시계가 어긋날 수 있는 지점은 `IndexingJobRepository.start()`의 `next_retry_at <= CURRENT_TIMESTAMP` 비교뿐 아니라 인라인 재시도의 남은 대기 시간 계산도 포함한다. `next_retry_at`을 DB 시각 기준으로 기록한 뒤 `Duration.between(LocalDateTime.now(), nextRetryAt)`처럼 앱 시각과 비교하면, 워커 시계가 빠른 경우 재시도 시각 전에 `start()`가 호출되어 `RETRY_WAIT`을 재획득하지 못하고 메시지만 ack될 수 있다. 따라서 Task 9는 실패 기록 시각과 인라인 대기 계산 시각을 모두 `currentDbTimestamp()`로 가져와 DB 시계로 통일한다. 다만 `LocalDateTime`을 `Instant`/`OffsetDateTime`으로 전면 교체하려면 엔티티·리포지토리·서비스 전체와 (외부 관리라 이 repo에서 확인 불가능한) DB 컬럼 타입(`TIMESTAMP` vs `TIMESTAMPTZ`)까지 맞춰야 하므로 이번 범위에서는 제외한다.

---

## File Structure

| 파일 | 상태 | 책임 |
|---|---|---|
| `src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListener.kt` | 수정 | DB 장애 시 nack(Task 1), traceId MDC 전파(Task 10) |
| `src/main/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGate.kt` | 신규 | DB 헬스체크 + 리스너 컨테이너 pause/resume |
| `src/main/kotlin/com/osscontest/worker/indexing/retrieval/FileTooLargeException.kt` | 신규 | 영구 실패 — 다운로드 전 판정 |
| `src/main/kotlin/com/osscontest/worker/indexing/parsing/CorruptedFileException.kt` | 신규 | 영구 실패 — 파서가 열지 못함 |
| `src/main/kotlin/com/osscontest/worker/indexing/parsing/ParseTimeoutException.kt` | 신규 | 재시도 가능 — 파싱 타임아웃 |
| `src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt` | 신규 | 타임아웃 있는 파싱 실행 + IOException 래핑 |
| `src/main/kotlin/com/osscontest/worker/indexing/retrieval/DocumentDownloadClient.kt` | 수정 | 반환 타입 `ByteArray` → `Path`(Task 5) |
| `src/main/kotlin/com/osscontest/worker/indexing/retrieval/S3DocumentDownloadClient.kt` | 수정 | 임시 파일로 스트리밍 다운로드 |
| `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt` | 수정 | Task 3·4·5·8·9 전부 여기 모임 — 실행 단계별 방어/계측/phase 갱신 |
| `src/main/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepository.kt` | 수정 | `updatePhase()`(Task 8), `currentDbTimestamp()`(Task 9) 쿼리 추가 |
| `src/main/kotlin/com/osscontest/worker/indexing/publication/entity/IndexingJobEntity.kt` | 수정 | `phase` 필드 추가(Task 8) |
| `src/main/kotlin/com/osscontest/worker/indexing/publication/service/IndexingFailureService.kt` | 수정 | `indexing_job_failed_total` 계측(Task 7) |
| `src/main/kotlin/com/osscontest/worker/indexing/consumer/RebalanceMetricsListener.kt` | 신규 | `kafka_rebalance_total` 계측 |
| `src/main/resources/application.yml` | 수정 | Task 1·2·3·4·6 설정값 |
| `src/main/resources/logback-spring.xml` | 신규 | `%X{traceId}` 로그 패턴(Task 10) |
| `build.gradle.kts` | 수정 | `spring-boot-starter-actuator`(Micrometer) 추가(Task 7) |

각 파일은 테스트 파일과 나란히 수정된다(경로는 각 Task의 `Files` 참고).

---

## Task 1: DB 장애 시 ack 대신 nack (P0-5-a)

**막는 장애**: DB가 죽어 있는데 `processRecord()`가 모든 예외를 로그만 남기고 삼켜서 ack해버리면, 그 메시지는 다시 오지 않는다 — DB에 아무 기록도 안 남긴 채 이벤트가 조용히 사라진다.

**Files:**
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListener.kt`
- Modify: `src/main/resources/application.yml`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListenerTest.kt`

- [ ] **Step 1: 실패하는 테스트 작성**

`IndexingKafkaListenerTest.kt`에 아래 두 테스트를 추가한다(파일 상단 import에 `org.springframework.dao.DataAccessResourceFailureException`, `java.time.Duration`을 추가):

```kotlin
import org.springframework.dao.DataAccessResourceFailureException
import java.time.Duration
```

```kotlin
    @Test
    fun `DB 장애(DataAccessException)면 ack 대신 배치 전체를 nack한다`() {
        whenever(runner.run(any())).thenThrow(DataAccessResourceFailureException("connection refused"))
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1)))

        listener.onMessage(records, ack)

        verify(ack, never()).acknowledge()
        verify(ack, times(1)).nack(0, Duration.ofSeconds(5))
    }

    @Test
    fun `DB 장애가 아닌 일반 예외는 여전히 ack한다`() {
        whenever(runner.run(any())).thenThrow(RuntimeException("boom"))
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1)))

        listener.onMessage(records, ack)

        verify(ack, times(1)).acknowledge()
        verify(ack, never()).nack(any(), any())
    }
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.IndexingKafkaListenerTest"`
Expected: FAIL — `Acknowledgment.nack(int, Duration)`가 아직 호출되지 않아 첫 번째 테스트가 실패(`ack.acknowledge()`가 대신 불림).

- [ ] **Step 3: `IndexingKafkaListener` 구현**

`IndexingKafkaListener.kt` 전체를 아래로 교체한다:

```kotlin
package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService

// 토픽 이름은 이벤트 타입에 종속되지 않게 짓는다("indexing.requested"가 아니라 "indexing") —
// INDEXING_REQUESTED와 DOCUMENT_DELETED가 같은 토픽·같은 파티션(documentId 키)으로 온다.
// 별도 토픽으로 쪼개면 "업로드 뒤 삭제"류 순서 보장이 깨진다(스펙 §0.3).
@Component
class IndexingKafkaListener(
    private val pipelineRunner: IndexingPipelineRunner,
    private val deletionHandler: DocumentDeletionHandler,
    private val objectMapper: ObjectMapper,
    @Qualifier("indexingBatchExecutor") private val executor: ExecutorService,
    @Value("\${indexing.db-health-gate.pause-nack-delay:PT5S}")
    private val nackDelay: Duration,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // spring.kafka.listener.type=batch + max-poll-records로 배치 크기가 정해진다(application.yml).
    // documentId(=메시지 key)로 그룹핑해 같은 문서의 이벤트는 순서대로, 서로 다른 문서는
    // 동시에 처리한다 — 배치 전체가 끝나야(모든 그룹의 future가 끝나야) 한 번만 ack한다.
    // §3.1의 "Kafka offset은 배치 전체를 봤다, DB status는 각 Job을 처리했다" 원칙.
    @KafkaListener(topics = ["indexing"], id = "indexing")
    fun onMessage(
        records: List<ConsumerRecord<String, String>>,
        ack: Acknowledgment,
    ) {
        val futures =
            records
                .groupBy { it.key() }
                .values
                .map { sameKeyRecords -> executor.submit { sameKeyRecords.forEach(::processRecord) } }
        try {
            futures.forEach { it.get() }
        } catch (e: ExecutionException) {
            // P0-5: DB에 아무것도 기록하지 못한 실패는 ack이 아니라 nack한다 —
            // always-ack 원칙의 유일한 예외(FAULT_TOLERANCE.md §3 P0-5-(c)).
            // 배치 리스너라 레코드 하나가 아니라 배치 전체(인덱스 0부터)가 되감긴다.
            // 이미 성공적으로 처리된 다른 documentId 그룹까지 재전달되지만,
            // UPSERT 수렴(§1.4-(2))으로 무해하다.
            if (e.cause is DataAccessException) {
                log.warn("DB unavailable, nacking whole batch", e.cause)
                ack.nack(0, nackDelay)
                return
            }
            throw e
        }
        ack.acknowledge()
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        var event: IndexingRequestedEvent? = null
        try {
            event = deserialize(record.value())
            when (event.eventType) {
                "INDEXING_REQUESTED" -> pipelineRunner.run(event)
                "DOCUMENT_DELETED" -> deletionHandler.handle(event)
                else ->
                    throw InvalidEventException(
                        "UNKNOWN_EVENT_TYPE",
                        "eventType=${event.eventType} is not recognized",
                    )
            }
        } catch (e: DataAccessException) {
            // 여기서 삼키지 않는다 — onMessage()의 futures.forEach { it.get() }가
            // ExecutionException으로 다시 던지도록 그대로 전파한다(P0-5).
            throw e
        } catch (e: DeserializationException) {
            log.error("event schema invalid: partition={} offset={}", record.partition(), record.offset(), e)
        } catch (e: InvalidEventException) {
            log.error(
                "event validation failed: code={} eventId={} documentId={} partition={} offset={}",
                e.code, event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        } catch (e: Exception) {
            // Throwable/Error(예: StackOverflowError)는 의도적으로 여기서 잡지 않고 그대로 전파한다 —
            // 이 documentId 그룹의 future를 실패시켜 onMessage()의 forEach { it.get() }에서 다시
            // 던져지고, 배치 ack 자체가 안 이뤄진다(워커가 정말 불안정하면 배치를 통째로 재전달받는
            // 게 낫다는 판단).
            log.error(
                "indexing failed: eventId={} documentId={} partition={} offset={}",
                event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        }
    }

    private fun deserialize(value: String): IndexingRequestedEvent =
        try {
            objectMapper.readValue(value, IndexingRequestedEvent::class.java)
        } catch (e: Exception) {
            throw DeserializationException(e)
        }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.IndexingKafkaListenerTest"`
Expected: PASS (기존 5개 + 신규 2개 = 7개 모두 통과)

- [ ] **Step 5: `application.yml`에 nack 지연 설정 추가**

`indexing:` 블록 안, `retry:` 다음에 추가:

```yaml
  db-health-gate:
    check-interval-ms: ${INDEXING_DB_HEALTH_CHECK_INTERVAL_MS:5000}
    pause-nack-delay: ${INDEXING_DB_HEALTH_PAUSE_NACK_DELAY:PT5S}
```

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListener.kt \
        src/main/resources/application.yml \
        src/test/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListenerTest.kt
git commit -m "feat: DB 장애 시 ack 대신 배치 전체를 nack하도록 변경 (P0-5-a)"
```

---

## Task 2: DbHealthGate — DB 상태에 따라 리스너 pause/resume (P0-5-b)

**막는 장애**: Task 1의 nack만 있으면 DB가 계속 죽어 있는 동안 5초마다 같은 배치를 계속 재전달받아 hot loop가 된다. 컨슈머 자체를 pause시켜야 한다.

**Files:**
- Create: `src/main/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGate.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGateTest.kt`

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.osscontest.worker.indexing.consumer

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer

class DbHealthGateTest {
    private val registry: KafkaListenerEndpointRegistry = mock()
    private val jdbcTemplate: JdbcTemplate = mock()
    private val container: MessageListenerContainer = mock()
    private val gate = DbHealthGate(registry, jdbcTemplate)

    @Test
    fun `DB SELECT 1이 실패하고 컨테이너가 실행 중이면 pause한다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java))
            .thenThrow(RuntimeException("connection refused"))
        whenever(container.isRunning).thenReturn(true)

        gate.check()

        verify(container).pause()
        verify(container, never()).resume()
    }

    @Test
    fun `DB가 정상이고 pause 요청 상태면 resume한다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java)).thenReturn(1)
        whenever(container.isPauseRequested).thenReturn(true)

        gate.check()

        verify(container).resume()
        verify(container, never()).pause()
    }

    @Test
    fun `DB가 정상이고 pause 상태가 아니면 아무것도 하지 않는다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java)).thenReturn(1)
        whenever(container.isPauseRequested).thenReturn(false)

        gate.check()

        verify(container, never()).pause()
        verify(container, never()).resume()
    }

    @Test
    fun `컨테이너를 찾지 못하면 아무것도 하지 않는다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(null)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java)).thenReturn(1)

        gate.check()
        // 예외 없이 조용히 반환하면 통과 — verify할 대상 자체가 없다.
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.DbHealthGateTest"`
Expected: FAIL — `DbHealthGate` 클래스가 없어 컴파일 자체가 실패한다.

- [ ] **Step 3: `DbHealthGate` 구현**

```kotlin
package com.osscontest.worker.indexing.consumer

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// P0-5-b: DB가 흔들리는 동안 리스너를 pause해 Task 1의 nack이 5초마다 같은 배치를
// 재전달받는 hot loop가 되는 걸 막는다. pause()는 poll()만 멈추고 heartbeat/session은
// 그대로 유지되므로 리밸런스가 나지 않는다(FAULT_TOLERANCE.md §3 P0-5-(b)).
@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${indexing.db-health-gate.check-interval-ms:5000}")
    fun check() {
        val healthy = runCatching { jdbcTemplate.queryForObject("SELECT 1", Int::class.java) }.isSuccess
        val container = registry.getListenerContainer(LISTENER_ID) ?: return
        when {
            !healthy && container.isRunning && !container.isPauseRequested -> {
                container.pause()
                log.warn("DB down — consumer paused")
            }
            healthy && container.isPauseRequested -> {
                container.resume()
                log.info("DB up — consumer resumed")
            }
        }
    }

    private companion object {
        const val LISTENER_ID = "indexing"
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.DbHealthGateTest"`
Expected: PASS (4개 모두)

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGate.kt \
        src/test/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGateTest.kt
git commit -m "feat: DB 헬스체크로 Kafka 리스너를 pause/resume하는 DbHealthGate 추가 (P0-5-b)"
```

---

## Task 3: 파일 크기 상한 — `FILE_TOO_LARGE` (P0-2-a, P0-1)

**막는 장애**: 상한 없이 대용량 파일을 그대로 받으면 OOM으로 fleet 전체가 죽을 수 있다. 다운로드 전에 걸러야 S3 트래픽도 아낀다.

**전제 확인**: `document_version.file_size` 컬럼은 이미 실제 DB에 존재한다 — `IndexingPipelineRunnerIntegrationTest.kt:60`의 시드 INSERT문에 `file_size` 컬럼이 이미 쓰이고 있다. `DocumentVersionEntity`에만 매핑이 빠져 있다.

**Files:**
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/publication/entity/DocumentVersionEntity.kt`
- Create: `src/main/kotlin/com/osscontest/worker/indexing/retrieval/FileTooLargeException.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt`
- Modify: `src/main/resources/application.yml`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt`

- [ ] **Step 1: `DocumentVersionEntity`에 `fileSize` 필드 추가**

`DocumentVersionEntity.kt`의 마지막 생성자 파라미터 뒤에 추가(기본값을 둬서 기존 호출부가 안 깨지게 한다):

```kotlin
    @Column(name = "embedding_version_no", nullable = false)
    var embeddingVersionNo: Long,
    @Column(name = "file_size", nullable = false)
    var fileSize: Long = 0,
)
```

- [ ] **Step 2: `FileTooLargeException` 작성**

```kotlin
package com.osscontest.worker.indexing.retrieval

class FileTooLargeException(
    val actualBytes: Long,
    val maxBytes: Long,
) : RuntimeException("file size $actualBytes exceeds max $maxBytes") {
    val code: String = "FILE_TOO_LARGE"
}
```

- [ ] **Step 3: 실패하는 테스트 작성**

`IndexingPipelineRunnerTest.kt`의 `newRunner()`에 새 파라미터를 추가해야 컴파일이 되므로, 먼저 헬퍼와 테스트를 같이 고친다. `maxFileSizeBytes` 필드와 `newRunner()` 시그니처를 아래로 바꾼다:

```kotlin
    private val maxAttempts = 5
    private val baseDelay: Duration = Duration.ofSeconds(30)
    private val maxFileSizeBytes = 209_715_200L // 200MB — 스펙 기본값

    private val runner = newRunner()

    private fun newRunner(strategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN) =
        IndexingPipelineRunner(
            indexingJobRepository = indexingJobRepository,
            documentRepository = documentRepository,
            eventValidator = eventValidator,
            downloadClient = downloadClient,
            parserRegistry = parserRegistry,
            chunkingService = chunkingService,
            chunkGuard = chunkGuard,
            indexingProcessor = indexingProcessor,
            indexingFailureService = indexingFailureService,
            workerId = "worker-test",
            maxAttempts = maxAttempts,
            baseDelay = baseDelay,
            maxFileSizeBytes = maxFileSizeBytes,
            chunkingStrategy = strategy,
        )
```

그 아래에 새 테스트를 추가한다:

```kotlin
    @Test
    fun `문서 크기가 상한을 넘으면 다운로드 전에 FILE_TOO_LARGE로 종결한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L, fileSize = maxFileSizeBytes + 1,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L), eq("FILE_TOO_LARGE"), any(), eq(true), eq(maxAttempts), eq(baseDelay), any(),
        )
        verify(downloadClient, never()).download(any())
        assertThat(indexingProcessor.calls).isEmpty()
    }
```

- [ ] **Step 4: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: FAIL — `IndexingPipelineRunner` 생성자에 `maxFileSizeBytes` 파라미터가 없어 컴파일 실패.

- [ ] **Step 5: `IndexingPipelineRunner`에 파일 크기 체크 추가**

생성자에 파라미터 추가(`baseDelay` 다음, `chunkingStrategy` 앞):

```kotlin
    @Value("\${indexing.retry.base-delay}")
    private val baseDelay: Duration,
    @Value("\${indexing.limits.max-file-size-bytes}")
    private val maxFileSizeBytes: Long,
```

`isRetryable()`과 `errorCodeOf()`에 케이스 추가:

```kotlin
    private fun isRetryable(e: Exception): Boolean =
        when (e) {
            is InvalidEventException -> false
            is ContentIntegrityException -> false
            is EmptyExtractionException -> false
            is ChunkLimitExceededException -> false
            is TotalTokenLimitExceededException -> false
            is UnsupportedMimeTypeException -> false
            is com.osscontest.worker.indexing.retrieval.FileTooLargeException -> false
            else -> true
        }

    private fun errorCodeOf(e: Exception): String =
        when (e) {
            is InvalidEventException -> e.code
            is ContentIntegrityException -> e.code
            is EmptyExtractionException -> e.code
            is ChunkLimitExceededException -> e.code
            is TotalTokenLimitExceededException -> e.code
            is UnsupportedMimeTypeException -> e.code
            is com.osscontest.worker.indexing.retrieval.FileTooLargeException -> e.code
            else -> e::class.simpleName ?: "INDEXING_ERROR"
        }
```

파일 상단 import에 `import com.osscontest.worker.indexing.retrieval.FileTooLargeException`을 추가하고, 위 두 `when` 절의 전체 경로(`com.osscontest.worker.indexing.retrieval.FileTooLargeException`)는 짧은 이름(`FileTooLargeException`)으로 바꾼다.

`processAcquiredJob()` 맨 앞, `downloadClient.download(...)` 호출 전에 체크를 추가한다:

```kotlin
    private fun processAcquiredJob(
        jobId: Long,
        event: IndexingRequestedEvent,
        documentVersion: DocumentVersionEntity,
    ) {
        if (documentVersion.fileSize > maxFileSizeBytes) {
            throw FileTooLargeException(documentVersion.fileSize, maxFileSizeBytes)
        }

        // 여기서 searchable 버전과 비교해 미리 건너뛰지 않는다 — ...(기존 주석 유지)
        val bytes = downloadClient.download(documentVersion.sourceObjectKey)
        // ... 이하 기존 코드 그대로
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: PASS (기존 10개 + 신규 1개 = 11개 모두 통과)

- [ ] **Step 7: `application.yml`에 설정 추가**

`indexing:` 블록에 새 최상위 섹션 추가(`chunking:` 앞이나 뒤 어디든):

```yaml
  limits:
    max-file-size-bytes: ${INDEXING_MAX_FILE_SIZE_BYTES:209715200}
```

- [ ] **Step 8: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/publication/entity/DocumentVersionEntity.kt \
        src/main/kotlin/com/osscontest/worker/indexing/retrieval/FileTooLargeException.kt \
        src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt \
        src/main/resources/application.yml \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt
git commit -m "feat: 파일 크기 상한 초과 시 다운로드 전 FILE_TOO_LARGE로 종결 (P0-2-a)"
```

---

## Task 4: 파싱 타임아웃 + 손상 파일 가드 (P0-2-b, P0-1)

**막는 장애**: 악성/손상 PDF가 파서를 무한 루프에 빠뜨리면 `max.poll.interval.ms`를 넘겨 불필요한 리밸런스가 난다. 반대로 그냥 손상된(빈, 잘린) 파일은 재시도해도 100% 다시 실패하므로 즉시 종결해야 한다.

**발견된 실제 코드 간극**: `PdfDocumentParser.parse()`(`src/main/kotlin/com/osscontest/worker/indexing/parsing/PdfDocumentParser.kt:17`)의 `Loader.loadPDF(...)`는 손상된 PDF에 `IOException`을 던지는데, 이게 현재 `IndexingPipelineRunner.isRetryable()`의 화이트리스트에 없어 **재시도 가능으로 잘못 분류되고 있다**(스펙 P0-1의 `CORRUPTED_FILE` 요구사항과 다름). 이번 Task가 같이 고친다.

**Files:**
- Create: `src/main/kotlin/com/osscontest/worker/indexing/parsing/CorruptedFileException.kt`
- Create: `src/main/kotlin/com/osscontest/worker/indexing/parsing/ParseTimeoutException.kt`
- Create: `src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt`
- Modify: `src/main/resources/application.yml`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt` (신규)
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt`

- [ ] **Step 1: 예외 두 개 작성**

```kotlin
package com.osscontest.worker.indexing.parsing

class CorruptedFileException(
    mimeType: String,
    cause: Throwable,
) : RuntimeException("Failed to parse document (mimeType=$mimeType): ${cause.message}", cause) {
    val code: String = "CORRUPTED_FILE"
}
```

```kotlin
package com.osscontest.worker.indexing.parsing

import java.time.Duration

class ParseTimeoutException(
    mimeType: String,
    timeout: Duration,
) : RuntimeException("Parsing timed out after $timeout (mimeType=$mimeType)") {
    val code: String = "PARSE_TIMEOUT"
}
```

- [ ] **Step 2: `ParsingTimeoutGuard`의 실패하는 테스트 작성**

```kotlin
package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.IOException
import java.time.Duration

class ParsingTimeoutGuardTest {
    @Test
    fun `제한시간 안에 끝나면 정상적으로 결과를 반환한다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(500), concurrency = 2)
        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))

        val result = guard.parse(parser, "x".toByteArray(), "text/plain")

        assertThat(result).containsExactly(block)
    }

    @Test
    fun `제한시간을 넘기면 ParseTimeoutException을 던진다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(100), concurrency = 2)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )

        assertThrows<ParseTimeoutException> { guard.parse(parser, "x".toByteArray(), "text/plain") }
    }

    @Test
    fun `파싱 중 IOException이 나면 CorruptedFileException으로 감싼다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofSeconds(5), concurrency = 2)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(sequence { throw IOException("bad pdf") })

        assertThrows<CorruptedFileException> { guard.parse(parser, "x".toByteArray(), "application/pdf") }
    }
}
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.parsing.ParsingTimeoutGuardTest"`
Expected: FAIL — `ParsingTimeoutGuard` 클래스가 없어 컴파일 실패.

- [ ] **Step 4: `ParsingTimeoutGuard` 구현**

```kotlin
package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.IOException
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

// P0-2-b: 파싱이 hang되면 max.poll.interval.ms를 넘겨 불필요한 리밸런스가 난다. 전용
// 스레드풀에서 파싱을 실행하고 시간 안에 못 끝나면 future를 취소한 뒤 ParseTimeoutException을
// 던진다(재시도 가능 — isRetryable() 기본값을 그대로 따른다, 별도 화이트리스트 등록 불필요).
// IOException(손상 파일)은 CorruptedFileException(영구 실패)으로 여기서 감싼다.
@Component
class ParsingTimeoutGuard(
    @Value("\${indexing.limits.parse-timeout:PT60S}")
    private val parseTimeout: Duration,
    @Value("\${indexing.consumer.concurrency:5}")
    concurrency: Int,
) {
    // IndexingBatchExecutorConfig의 executor(문서 처리 자체)와는 다른 풀이다 — 파싱이
    // 걸려도 취소된 스레드가 이 풀에만 누적된다(parse_thread_leaked, P1-2).
    private val parseExecutor = Executors.newFixedThreadPool(concurrency)

    fun parse(
        parser: DocumentParser,
        bytes: ByteArray,
        mimeType: String,
    ): List<ParsedBlock> {
        val future =
            parseExecutor.submit(
                Callable {
                    try {
                        parser.parse(bytes.inputStream()).toList()
                    } catch (e: IOException) {
                        throw CorruptedFileException(mimeType, e)
                    }
                },
            )
        return try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            throw ParseTimeoutException(mimeType, parseTimeout)
        } catch (e: ExecutionException) {
            throw (e.cause as? Exception) ?: e
        }
    }

    @PreDestroy
    fun shutdown() {
        parseExecutor.shutdown()
    }
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.parsing.ParsingTimeoutGuardTest"`
Expected: PASS (3개 모두)

- [ ] **Step 6: `IndexingPipelineRunner`가 `ParsingTimeoutGuard`를 쓰도록 실패하는 테스트 작성**

`IndexingPipelineRunnerTest.kt`의 import에 `import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard`를 추가하고, mock 선언과 `newRunner()`를 아래로 바꾼다:

```kotlin
    private val parserRegistry: DocumentParserRegistry = mock()
    private val parsingTimeoutGuard: ParsingTimeoutGuard = mock()
    private val chunkingService: ChunkingService = mock()
```

```kotlin
    private fun newRunner(strategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN) =
        IndexingPipelineRunner(
            indexingJobRepository = indexingJobRepository,
            documentRepository = documentRepository,
            eventValidator = eventValidator,
            downloadClient = downloadClient,
            parserRegistry = parserRegistry,
            parsingTimeoutGuard = parsingTimeoutGuard,
            chunkingService = chunkingService,
            chunkGuard = chunkGuard,
            indexingProcessor = indexingProcessor,
            indexingFailureService = indexingFailureService,
            workerId = "worker-test",
            maxAttempts = maxAttempts,
            baseDelay = baseDelay,
            maxFileSizeBytes = maxFileSizeBytes,
            chunkingStrategy = strategy,
        )
```

기존 테스트들이 쓰던 `whenever(parser.parse(any())).thenReturn(sequenceOf(block))` + `parserRegistry.findParser(...)` 스텁 대신, **`parsingTimeoutGuard.parse(...)`를 직접 스텁**하도록 아래 세 테스트를 고친다(파서 mock 자체는 더 이상 필요 없다):

`이미 더 최신 버전이 searchable이어도 임베딩까지 끝까지 처리한다`, `정상 흐름이면 다운로드부터 청킹까지 수행하고 IndexingProcessor를 호출한다`, `청킹 전략은 설정값을 따른다`, `IndexingProcessor가 예외를 던지면 run은 예외를 삼키고 recordFailure를 호출한다` — 이 네 테스트에서 아래 세 줄을 지운다:

```kotlin
        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
```

대신 아래로 바꾼다(파서 mock 자체는 `parserRegistry.findParser`가 반환할 값으로만 필요하다):

```kotlin
        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(parser, bytes, "text/plain")).thenReturn(listOf(block))
```

그리고 이 네 테스트 안의 `chunkingService.chunk(eq(listOf(block)), ...)` 스텁/검증은 그대로 둔다(청킹은 여전히 `List<ParsedBlock>`을 받는다 — "P0-2(c) 범위 조정" 참고).

- [ ] **Step 7: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: FAIL — `IndexingPipelineRunner` 생성자에 `parsingTimeoutGuard` 파라미터가 없어 컴파일 실패.

- [ ] **Step 8: `IndexingPipelineRunner` 수정**

생성자에 `parserRegistry` 다음, `chunkingService` 앞에 파라미터 추가:

```kotlin
    private val parserRegistry: DocumentParserRegistry,
    private val parsingTimeoutGuard: ParsingTimeoutGuard,
    private val chunkingService: ChunkingService,
```

`isRetryable()`/`errorCodeOf()`에 `CorruptedFileException` 케이스 추가(순서는 상관없다):

```kotlin
            is CorruptedFileException -> false
```
```kotlin
            is CorruptedFileException -> e.code
```

`processAcquiredJob()` 안의 파싱 두 줄을 교체한다:

```kotlin
        val parser = parserRegistry.findParser(documentVersion.mimeType)
        val blocks = parser.parse(bytes.inputStream()).toList()
```

```kotlin
        val parser = parserRegistry.findParser(documentVersion.mimeType)
        val blocks = parsingTimeoutGuard.parse(parser, bytes, documentVersion.mimeType)
```

파일 상단 import에 `import com.osscontest.worker.indexing.parsing.CorruptedFileException`과 `import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard`를 추가한다.

- [ ] **Step 9: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: PASS (11개 전부)

- [ ] **Step 10: `application.yml`에 설정 추가**

`indexing.limits:` 블록(Task 3에서 만든)에 한 줄 추가:

```yaml
  limits:
    max-file-size-bytes: ${INDEXING_MAX_FILE_SIZE_BYTES:209715200}
    parse-timeout: ${INDEXING_PARSE_TIMEOUT:PT60S}
```

- [ ] **Step 11: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/parsing/CorruptedFileException.kt \
        src/main/kotlin/com/osscontest/worker/indexing/parsing/ParseTimeoutException.kt \
        src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt \
        src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt \
        src/main/resources/application.yml \
        src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt
git commit -m "feat: 파싱 타임아웃 가드 도입 및 손상 파일(CORRUPTED_FILE) 영구 실패 분류 추가 (P0-2-b, P0-1)"
```

---

## Task 5: 다운로드를 임시 파일 스트리밍으로 전환 (P0-2-c)

**막는 장애**: 현재 `S3DocumentDownloadClient.download()`가 파일 전체를 힙 `ByteArray`로 반환하고, `IndexingPipelineRunner`가 `bytes.inputStream()` + `MessageDigest.digest(bytes)`로 또 한 번 참조를 들고 있어 대용량 문서에서 OOM 경로가 여러 겹이다.

**범위**: "P0-2(c) 범위 조정" 절 참고 — 다운로드~해시 계산까지만 스트리밍하고, 청킹은 여전히 `List<ParsedBlock>`을 받는다.

**Files:**
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/retrieval/DocumentDownloadClient.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/retrieval/S3DocumentDownloadClient.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerIntegrationTest.kt`

- [ ] **Step 1: 인터페이스 변경**

```kotlin
package com.osscontest.worker.indexing.retrieval

import java.nio.file.Path

interface DocumentDownloadClient {
    /**
     * 반환된 [Path]는 호출자가 다 쓴 뒤 반드시 삭제해야 하는 임시 파일이다.
     * AWS SDK exceptions (e.g. [software.amazon.awssdk.services.s3.model.NoSuchKeyException],
     * [software.amazon.awssdk.core.exception.SdkClientException]) propagate unmodified — this is
     * an intentional contract, not an oversight; error handling is deferred to the pipeline runner.
     */
    fun download(objectKey: String): Path
}
```

- [ ] **Step 2: `S3DocumentDownloadClient`가 임시 파일로 스트리밍하도록 변경**

```kotlin
package com.osscontest.worker.indexing.retrieval

import com.osscontest.worker.indexing.retrieval.config.StorageProperties
import org.springframework.stereotype.Component
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.nio.file.Files
import java.nio.file.Path

@Component
class S3DocumentDownloadClient(
    private val s3Client: S3Client,
    private val properties: StorageProperties,
) : DocumentDownloadClient {
    override fun download(objectKey: String): Path {
        val request =
            GetObjectRequest.builder()
                .bucket(properties.bucket)
                .key(objectKey)
                .build()
        val tempFile = Files.createTempFile("indexing-download-", ".tmp")
        s3Client.getObject(request, ResponseTransformer.toFile(tempFile))
        return tempFile
    }
}
```

- [ ] **Step 3: `ParsingTimeoutGuard`가 `Path`를 받도록 변경**

`ParsingTimeoutGuardTest.kt`를 먼저 고친다 — `import java.nio.file.Files`를 추가하고, 각 테스트의 `guard.parse(parser, "x".toByteArray(), "text/plain")` 호출을 임시 파일 기반으로 바꾼다:

```kotlin
import java.nio.file.Files
```

```kotlin
    @Test
    fun `제한시간 안에 끝나면 정상적으로 결과를 반환한다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(500), concurrency = 2)
        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        val result = guard.parse(parser, path, "text/plain")

        assertThat(result).containsExactly(block)
    }

    @Test
    fun `제한시간을 넘기면 ParseTimeoutException을 던진다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(100), concurrency = 2)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<ParseTimeoutException> { guard.parse(parser, path, "text/plain") }
    }

    @Test
    fun `파싱 중 IOException이 나면 CorruptedFileException으로 감싼다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofSeconds(5), concurrency = 2)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(sequence { throw IOException("bad pdf") })
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<CorruptedFileException> { guard.parse(parser, path, "application/pdf") }
    }
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.parsing.ParsingTimeoutGuardTest"`
Expected: FAIL — `ParsingTimeoutGuard.parse(DocumentParser, Path, String)` 오버로드가 없어 컴파일 실패.

`ParsingTimeoutGuard.kt`의 `parse()` 시그니처와 본문을 아래로 바꾼다(`import java.io.IOException` 아래 `import java.nio.file.Files`, `import java.nio.file.Path` 추가, `bytes: ByteArray` 파라미터를 `path: Path`로):

```kotlin
    fun parse(
        parser: DocumentParser,
        path: Path,
        mimeType: String,
    ): List<ParsedBlock> {
        val future =
            parseExecutor.submit(
                Callable {
                    try {
                        Files.newInputStream(path).use { parser.parse(it).toList() }
                    } catch (e: IOException) {
                        throw CorruptedFileException(mimeType, e)
                    }
                },
            )
        return try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            throw ParseTimeoutException(mimeType, parseTimeout)
        } catch (e: ExecutionException) {
            throw (e.cause as? Exception) ?: e
        }
    }
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.parsing.ParsingTimeoutGuardTest"`
Expected: PASS (3개)

- [ ] **Step 4: `IndexingPipelineRunner`가 임시 파일을 다운로드·해시·삭제하도록 변경**

`IndexingPipelineRunnerTest.kt`에 `tempFileOf` 헬퍼를 추가하고, `downloadClient.download("k")` 스텁 6곳을 전부 `tempFileOf(bytes)`(또는 `tempFileOf("corrupted content".toByteArray())`)로 바꾼다. 파일 하단 import에 `import java.nio.file.Files`, `import java.nio.file.Path`를 추가하고, `sha256Hex` 헬퍼 아래에 추가한다:

```kotlin
    private fun tempFileOf(bytes: ByteArray): Path {
        val path = Files.createTempFile("test-download-", ".tmp")
        Files.write(path, bytes)
        return path
    }
```

`whenever(downloadClient.download("k")).thenReturn(bytes)`가 나오는 5개 테스트(`이미 더 최신 버전이...`, `정상 흐름이면...`, `IndexingProcessor가 예외를...`, `청킹 전략은...`)와, `콘텐츠 해시가 일치하지 않으면...` 테스트의 `whenever(downloadClient.download("k")).thenReturn("corrupted content".toByteArray())`를 각각 `tempFileOf(bytes)` / `tempFileOf("corrupted content".toByteArray())`로 바꾼다. 같은 이유로 `parsingTimeoutGuard.parse(parser, bytes, "text/plain")` 스텁의 `bytes`도 실제로는 쓰이지 않으므로(파일 경로 매칭은 mockito가 참조 동일성으로 비교하지 않고 실제 값으로 비교하는데, Path는 매번 새로 생성되므로) `any()`로 완화한다:

```kotlin
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: FAIL — `downloadClient.download()`가 아직 `ByteArray`를 반환해 컴파일 실패.

`IndexingPipelineRunner.kt`의 `processAcquiredJob()`을 아래로 교체한다:

```kotlin
    private fun processAcquiredJob(
        jobId: Long,
        event: IndexingRequestedEvent,
        documentVersion: DocumentVersionEntity,
    ) {
        if (documentVersion.fileSize > maxFileSizeBytes) {
            throw FileTooLargeException(documentVersion.fileSize, maxFileSizeBytes)
        }

        // 여기서 searchable 버전과 비교해 미리 건너뛰지 않는다 — 이전 버전으로 되돌리기 기능이
        // 그 버전의 청크/임베딩이 실제로 저장돼 있어야 성립하므로, embedding_version_no가 더
        // 작다고 해서 임베딩 자체를 스킵하면 안 된다(되돌릴 때마다 처음부터 재인덱싱해야 함).
        // "최신 아닌 버전이 검색을 덮어쓰지 않는다"는 보장은 여기가 아니라 §1.4-(3)의
        // searchable_version_id 승격 UPDATE(embedding_version_no 비교 후 조건부 갱신)에서
        // 맡는다 — 그 UPDATE는 IndexingProcessor.process() 안에서 일어난다. 즉 "항상 임베딩은
        // 하되 승격만 안 한다"가 여기의 계약이다.
        val tempFile = downloadClient.download(documentVersion.sourceObjectKey)
        try {
            val actualHash = "sha256:" + sha256HexOf(tempFile)
            if (actualHash != documentVersion.contentHash) {
                throw ContentIntegrityException(documentVersion.contentHash, actualHash)
            }

            val parser = parserRegistry.findParser(documentVersion.mimeType)
            val blocks = parsingTimeoutGuard.parse(parser, tempFile, documentVersion.mimeType)

            val chunks = chunkingService.chunk(blocks, chunkingStrategy)
            chunkGuard.assertValid(chunks)

            val context =
                IndexingContext(
                    jobId = jobId,
                    documentId = event.documentId,
                    documentVersionId = documentVersion.id,
                    versionNo = documentVersion.versionNo,
                    extractedMetadata = null,
                )

            indexingProcessor.process(context, chunks)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }
```

`sha256Hex(bytes: ByteArray)`를 `sha256HexOf(path: Path)`로 교체한다:

```kotlin
    private fun sha256HexOf(path: Path): String {
        val digest = MessageDigest.getInstance("SHA-256")
        DigestInputStream(Files.newInputStream(path), digest).use { stream ->
            val buffer = ByteArray(8192)
            while (stream.read(buffer) != -1) {
                // 읽기만 하면 DigestInputStream이 내부적으로 digest를 갱신한다.
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }
```

파일 상단 import에 `java.io.DigestInputStream`, `java.nio.file.Files`, `java.nio.file.Path`를 추가한다.

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: PASS (11개 전부)

- [ ] **Step 6: 통합 테스트도 새 반환 타입에 맞춘다**

`IndexingPipelineRunnerIntegrationTest.kt`의 아래 줄:

```kotlin
        whenever(downloadClient.download("docs/900001/v1.txt")).thenReturn("hello world".toByteArray())
```

을 아래로 바꾼다(파일 상단 import에 `java.nio.file.Files` 추가):

```kotlin
        whenever(downloadClient.download("docs/900001/v1.txt")).thenAnswer {
            Files.createTempFile("integration-test-", ".tmp")
                .also { Files.write(it, "hello world".toByteArray()) }
        }
```

러너는 매 시도 후 임시 파일을 삭제하므로, 인라인 재시도를 검증하는 mock도 실제 다운로드
클라이언트처럼 호출할 때마다 새 파일을 반환해야 한다.

Run: `./gradlew compileTestKotlin`
Expected: 컴파일 성공(이 테스트는 `@Tag("integration")`이라 실제 Postgres 없이는 실행까지는 안 되지만, 최소한 컴파일은 통과해야 한다).

- [ ] **Step 7: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/retrieval/DocumentDownloadClient.kt \
        src/main/kotlin/com/osscontest/worker/indexing/retrieval/S3DocumentDownloadClient.kt \
        src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt \
        src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt \
        src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerIntegrationTest.kt
git commit -m "refactor: 다운로드를 힙 ByteArray 대신 임시 파일 스트리밍으로 전환 (P0-2-c)"
```

---

## Task 6: `max.poll.interval.ms` 900초로 상향 (P0-3)

**막는 장애**: 인라인 재시도 5회(선형 백오프 30+60+90+120=300초) + 처리 5회(120초×5=600초) = 900초가 필요한데, 현재 설정은 600초라 예산 초과로 리밸런스가 날 수 있다.

**Files:**
- Modify: `src/main/resources/application.yml`
- Test: `src/test/kotlin/com/osscontest/worker/config/ApplicationYmlConfigTest.kt` (신규)

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.osscontest.worker.config

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.io.ClassPathResource
import kotlin.test.assertEquals

class ApplicationYmlConfigTest {
    @Test
    fun `max poll interval ms는 900초(900000)로 설정돼 있다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertEquals("900000", props.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms"))
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.config.ApplicationYmlConfigTest"`
Expected: FAIL — 현재 값은 `600000`.

- [ ] **Step 3: `application.yml` 수정**

```yaml
      properties:
        max.poll.interval.ms: 900000
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.config.ApplicationYmlConfigTest"`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/resources/application.yml \
        src/test/kotlin/com/osscontest/worker/config/ApplicationYmlConfigTest.kt
git commit -m "fix: max.poll.interval.ms를 600초에서 900초로 상향 (P0-3, 재시도 예산 확보)"
```

---

## Task 7: 메트릭 계측 (P1-2)

**막는 장애**: 지금 상태가 실제로 안전한지 확인할 유일한 수단. 특히 `kafka_rebalance_total`이 오르면 P0-3의 예산 계산이 틀렸다는 뜻이고, `indexing_job_duration_seconds` p99가 120초를 넘으면 P0-2의 리소스 가드 전제가 깨졌다는 뜻이다.

**7개 중 `kafka_consumer_lag{partition}`은 코드가 필요 없다** — `spring-boot-starter-actuator`를 추가하면 Spring for Apache Kafka가 `MeterRegistry` 빈을 감지해 컨슈머 클라이언트 메트릭(랙 포함)을 자동으로 Micrometer에 등록한다.

**Files:**
- Modify: `build.gradle.kts`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/publication/service/IndexingFailureService.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGate.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt`
- Create: `src/main/kotlin/com/osscontest/worker/indexing/consumer/RebalanceMetricsListener.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/publication/service/IndexingFailureServiceTest.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGateTest.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/consumer/RebalanceMetricsListenerTest.kt` (신규)

- [ ] **Step 1: Micrometer 의존성 추가**

`build.gradle.kts`의 `dependencies` 블록, `spring-boot-starter-kafka` 다음에 추가:

```kotlin
    implementation("org.springframework.boot:spring-boot-starter-actuator")
```

Run: `./gradlew build -x test`
Expected: 성공 (의존성만 추가한 상태이므로 기존 코드는 영향 없음)

- [ ] **Step 2: `indexing_job_failed_total{errorCode}` — `IndexingFailureService`**

기존 `IndexingFailureServiceTest.kt`가 있는지 먼저 확인한다: `find src/test -iname "IndexingFailureServiceTest.kt"`. 없다면 아래로 새로 만들고, 있다면 아래 두 테스트를 추가한다.

```kotlin
package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID

class IndexingFailureServiceTest {
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val service = IndexingFailureService(indexingJobRepository, meterRegistry)

    private fun processingJob() =
        IndexingJobEntity(
            id = 1L, sourceEventId = UUID.randomUUID(), documentId = 1L, documentVersionId = 1L,
            status = IndexingJobStatus.PROCESSING, attemptCount = 5, nextRetryAt = null, workerId = "w",
            lastErrorCode = null, lastErrorMessage = null, traceId = null, startedAt = null,
            completedAt = null, updatedAt = LocalDateTime.now(),
        )

    @Test
    fun `영구 실패로 FAILED 종결되면 indexing_job_failed_total이 errorCode 태그로 증가한다`() {
        whenever(indexingJobRepository.findByIdForUpdate(1L)).thenReturn(processingJob())

        service.recordFailure(
            jobId = 1L, errorCode = "HASH_MISMATCH", errorMessage = "mismatch", permanent = true,
            maxAttempts = 5, baseDelay = Duration.ofSeconds(30), failedAt = LocalDateTime.now(),
        )

        assertThat(meterRegistry.get("indexing_job_failed_total").tag("errorCode", "HASH_MISMATCH").counter().count())
            .isEqualTo(1.0)
    }

    @Test
    fun `RETRY_WAIT으로 끝나면 indexing_job_failed_total이 증가하지 않는다`() {
        whenever(indexingJobRepository.findByIdForUpdate(1L)).thenReturn(processingJob().apply { attemptCount = 1 })

        service.recordFailure(
            jobId = 1L, errorCode = "TIMEOUT", errorMessage = "timeout", permanent = false,
            maxAttempts = 5, baseDelay = Duration.ofSeconds(30), failedAt = LocalDateTime.now(),
        )

        assertThat(meterRegistry.find("indexing_job_failed_total").counters()).isEmpty()
    }
}
```

기존 테스트 파일이 이미 있었다면(생성자에 `meterRegistry`가 없는 버전), 그 파일의 생성자 호출부에도 `SimpleMeterRegistry()`를 추가해야 컴파일된다.

Run: `./gradlew test --tests "com.osscontest.worker.indexing.publication.service.IndexingFailureServiceTest"`
Expected: FAIL — `IndexingFailureService` 생성자에 `meterRegistry` 파라미터가 없어 컴파일 실패.

`IndexingFailureService.kt`를 아래로 수정한다:

```kotlin
package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.LocalDateTime

@Service
class IndexingFailureService(
    private val indexingJobRepository: IndexingJobRepository,
    private val meterRegistry: MeterRegistry,
) {
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun recordFailure(
        jobId: Long,
        errorCode: String,
        errorMessage: String,
        permanent: Boolean,
        maxAttempts: Int,
        baseDelay: Duration,
        failedAt: LocalDateTime,
    ): IndexingJobStatus {
        val job =
            checkNotNull(indexingJobRepository.findByIdForUpdate(jobId)) {
                "Indexing job $jobId does not exist"
            }
        if (job.status != IndexingJobStatus.PROCESSING) {
            return job.status
        }

        job.lastErrorCode = errorCode.take(MAX_ERROR_CODE_LENGTH)
        job.lastErrorMessage = errorMessage.take(MAX_ERROR_MESSAGE_LENGTH)
        job.updatedAt = failedAt

        return if (permanent || job.attemptCount >= maxAttempts) {
            job.status = IndexingJobStatus.FAILED
            job.nextRetryAt = null
            job.completedAt = failedAt
            // P1-2: DLQ가 없는 이 설계에서 실패를 감지하는 핵심 지표(FAULT_TOLERANCE.md §3 P1-2).
            meterRegistry.counter("indexing_job_failed_total", "errorCode", job.lastErrorCode!!).increment()
            IndexingJobStatus.FAILED
        } else {
            job.status = IndexingJobStatus.RETRY_WAIT
            val multiplier = job.attemptCount.coerceAtLeast(1).toLong()
            job.nextRetryAt = failedAt.plus(baseDelay.multipliedBy(multiplier))
            job.completedAt = null
            IndexingJobStatus.RETRY_WAIT
        }
    }

    private companion object {
        const val MAX_ERROR_CODE_LENGTH = 100
        const val MAX_ERROR_MESSAGE_LENGTH = 1_000
    }
}
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.publication.service.IndexingFailureServiceTest"`
Expected: PASS

- [ ] **Step 3: `indexing_inline_retry_total{attempt}` + `indexing_job_duration_seconds{phase=total}` — `IndexingPipelineRunner`**

`IndexingPipelineRunnerTest.kt`의 `newRunner()`에 `meterRegistry` 파라미터를 추가하고(테스트 클래스 상단에 `private val meterRegistry = SimpleMeterRegistry()` 선언, import `io.micrometer.core.instrument.simple.SimpleMeterRegistry` 추가), 아래 테스트를 추가한다:

```kotlin
    @Test
    fun `재시도 가능한 실패마다 indexing_inline_retry_total이 attempt 태그로 증가한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        whenever(eventValidator.validate(any()))
            .thenThrow(RuntimeException("temporary"))

        val retryWaitJob: IndexingJobEntity = mock()
        whenever(retryWaitJob.attemptCount).thenReturn(1)
        whenever(retryWaitJob.nextRetryAt).thenReturn(LocalDateTime.now())
        whenever(indexingJobRepository.findById(any())).thenReturn(Optional.of(retryWaitJob))
        whenever(indexingJobRepository.start(any(), any(), any())).thenReturn(1, 0)
        whenever(indexingJobRepository.currentDbTimestamp()).thenReturn(LocalDateTime.now())

        runner.run(sampleEvent())

        assertThat(meterRegistry.get("indexing_inline_retry_total").tag("attempt", "1").counter().count())
            .isEqualTo(1.0)
    }

    @Test
    fun `성공하면 indexing_job_duration_seconds가 phase=total 태그로 기록된다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))
        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))
        val chunk = Chunk(chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2, pageFrom = null, pageTo = null, sectionPath = null, metadata = null)
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.FIXED_TOKEN))).thenReturn(listOf(chunk))

        runner.run(sampleEvent())

        assertThat(meterRegistry.get("indexing_job_duration_seconds").tag("phase", "total").timer().count())
            .isEqualTo(1L)
    }
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: FAIL — 생성자에 `meterRegistry` 파라미터가 없어 컴파일 실패.

`IndexingPipelineRunner.kt`에 `MeterRegistry` 주입 + 계측 두 곳을 추가한다. 생성자 마지막(`chunkingStrategy` 뒤)에:

```kotlin
    @Value("\${indexing.chunking.strategy:FIXED_TOKEN}")
    private val chunkingStrategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN,
    private val meterRegistry: MeterRegistry,
) {
```

`run()` 전체를 아래로 교체한다(전체 소요 시간 타이머 + 재시도 카운터 두 곳 추가):

```kotlin
    fun run(event: IndexingRequestedEvent) {
        val documentVersionId = event.documentVersionId
        if (documentVersionId == null) {
            log.error(
                "INDEXING_REQUESTED event {} (documentId={}) has no documentVersionId, dropping",
                event.eventId, event.documentId,
            )
            return
        }

        val jobId = acquireJobId(event, documentVersionId) ?: return
        val sample = Timer.start(meterRegistry)

        while (true) {
            val acquired = indexingJobRepository.start(jobId, workerId, maxAttempts)
            if (acquired != 1) {
                indexingJobRepository.failIfAttemptsExceeded(jobId, maxAttempts)
                log.info("job {} not acquired (already handled or attempts exceeded)", jobId)
                sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                return
            }

            try {
                val documentVersion = eventValidator.validate(event)
                val document =
                    documentRepository.findById(event.documentId).orElseThrow {
                        InvalidEventException("DOCUMENT_NOT_FOUND", "document ${event.documentId} does not exist")
                    }
                if (document.tenantId != event.tenantId) {
                    throw InvalidEventException(
                        "TENANT_MISMATCH",
                        "event tenantId=${event.tenantId} but document belongs to tenant ${document.tenantId}",
                    )
                }

                processAcquiredJob(jobId, event, documentVersion)
                sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                return
            } catch (e: Exception) {
                val failedAt = indexingJobRepository.currentDbTimestamp()
                val status =
                    indexingFailureService.recordFailure(
                        jobId = jobId,
                        errorCode = errorCodeOf(e),
                        errorMessage = e.message ?: "indexing failed",
                        permanent = !isRetryable(e),
                        maxAttempts = maxAttempts,
                        baseDelay = baseDelay,
                        failedAt = failedAt,
                    )
                if (status != IndexingJobStatus.RETRY_WAIT) {
                    log.warn("job {} resolved to {}", jobId, status, e)
                    sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                    return
                }
                val job = indexingJobRepository.findById(jobId).orElseThrow()
                // P1-2: 어느 시도에서 실패/성공하는지 — 예산 튜닝 근거(FAULT_TOLERANCE.md §3 P1-2).
                meterRegistry.counter("indexing_inline_retry_total", "attempt", job.attemptCount.toString()).increment()
                val nextRetryAt = job.nextRetryAt!!
                val retryClock = indexingJobRepository.currentDbTimestamp()
                val waitMillis = Duration.between(retryClock, nextRetryAt).toMillis().coerceAtLeast(0)
                log.warn("job {} in RETRY_WAIT, retrying in-process after {}ms", jobId, waitMillis, e)
                Thread.sleep(waitMillis)
            }
        }
    }
```

파일 상단 import에 `io.micrometer.core.instrument.MeterRegistry`, `io.micrometer.core.instrument.Timer`를 추가한다.

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: PASS (13개 전부)

- [ ] **Step 4: `db_health_gate_paused_total` — `DbHealthGate`**

`DbHealthGateTest.kt`에 `SimpleMeterRegistry` 주입을 추가하고(생성자 호출부에 `meterRegistry` 전달) 테스트를 추가한다:

```kotlin
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
```

```kotlin
    private val meterRegistry = SimpleMeterRegistry()
    private val gate = DbHealthGate(registry, jdbcTemplate, meterRegistry)
```

```kotlin
    @Test
    fun `pause와 resume 전이마다 db_health_gate_paused_total이 증가한다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java))
            .thenThrow(RuntimeException("down"))
        whenever(container.isRunning).thenReturn(true)

        gate.check()

        assertThat(meterRegistry.get("db_health_gate_paused_total").counter().count()).isEqualTo(1.0)
    }
```

(`import org.assertj.core.api.Assertions.assertThat` 추가)

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.DbHealthGateTest"`
Expected: FAIL — 생성자에 `meterRegistry`가 없어 컴파일 실패.

`DbHealthGate.kt`를 아래로 수정한다:

```kotlin
package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
    private val meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${indexing.db-health-gate.check-interval-ms:5000}")
    fun check() {
        val healthy = runCatching { jdbcTemplate.queryForObject("SELECT 1", Int::class.java) }.isSuccess
        val container = registry.getListenerContainer(LISTENER_ID) ?: return
        when {
            !healthy && container.isRunning && !container.isPauseRequested -> {
                container.pause()
                meterRegistry.counter("db_health_gate_paused_total").increment()
                log.warn("DB down — consumer paused")
            }
            healthy && container.isPauseRequested -> {
                container.resume()
                log.info("DB up — consumer resumed")
            }
        }
    }

    private companion object {
        const val LISTENER_ID = "indexing"
    }
}
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.DbHealthGateTest"`
Expected: PASS (5개 전부)

- [ ] **Step 5: `parse_thread_leaked` — `ParsingTimeoutGuard`**

`ParsingTimeoutGuardTest.kt`에 `SimpleMeterRegistry` 주입을 추가하고(모든 `ParsingTimeoutGuard(...)` 생성 호출에 `meterRegistry = SimpleMeterRegistry()` 추가) 테스트를 추가한다:

```kotlin
    @Test
    fun `타임아웃이 발생하면 parse_thread_leaked 게이지가 증가한다`() {
        val meterRegistry = SimpleMeterRegistry()
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(100), concurrency = 2, meterRegistry = meterRegistry)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<ParseTimeoutException> { guard.parse(parser, path, "text/plain") }

        assertThat(meterRegistry.get("parse_thread_leaked").gauge().value()).isEqualTo(1.0)
    }
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.parsing.ParsingTimeoutGuardTest"`
Expected: FAIL — 생성자에 `meterRegistry`가 없어 컴파일 실패.

`ParsingTimeoutGuard.kt`를 아래로 수정한다(누적 타임아웃 횟수를 게이지로 노출 — "취소했지만 실제로 스레드가 죽었는지"는 확인할 수 없으므로, 취소 이벤트 누적치를 근사치로 쓴다):

```kotlin
package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

@Component
class ParsingTimeoutGuard(
    @Value("\${indexing.limits.parse-timeout:PT60S}")
    private val parseTimeout: Duration,
    @Value("\${indexing.consumer.concurrency:5}")
    concurrency: Int,
    private val meterRegistry: MeterRegistry,
) {
    private val parseExecutor = Executors.newFixedThreadPool(concurrency)
    // P1-2: 타임아웃으로 취소된(=이론상 계속 살아있을 수 있는) 파싱 스레드 누적 횟수의 근사치.
    // future.cancel(true)가 실제로 스레드를 죽였는지는 확인할 방법이 없어(PDFBox 등 네이티브
    // 호출은 인터럽트에 응답하지 않을 수 있다), "취소를 시도한 횟수"를 대리 지표로 쓴다.
    private val leakedThreadCount = AtomicInteger(0)

    init {
        meterRegistry.gauge("parse_thread_leaked", leakedThreadCount)
    }

    fun parse(
        parser: DocumentParser,
        path: Path,
        mimeType: String,
    ): List<ParsedBlock> {
        val future =
            parseExecutor.submit(
                Callable {
                    try {
                        Files.newInputStream(path).use { parser.parse(it).toList() }
                    } catch (e: IOException) {
                        throw CorruptedFileException(mimeType, e)
                    }
                },
            )
        return try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            leakedThreadCount.incrementAndGet()
            throw ParseTimeoutException(mimeType, parseTimeout)
        } catch (e: ExecutionException) {
            throw (e.cause as? Exception) ?: e
        }
    }

    @PreDestroy
    fun shutdown() {
        parseExecutor.shutdown()
    }
}
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.parsing.ParsingTimeoutGuardTest"`
Expected: PASS (4개 전부)

- [ ] **Step 6: `kafka_rebalance_total` — `RebalanceMetricsListener`**

```kotlin
package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

// P1-2: 오르면 P0-3의 재시도 예산 계산이 실제로 틀렸다는 신호다(FAULT_TOLERANCE.md §3 P1-2
// 핵심 지표). Kafka listener container에 연결하는 배선은 Task 7 Step 7에서 한다 —
// 이 클래스 자체는 순수하게 카운터 증가 로직만 담당해 단위 테스트가 가능하다.
class RebalanceMetricsListener(
    private val meterRegistry: MeterRegistry,
) : ConsumerRebalanceListener {
    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        if (partitions.isNotEmpty()) {
            meterRegistry.counter("kafka_rebalance_total").increment()
        }
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) = Unit
}
```

- [ ] **Step 7: `RebalanceMetricsListener` 테스트**

```kotlin
package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RebalanceMetricsListenerTest {
    private val meterRegistry = SimpleMeterRegistry()
    private val listener = RebalanceMetricsListener(meterRegistry)

    @Test
    fun `파티션이 회수되면 kafka_rebalance_total이 증가한다`() {
        listener.onPartitionsRevoked(listOf(TopicPartition("indexing", 0)))

        assertThat(meterRegistry.get("kafka_rebalance_total").counter().count()).isEqualTo(1.0)
    }

    @Test
    fun `빈 파티션 목록으로 호출되면(리밸런스 없음) 증가하지 않는다`() {
        listener.onPartitionsRevoked(emptyList())

        assertThat(meterRegistry.find("kafka_rebalance_total").counters()).isEmpty()
    }

    @Test
    fun `파티션 할당 콜백은 카운터를 건드리지 않는다`() {
        listener.onPartitionsAssigned(listOf(TopicPartition("indexing", 0)))

        assertThat(meterRegistry.find("kafka_rebalance_total").counters()).isEmpty()
    }
}
```

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.RebalanceMetricsListenerTest"`
Expected: PASS (3개) — 새 클래스라 별도 RED 단계 없이 바로 구현+테스트를 함께 작성했다.

- [ ] **Step 8: 리스너 컨테이너에 `RebalanceMetricsListener` 연결**

`IndexingKafkaListener.kt`가 있는 `consumer` 패키지에 `Configuration` 클래스를 하나 추가한다(기존 `@KafkaListener` 애노테이션 자체는 건드리지 않고, 컨테이너 커스터마이저로 배선한다):

```kotlin
package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ContainerCustomizer
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class RebalanceMetricsConfig {
    @Bean
    fun rebalanceMetricsContainerCustomizer(
        meterRegistry: MeterRegistry,
    ): ContainerCustomizer<ConcurrentMessageListenerContainer<String, String>> =
        ContainerCustomizer { container ->
            container.containerProperties.consumerRebalanceListener = RebalanceMetricsListener(meterRegistry)
        }
}
```

이 배선 자체(Spring이 `ContainerCustomizer` 빈을 실제로 `@KafkaListener` 컨테이너에 적용하는지)는 실제 Kafka 브로커 없이는 검증할 수 없다 — `FAULT_TOLERANCE.md` §5.2/§5.3의 리밸런스 시나리오(카오스 테스트)로 최종 검증한다. 여기서는 컴파일만 확인한다.

Run: `./gradlew compileKotlin`
Expected: 성공

- [ ] **Step 9: 전체 테스트 통과 확인**

Run: `./gradlew test`
Expected: PASS (integration 태그 제외 전체)

- [ ] **Step 10: 커밋**

```bash
git add build.gradle.kts \
        src/main/kotlin/com/osscontest/worker/indexing/publication/service/IndexingFailureService.kt \
        src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt \
        src/main/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGate.kt \
        src/main/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuard.kt \
        src/main/kotlin/com/osscontest/worker/indexing/consumer/RebalanceMetricsListener.kt \
        src/main/kotlin/com/osscontest/worker/indexing/consumer/RebalanceMetricsConfig.kt \
        src/test/kotlin/com/osscontest/worker/indexing/publication/service/IndexingFailureServiceTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/consumer/DbHealthGateTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/parsing/ParsingTimeoutGuardTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/consumer/RebalanceMetricsListenerTest.kt
git commit -m "feat: Micrometer 메트릭 6종 계측 + Kafka consumer 메트릭 자동 노출 (P1-2)"
```

---

## Task 8: 진행률 `phase` 컬럼 (P1-1)

**전제(중요)**: `indexing_job.phase VARCHAR(30)` 컬럼이 실제 DB에 이미 추가돼 있어야 한다(스펙 §7.2 요청 — 이 repo는 `ddl-auto: none`이고 마이그레이션 파일이 없어 스키마를 직접 못 만든다). 컬럼이 없으면 이 Task의 통합 테스트(Step 5)가 실패한다. 단위 테스트(Step 1~4)는 mock만 쓰므로 컬럼 유무와 무관하게 통과한다.

**막는 장애**: 인라인 재시도가 리스너 안에서 도는 동안 외부에는 아무 진행 상황도 안 보인다 — `phase`가 없으면 사용자는 "멈췄다"고 오인한다.

**Files:**
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/publication/entity/IndexingJobEntity.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepository.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerIntegrationTest.kt`

- [ ] **Step 1: `IndexingJobEntity`에 `phase` 필드 추가**

```kotlin
    @Column(name = "trace_id")
    var traceId: String?,
    @Column(name = "phase")
    var phase: String? = null,
    @Column(name = "started_at")
    var startedAt: LocalDateTime?,
```

(기존 `IndexingJobEntity(...)` 생성 호출부는 named argument를 쓰고 있고 `phase`에 기본값이 있으므로 컴파일이 깨지지 않는다.)

- [ ] **Step 2: 실패하는 테스트 작성**

`IndexingPipelineRunnerTest.kt`의 `정상 흐름이면 다운로드부터 청킹까지 수행하고 IndexingProcessor를 호출한다` 테스트 끝에 검증을 추가한다:

```kotlin
        val order = inOrder(indexingJobRepository)
        order.verify(indexingJobRepository).updatePhase(5001L, "DOWNLOADING")
        order.verify(indexingJobRepository).updatePhase(5001L, "PARSING")
        order.verify(indexingJobRepository).updatePhase(5001L, "CHUNKING")
        order.verify(indexingJobRepository).updatePhase(5001L, "EMBEDDING")
```

(`import org.mockito.kotlin.inOrder` 추가)

- [ ] **Step 3: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: FAIL — `IndexingJobRepository.updatePhase()`가 없어 컴파일 실패.

- [ ] **Step 4: `updatePhase` 쿼리 + `IndexingPipelineRunner` 갱신 지점 추가**

`IndexingJobRepository.kt`에 기존 네이티브 쿼리들과 같은 스타일로 추가(파일 끝, `failActiveJobsForDocument` 다음):

```kotlin
    // P1-1: 진행률 노출. 완료 후가 아니라 각 단계 "진입 직전"에 호출해야 사용자가 가장 오래
    // 걸리는 구간(특히 EMBEDDING)에서도 멈춘 것처럼 보이지 않는다(스펙 §4 P1-1).
    @Modifying
    @Transactional
    @Query(
        value = "UPDATE indexing_job SET phase = :phase, updated_at = CURRENT_TIMESTAMP WHERE id = :jobId",
        nativeQuery = true,
    )
    fun updatePhase(
        @Param("jobId") jobId: Long,
        @Param("phase") phase: String,
    ): Int
```

`IndexingPipelineRunner.kt`의 `processAcquiredJob()`에 단계 진입마다 호출을 끼워 넣는다(전체를 아래로 교체):

```kotlin
    private fun processAcquiredJob(
        jobId: Long,
        event: IndexingRequestedEvent,
        documentVersion: DocumentVersionEntity,
    ) {
        if (documentVersion.fileSize > maxFileSizeBytes) {
            throw FileTooLargeException(documentVersion.fileSize, maxFileSizeBytes)
        }

        indexingJobRepository.updatePhase(jobId, "DOWNLOADING")
        val tempFile = downloadClient.download(documentVersion.sourceObjectKey)
        try {
            val actualHash = "sha256:" + sha256HexOf(tempFile)
            if (actualHash != documentVersion.contentHash) {
                throw ContentIntegrityException(documentVersion.contentHash, actualHash)
            }

            indexingJobRepository.updatePhase(jobId, "PARSING")
            val parser = parserRegistry.findParser(documentVersion.mimeType)
            val blocks = parsingTimeoutGuard.parse(parser, tempFile, documentVersion.mimeType)

            indexingJobRepository.updatePhase(jobId, "CHUNKING")
            val chunks = chunkingService.chunk(blocks, chunkingStrategy)
            chunkGuard.assertValid(chunks)

            val context =
                IndexingContext(
                    jobId = jobId,
                    documentId = event.documentId,
                    documentVersionId = documentVersion.id,
                    versionNo = documentVersion.versionNo,
                    extractedMetadata = null,
                )

            indexingJobRepository.updatePhase(jobId, "EMBEDDING")
            // PUBLISHING 전이는 IndexingProcessor.process() 내부(UPSERT/fencing 비교 직전)에서
            // 이뤄져야 한다 — 이 계획 밖(별도 저장소)에서 구현된다(스펙 §7.1 요청 사항).
            indexingProcessor.process(context, chunks)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }
```

인라인 재시도 진입 시 `phase`가 이전 시도 값을 이어받지 않고 `DOWNLOADING`부터 다시 시작해야 한다는 스펙 요구사항은, 위 코드가 `processAcquiredJob()`이 호출될 때마다(=재시도 루프를 돌 때마다) `updatePhase(jobId, "DOWNLOADING")`을 다시 호출하므로 별도 리셋 로직 없이 이미 충족된다.

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: PASS (13개 전부)

- [ ] **Step 6: 통합 테스트에 phase 검증 추가(선택 — 실 DB에 `phase` 컬럼이 있을 때만 실행됨)**

`IndexingPipelineRunnerIntegrationTest.kt`의 `재시도 가능한 오류는...` 테스트 마지막에 추가:

```kotlin
        assertThat(job.phase).isEqualTo("EMBEDDING")
```

- [ ] **Step 7: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/publication/entity/IndexingJobEntity.kt \
        src/main/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepository.kt \
        src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerIntegrationTest.kt
git commit -m "feat: 각 처리 단계 진입 직전 indexing_job.phase 갱신 (P1-1, DOWNLOADING~EMBEDDING)"
```

---

## Task 9: 재시도 시각을 DB 기준으로 통일 (P2-2, 범위 조정판)

**막는 장애**: `recordFailure()`가 애플리케이션 시각(`LocalDateTime.now()`)으로 `next_retry_at`을 계산하는데, `start()`의 크래시 재획득 비교(`next_retry_at <= CURRENT_TIMESTAMP`)는 DB 시각을 쓴다 — 두 서버의 NTP 오차만큼 재획득 타이밍이 어긋날 수 있다(B-Gap-5). 범위는 "P2-2 범위 조정" 절 참고.

**Files:**
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepository.kt`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt`

- [ ] **Step 1: 실패하는 테스트 작성**

`IndexingPipelineRunnerTest.kt`의 `IndexingProcessor가 예외를 던지면...` 테스트에 검증을 추가한다:

```kotlin
        verify(indexingJobRepository).currentDbTimestamp()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: FAIL — `IndexingJobRepository.currentDbTimestamp()`가 없어 컴파일 실패.

- [ ] **Step 3: `currentDbTimestamp()` 쿼리 추가 + `IndexingPipelineRunner`가 이를 사용하도록 변경**

`IndexingJobRepository.kt` 끝에 추가:

```kotlin
    // P2-2: recordFailure()가 next_retry_at을 계산할 때 애플리케이션 시각이 아니라 DB 시각을
    // 쓰도록 한다 — start()의 재획득 비교(next_retry_at <= CURRENT_TIMESTAMP)도 DB 시각이라
    // 두 시각의 출처를 맞춰야 NTP 오차로 인한 크래시 재획득 타이밍 어긋남이 없다(B-Gap-5).
    @Query(value = "SELECT CURRENT_TIMESTAMP", nativeQuery = true)
    fun currentDbTimestamp(): LocalDateTime
```

(파일 상단 import에 `java.time.LocalDateTime` 추가)

`IndexingPipelineRunner.kt`의 `run()`에서 `recordFailure(...)` 호출의 `failedAt = LocalDateTime.now()`를 바꾼다:

```kotlin
                val failedAt = indexingJobRepository.currentDbTimestamp()
                val status =
                    indexingFailureService.recordFailure(
                        jobId = jobId,
                        errorCode = errorCodeOf(e),
                        errorMessage = e.message ?: "indexing failed",
                        permanent = !isRetryable(e),
                        maxAttempts = maxAttempts,
                        baseDelay = baseDelay,
                        failedAt = failedAt,
                    )
```

`RETRY_WAIT`이면 `nextRetryAt`을 읽은 뒤 DB 현재 시각을 다시 조회해 남은 시간을 계산한다:

```kotlin
                val job = indexingJobRepository.findById(jobId).orElseThrow()
                val nextRetryAt = job.nextRetryAt!!
                val retryClock = indexingJobRepository.currentDbTimestamp()
                val waitMillis = Duration.between(retryClock, nextRetryAt).toMillis().coerceAtLeast(0)
```

이렇게 해야 앱 시계가 DB보다 빠른 경우에도 재시도 시각 전에 `start()`가 실행되어 Job이
`RETRY_WAIT`에 고립되는 경로가 생기지 않는다.

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunnerTest"`
Expected: PASS (13개 전부)

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/com/osscontest/worker/indexing/publication/repository/IndexingJobRepository.kt \
        src/main/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunner.kt \
        src/test/kotlin/com/osscontest/worker/indexing/pipeline/service/IndexingPipelineRunnerTest.kt
git commit -m "fix: 재시도 실패 시각을 앱 시계 대신 DB 시계 기준으로 기록 (P2-2, B-Gap-5)"
```

---

## Task 10: `trace_id`를 MDC로 전파해 모든 로그에 포함 (P2-5)

**전제**: `indexing_job.trace_id` DB 저장은 이미 구현돼 있다 — `IndexingJobRepository.insertIfAbsent()`가 `event.traceId`를 그대로 넣는다. 이 Task는 **로그에 노출**하는 부분만 추가한다.

**Files:**
- Create: `src/main/resources/logback-spring.xml`
- Modify: `src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListener.kt`
- Test: `src/test/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListenerTest.kt`

- [ ] **Step 1: 로그 패턴에 traceId 추가**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <include resource="org/springframework/boot/logging/logback/defaults.xml"/>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%X{traceId:-}] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

- [ ] **Step 2: 실패하는 테스트 작성**

`IndexingKafkaListenerTest.kt`의 `indexingRequestedJson()` 헬퍼에 `traceId` 파라미터를 추가한다:

```kotlin
    private fun indexingRequestedJson(
        documentId: Long,
        documentVersionId: Long = 1001,
        traceId: String? = null,
    ) = """
        {"eventId":"${java.util.UUID.randomUUID()}","eventType":"INDEXING_REQUESTED",
         "schemaVersion":1,"tenantId":7,"documentId":$documentId,"documentVersionId":$documentVersionId,
         "occurredAt":"2026-08-16T09:14:22Z","traceId":${traceId?.let { "\"$it\"" } ?: "null"}}
        """.trimIndent()
```

아래 테스트를 추가한다(import에 `org.slf4j.MDC` 추가):

```kotlin
    @Test
    fun `이벤트 처리 중에는 traceId가 MDC에 설정되고, 끝나면 제거된다`() {
        var capturedDuringCall: String? = null
        whenever(runner.run(any())).thenAnswer {
            capturedDuringCall = MDC.get("traceId")
            null
        }
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1, traceId = "abc-123")))

        listener.onMessage(records, ack)

        assertThat(capturedDuringCall).isEqualTo("abc-123")
        assertThat(MDC.get("traceId")).isNull()
    }
```

(import에 `org.assertj.core.api.Assertions.assertThat` 추가)

- [ ] **Step 3: 테스트 실패 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.IndexingKafkaListenerTest"`
Expected: FAIL — `capturedDuringCall`이 `null`(MDC를 아직 설정하지 않음).

- [ ] **Step 4: `processRecord()`에 MDC 설정 추가**

`IndexingKafkaListener.kt`의 `processRecord()`를 아래로 교체한다:

```kotlin
    private fun processRecord(record: ConsumerRecord<String, String>) {
        var event: IndexingRequestedEvent? = null
        try {
            event = deserialize(record.value())
            MDC.put("traceId", event.traceId ?: "-")
            when (event.eventType) {
                "INDEXING_REQUESTED" -> pipelineRunner.run(event)
                "DOCUMENT_DELETED" -> deletionHandler.handle(event)
                else ->
                    throw InvalidEventException(
                        "UNKNOWN_EVENT_TYPE",
                        "eventType=${event.eventType} is not recognized",
                    )
            }
        } catch (e: DataAccessException) {
            throw e
        } catch (e: DeserializationException) {
            log.error("event schema invalid: partition={} offset={}", record.partition(), record.offset(), e)
        } catch (e: InvalidEventException) {
            log.error(
                "event validation failed: code={} eventId={} documentId={} partition={} offset={}",
                e.code, event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        } catch (e: Exception) {
            log.error(
                "indexing failed: eventId={} documentId={} partition={} offset={}",
                event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        } finally {
            MDC.remove("traceId")
        }
    }
```

파일 상단 import에 `org.slf4j.MDC`를 추가한다.

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew test --tests "com.osscontest.worker.indexing.consumer.IndexingKafkaListenerTest"`
Expected: PASS (8개 전부)

- [ ] **Step 6: 전체 회귀 테스트**

Run: `./gradlew test`
Expected: PASS (integration 태그 제외 전체)

- [ ] **Step 7: 커밋**

```bash
git add src/main/resources/logback-spring.xml \
        src/main/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListener.kt \
        src/test/kotlin/com/osscontest/worker/indexing/consumer/IndexingKafkaListenerTest.kt
git commit -m "feat: traceId를 MDC로 전파해 모든 로그 라인에 포함 (P2-5)"
```

---

## 이 계획 완료 후 남는 것

- **인프라 요청**(코드 아님, 스펙 §7.2/§7.4): `indexing_job.phase` 컬럼 실제 DB 추가(Task 8 전제), `max.poll.interval.ms` 배포 환경 반영(Task 6은 `application.yml` literal 값만 바꾼다 — 배포 파이프라인이 이 파일을 그대로 쓰는지 확인 필요).
- **다른 담당자 요청**(스펙 §7.1): `IndexingProcessor`(태성) 쪽에서 `PUBLISHING` phase 갱신, 임베딩 예외를 `isRetryable()` 화이트리스트에 등록 가능한 형태로 던지기, 임베딩 타임아웃 30초.
- **의도적으로 제외**: P1-3, P2-4(스펙에서 이미 제외 확정), P2-1·P2-3(인프라/실측 선행 필요 — 이 계획 범위 밖).
- **§8 카오스 테스트**(FAULT_TOLERANCE.md §5.8, 스펙 §8.2): 이 계획의 단위/통합 테스트로는 검증 안 되는 최종 성적표 — Task 1~10이 다 머지된 뒤 별도로 실행한다.

---

**Plan complete and saved to `docs/superpowers/plans/2026-08-19-track-b-fault-tolerance.md`.** Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
