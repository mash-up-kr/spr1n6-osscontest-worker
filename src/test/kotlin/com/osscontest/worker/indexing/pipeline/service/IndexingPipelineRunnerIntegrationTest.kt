package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.kotlin.whenever
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.jdbc.Sql
import java.time.Instant
import java.util.UUID

// 이 테스트는 pipelineRunner.run()을 직접 호출해 단일 실행 경로만 검증하려는 의도다.
// 이 브랜치의 @SpringBootTest 컨텍스트에는 IndexingRetryScheduler(@Scheduled)와
// IndexingKafkaListener(@KafkaListener)가 둘 다 실제 빈으로 살아 있다 — 둘 다 같은
// source_event_id로 pipelineRunner.run()을 다시 호출할 수 있는 별도 진입점이라, 테스트가
// 시딩한 행을 백그라운드에서 동시에 건드리면(중복 처리) 검증이 흔들릴 수 있다.
// 예전에는 스케줄러의 기본 initialDelay(운영 기본값)에 기대어 이 경합을 "10초 안에는 안
// 일어나겠지"로 회피했는데, 이는 타이밍/환경에 의존하는 완화책일 뿐 근본 해결이 아니다.
// 대신 여기서는 두 진입점을 결정적으로 완전히 차단한다: 스케줄러는 initial-delay-ms/
// poll-interval-ms를 사실상 무한대로 올려 테스트 실행 시간 안에는 절대 안 도는 것을 보장하고,
// Kafka 리스너 컨테이너는 spring.kafka.listener.auto-startup=false로 아예 시작되지 않게 한다
// (이 리스너는 @KafkaListener(topics=["indexing"], id="indexing")에 별도 containerFactory를
// 지정하지 않아 Boot가 자동구성한 기본 ConcurrentKafkaListenerContainerFactory를 쓰므로,
// KafkaProperties.listener.autoStartup이 그대로 적용된다).
@Tag("integration")
@SpringBootTest(
    properties = [
        "indexing.retry.initial-delay-ms=999999999",
        "indexing.retry.poll-interval-ms=999999999",
        "spring.kafka.listener.auto-startup=false",
    ],
)
@ActiveProfiles("local")
class IndexingPipelineRunnerIntegrationTest(
    private val pipelineRunner: IndexingPipelineRunner,
    private val indexingJobRepository: IndexingJobRepository,
    private val fakeIndexingProcessor: FakeIndexingProcessor,
) {
    // Spring Boot가 @SpringBootTest 클래스 안의 중첩 @TestConfiguration을 자동으로 인식해서
    // 메인 컨텍스트에 얹어준다 — IndexingProcessor의 실제 구현이 없는(Task 2) 이 브랜치에서
    // 유일하게 이 fake만 빈으로 등록된다.
    @TestConfiguration
    class FakeIndexingProcessorConfig {
        @Bean
        fun indexingProcessor(): IndexingProcessor = FakeIndexingProcessor()
    }

    // 원문 다운로드는 실제 S3/MinIO에 의존하지 않도록 mock으로 대체한다.
    @MockitoBean
    private lateinit var downloadClient: DocumentDownloadClient

    // 아래 @Sql 시드들이 쓰는 900001/900002는 이 테스트 스위트가 예약해 둔 테스트 전용 ID
    // 범위다 — 로컬/CI에서 언제든 지우고 다시 만들어도 되는, 수동 개발/데모 데이터와 공유하지
    // 않는 일회성 Postgres 인스턴스라는 전제를 깔고 있다. 공유 DB에서 이 테스트를 돌리면
    // 같은 ID를 쓰는 다른 데이터와 충돌할 수 있다.
    @Test
    @Sql(
        statements = [
            "INSERT INTO tenant (id, name) VALUES (900001, 'integration-test-tenant')",
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_version_no) " +
                "VALUES (900001, 900001, 'test-user', 'integration test doc', 1)",
            "INSERT INTO document_version " +
                "(id, document_id, version_no, source_object_key, original_filename, mime_type, " +
                " file_size, content_hash, embedding_version_no, created_by_principal_id) " +
                "VALUES (900001, 900001, 1, 'docs/900001/v1.txt', decode('746573742e747874', 'hex'), " +
                " 'text/plain', 11, " +
                " 'sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9', 1, 'test-user')",
        ],
    )
    fun `실패 후 attempt_count가 상한 미만이면 RETRY_WAIT으로 전이한다`() {
        whenever(downloadClient.download("docs/900001/v1.txt")).thenReturn("hello world".toByteArray())
        val event = sampleEvent(documentId = 900001L, documentVersionId = 900001L)

        fakeIndexingProcessor.throwOnNextCall = RuntimeException("simulated processing failure")
        pipelineRunner.run(event)

        val job = indexingJobRepository.findBySourceEventId(event.eventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        assertThat(job.attemptCount).isEqualTo(1)
    }

    @Test
    @Sql(
        statements = [
            "INSERT INTO tenant (id, name) VALUES (900002, 'integration-test-tenant-2')",
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_version_no) " +
                "VALUES (900002, 900002, 'test-user', 'integration test doc 2', 1)",
            "INSERT INTO document_version " +
                "(id, document_id, version_no, source_object_key, original_filename, mime_type, " +
                " file_size, content_hash, embedding_version_no, created_by_principal_id) " +
                "VALUES (900002, 900002, 1, 'docs/900002/v1.txt', decode('746573742e747874', 'hex'), " +
                " 'text/plain', 11, " +
                " 'sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9', 1, 'test-user')",
            // 상한(기본 5) 직전 상태를 직접 시딩한다 — attempt_count=4, next_retry_at은 이미 지남.
            "INSERT INTO indexing_job " +
                "(source_event_id, document_id, document_version_id, status, attempt_count, next_retry_at, updated_at) " +
                "VALUES ('22222222-2222-2222-2222-222222222222', 900002, 900002, 'RETRY_WAIT', 4, " +
                " CURRENT_TIMESTAMP - INTERVAL '1 minute', CURRENT_TIMESTAMP)",
        ],
    )
    fun `attempt_count가 상한에 도달하면 FAILED로 종결한다`() {
        whenever(downloadClient.download("docs/900002/v1.txt")).thenReturn("hello world".toByteArray())
        val event =
            sampleEvent(
                documentId = 900002L,
                documentVersionId = 900002L,
                eventId = UUID.fromString("22222222-2222-2222-2222-222222222222"),
            )

        fakeIndexingProcessor.throwOnNextCall = RuntimeException("simulated processing failure")
        pipelineRunner.run(event)

        val job = indexingJobRepository.findBySourceEventId(event.eventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(job.attemptCount).isEqualTo(5)
    }

    private fun sampleEvent(
        documentId: Long,
        documentVersionId: Long,
        eventId: UUID = UUID.randomUUID(),
    ) = IndexingRequestedEvent(
        eventId = eventId,
        eventType = "INDEXING_REQUESTED",
        eventSchemaVersion = 1,
        tenantId = documentId,
        documentId = documentId,
        documentVersionId = documentVersionId,
        occurredAt = Instant.now(),
        traceId = null,
    )
}
