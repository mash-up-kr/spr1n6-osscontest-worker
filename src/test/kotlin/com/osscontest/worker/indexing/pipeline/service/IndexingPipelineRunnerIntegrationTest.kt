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

// 이 테스트는 pipelineRunner.run()을 직접 호출해 단일 실행 경로(인프로세스 재시도 포함)만
// 검증하려는 의도다.
@Tag("integration")
@SpringBootTest(
    properties = [
        "spring.kafka.listener.auto-startup=false",
        "indexing.retry.base-delay=PT0.01S",
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
        fun indexingProcessor(indexingJobRepository: IndexingJobRepository): IndexingProcessor =
            FakeIndexingProcessor(indexingJobRepository)
    }

    // 원문 다운로드는 실제 S3/MinIO에 의존하지 않도록 mock으로 대체한다.
    @MockitoBean
    private lateinit var downloadClient: DocumentDownloadClient

    // 아래 @Sql 시드들이 쓰는 900001~900003는 이 테스트 스위트가 예약해 둔 테스트 전용 ID
    // 범위다 — 로컬/CI에서 언제든 지우고 다시 만들어도 되는, 수동 개발/데모 데이터와 공유하지
    // 않는 일회성 Postgres 인스턴스라는 전제를 깔고 있다.
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
    fun `재시도 가능한 오류는 인프로세스로 재시도하다가 성공하면 COMPLETED로 끝난다`() {
        whenever(downloadClient.download("docs/900001/v1.txt")).thenReturn("hello world".toByteArray())
        val event = sampleEvent(documentId = 900001L, documentVersionId = 900001L)

        // 처음 두 번은 실패(재시도 가능한 RuntimeException), 세 번째는 성공하도록 설정.
        fakeIndexingProcessor.failuresBeforeSuccess = 2

        pipelineRunner.run(event)

        val job = indexingJobRepository.findBySourceEventId(event.eventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(job.attemptCount).isEqualTo(3)
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
        ],
    )
    fun `재시도 가능한 오류가 상한만큼 계속되면 FAILED로 종결한다`() {
        whenever(downloadClient.download("docs/900002/v1.txt")).thenReturn("hello world".toByteArray())
        val event = sampleEvent(documentId = 900002L, documentVersionId = 900002L)

        fakeIndexingProcessor.failuresBeforeSuccess = Int.MAX_VALUE // 계속 실패

        pipelineRunner.run(event)

        val job = indexingJobRepository.findBySourceEventId(event.eventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(job.attemptCount).isEqualTo(5) // indexing.retry.max-attempts 기본값
    }

    @Test
    @Sql(
        statements = [
            "INSERT INTO tenant (id, name) VALUES (900003, 'integration-test-tenant-3')",
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_version_no) " +
                "VALUES (900003, 900003, 'test-user', 'integration test doc 3', 1)",
            "INSERT INTO document_version " +
                "(id, document_id, version_no, source_object_key, original_filename, mime_type, " +
                " file_size, content_hash, embedding_version_no, created_by_principal_id) " +
                "VALUES (900003, 900003, 1, 'docs/900003/v1.txt', decode('746573742e747874', 'hex'), " +
                " 'text/plain', 11, " +
                " 'sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9', 1, 'test-user')",
        ],
    )
    fun `영구 실패(콘텐츠 무결성 오류)는 재시도 없이 즉시 FAILED로 종결한다`() {
        // content_hash가 실제 다운로드 바이트와 다르게 만들어 ContentIntegrityException을 유발한다 —
        // 같은 입력이면 항상 같은 결과이므로 재시도할 필요가 없는 영구 실패 케이스다.
        whenever(downloadClient.download("docs/900003/v1.txt")).thenReturn("different content".toByteArray())
        val event = sampleEvent(documentId = 900003L, documentVersionId = 900003L)

        pipelineRunner.run(event)

        val job = indexingJobRepository.findBySourceEventId(event.eventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(job.attemptCount).isEqualTo(1) // 재시도 없이 1회 시도만에 종결
        assertThat(job.lastErrorCode).isEqualTo("HASH_MISMATCH")
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
