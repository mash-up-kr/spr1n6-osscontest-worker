package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.support.claimFixture
import com.osscontest.worker.indexing.queue.JobQueueStore
import com.osscontest.worker.indexing.queue.JobType
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.whenever
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.jdbc.Sql
import java.nio.file.Files
import java.util.UUID

// 매 시도를 새 claim으로 실행하여 영속 재시도와 시도 상한을 검증한다.
@Tag("integration")
@SpringBootTest(
    properties = [
        "indexing.queue.enabled=false",
        "indexing.database.check-interval=PT0.01S",
        "indexing.retry.base-delay=PT0.01S",
    ],
)
@ActiveProfiles("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexingPipelineRunnerIntegrationTest(
    private val pipelineRunner: IndexingPipelineRunner,
    private val indexingJobRepository: IndexingJobRepository,
    private val fakeIndexingProcessor: FakeIndexingProcessor,
    private val jdbcTemplate: JdbcTemplate,
    private val queue: JobQueueStore,
) {
    // 실제 임베딩 공급자 대신 실패 횟수와 Job 완료를 제어할 수 있는 테스트 대역을 사용한다.
    @TestConfiguration
    class FakeIndexingProcessorConfig {
        @Bean
        @Primary
        fun indexingProcessor(indexingJobRepository: IndexingJobRepository): IndexingProcessor =
            FakeIndexingProcessor(indexingJobRepository)
    }

    // 원문 다운로드는 실제 S3/MinIO에 의존하지 않도록 mock으로 대체한다.
    @MockitoBean
    private lateinit var downloadClient: DocumentDownloadClient

    @MockitoBean private lateinit var breaker: com.osscontest.worker.indexing.queue.CircuitBreaker

    @BeforeAll
    fun verifyDedicatedDatabase() {
        assertDedicatedIntegrationDatabase(jdbcTemplate)
    }

    @BeforeEach
    fun resetProcessor() {
        whenever(breaker.admission()).thenReturn(com.osscontest.worker.indexing.queue.ClaimAdmission.NORMAL)
        com.osscontest.worker.support.allowCircuitCalls(breaker)
        fakeIndexingProcessor.reset()
    }

    @AfterEach
    fun cleanUp() {
        jdbcTemplate.update("DELETE FROM indexing_job")
        jdbcTemplate.update("DELETE FROM document_chunk")
        jdbcTemplate.update("DELETE FROM outbox_event")
        jdbcTemplate.update("DELETE FROM document_version")
        jdbcTemplate.update("DELETE FROM document")
        jdbcTemplate.update("DELETE FROM tenant")
    }

    // 아래 @Sql 시드들이 쓰는 900001~900003는 이 테스트 스위트가 예약해 둔 테스트 전용 ID
    // 범위다 — 로컬/CI에서 언제든 지우고 다시 만들어도 되는, 수동 개발/데모 데이터와 공유하지
    // 않는 일회성 Postgres 인스턴스라는 전제를 깔고 있다.
    @Test
    @Sql(
        statements = [
            "INSERT INTO tenant (id, name) VALUES (900001, 'integration-test-tenant')",
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_upload_version_no) " +
                "VALUES (900001, 900001, 'test-user', 'integration test doc', 1)",
            "INSERT INTO document_version " +
                "(id, document_id, version_no, source_object_key, original_filename, mime_type, " +
                " file_size, content_hash, embedding_version_no, created_by_principal_id) " +
                "VALUES (900001, 900001, 1, 'docs/900001/v1.txt', decode('746573742e747874', 'hex'), " +
                " 'text/plain', 11, " +
                " 'sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9', 1, 'test-user')",
        ],
    )
    fun `재시도 가능한 오류는 새 claim에서 성공하면 COMPLETED로 끝난다`() {
        whenever(downloadClient.download("docs/900001/v1.txt")).thenAnswer {
            Files.createTempFile("integration-test-", ".tmp")
                .also { Files.write(it, "hello world".toByteArray()) }
        }
        val sourceEventId = UUID.randomUUID()
        queue.enqueue(sourceEventId, 900001L, 900001L, JobType.INDEX, null)

        // 처음 두 번은 실패(재시도 가능한 RuntimeException), 세 번째는 성공하도록 설정.
        fakeIndexingProcessor.failuresBeforeSuccess = 2

        executeAttempts(sourceEventId, 3, IndexingJobStatus.COMPLETED)

        val job = indexingJobRepository.findBySourceEventId(sourceEventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(job.attemptCount).isEqualTo(3)
        assertThat(job.phase).isEqualTo("EMBEDDING")
    }

    @Test
    @Sql(
        statements = [
            "INSERT INTO tenant (id, name) VALUES (900002, 'integration-test-tenant-2')",
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_upload_version_no) " +
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
        whenever(downloadClient.download("docs/900002/v1.txt")).thenAnswer {
            Files.createTempFile("integration-test-", ".tmp")
                .also { Files.write(it, "hello world".toByteArray()) }
        }
        val sourceEventId = UUID.randomUUID()
        queue.enqueue(sourceEventId, 900002L, 900002L, JobType.INDEX, null)

        fakeIndexingProcessor.failuresBeforeSuccess = Int.MAX_VALUE // 계속 실패

        executeAttempts(sourceEventId, 5, IndexingJobStatus.FAILED)

        val job = indexingJobRepository.findBySourceEventId(sourceEventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(job.attemptCount).isEqualTo(5) // indexing.retry.max-attempts 기본값
    }

    @Test
    @Sql(
        statements = [
            "INSERT INTO tenant (id, name) VALUES (900003, 'integration-test-tenant-3')",
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_upload_version_no) " +
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
        whenever(downloadClient.download("docs/900003/v1.txt")).thenAnswer {
            Files.createTempFile("integration-test-", ".tmp")
                .also { Files.write(it, "different content".toByteArray()) }
        }
        val sourceEventId = UUID.randomUUID()
        queue.enqueue(sourceEventId, 900003L, 900003L, JobType.INDEX, null)

        pipelineRunner.run(checkNotNull(queue.claimFixture(5) {}))

        val job = indexingJobRepository.findBySourceEventId(sourceEventId)
        assertThat(job).isNotNull
        assertThat(job!!.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(job.attemptCount).isEqualTo(1) // 재시도 없이 1회 시도만에 종결
        assertThat(job.lastErrorCode).isEqualTo("HASH_MISMATCH")
    }

    private fun executeAttempts(event: UUID, count: Int, terminal: IndexingJobStatus) {
        var original: Map<String, Any?>? = null
        var previousEpoch = 0L
        repeat(count) { index ->
            val claim = checkNotNull(queue.claim(5, "runner-test-${index + 1}"))
            val claimed = jdbcTemplate.queryForMap("SELECT * FROM indexing_job WHERE id=?", claim.token.jobId)
            if (original == null) original = claimed
            for (column in listOf("id", "source_event_id", "enqueue_seq")) {
                assertThat(claimed[column]).isEqualTo(original[column])
            }
            assertThat(claim.sourceEventId).isEqualTo(event)
            assertThat(claim.attemptCount).isEqualTo(index + 1)
            assertThat(claim.token.epoch).isEqualTo(previousEpoch + 1)
            pipelineRunner.run(claim)
            val saved = indexingJobRepository.findById(claim.token.jobId).orElseThrow()
            assertThat(saved.executionEpoch).isEqualTo(claim.token.epoch)
            assertThat(saved.attemptCount).isEqualTo(claim.attemptCount)
            assertThat(saved.workerId).isNull()
            assertThat(saved.leaseUntil).isNull()
            if (index < count - 1) {
                assertThat(saved.status).isEqualTo(IndexingJobStatus.RETRY_WAIT)
                assertThat(saved.outcome).isEqualTo("RETRY_SCHEDULED")
                // 대상 여부를 확인하기 전에 미래 재시도 시각을 고정하고, 그다음 시각을 지나게 만든다.
                jdbcTemplate.update("UPDATE indexing_job SET next_retry_at=clock_timestamp()+INTERVAL '1 minute' WHERE id=?", saved.id)
                assertThat(queue.claimFixture(5) {}).isNull()
                jdbcTemplate.update("UPDATE indexing_job SET next_retry_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", saved.id)
            } else {
                assertThat(saved.status).isEqualTo(terminal)
                assertThat(saved.outcome).isEqualTo(if (terminal == IndexingJobStatus.COMPLETED) "INDEXED" else "FAILED_PROCESSING")
                assertThat(queue.claimFixture(5) {}).isNull()
            }
            previousEpoch = claim.token.epoch
        }
        assertThat(fakeIndexingProcessor.calls).hasSize(count)
    }
}
