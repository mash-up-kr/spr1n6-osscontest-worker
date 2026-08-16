package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID

// recordFailure()는 @Transactional(REQUIRES_NEW)다 — 별도 커넥션/트랜잭션으로 즉시 커밋되어야
// 바깥 트랜잭션이 롤백돼도 실패 기록이 남는다. @DataJpaTest의 기본 동작(테스트 메서드당
// 트랜잭션으로 감싸고 종료 시 롤백)을 그대로 두면, REQUIRES_NEW 트랜잭션이 별도 커넥션을 쓰므로
// 바깥 트랜잭션에서 커밋되지 않은 시딩/저장 데이터를 보지 못한다(READ COMMITTED 격리 하에서
// 보이지 않음). 그래서 클래스 레벨에서 @Transactional(propagation = NOT_SUPPORTED)로
// 테스트 트랜잭션 래핑 자체를 비활성화하고, 각 문장이 즉시 커밋되게 한 뒤 @AfterEach에서
// 직접 정리한다.
@Tag("integration")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("local")
@Import(IndexingFailureService::class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class IndexingFailureServiceIntegrationTest(
    private val indexingFailureService: IndexingFailureService,
    private val indexingJobRepository: IndexingJobRepository,
    private val jdbcTemplate: JdbcTemplate,
) {
    @BeforeEach
    fun seedDocumentVersionFixture() {
        jdbcTemplate.update(
            "INSERT INTO tenant (id, name, created_at) VALUES (1, 'test-tenant', CURRENT_TIMESTAMP)",
        )
        jdbcTemplate.update(
            "INSERT INTO document (id, tenant_id, owner_principal_id, title) VALUES (1, 1, 'owner', 'test title')",
        )
        jdbcTemplate.update(
            """
            INSERT INTO document_version
                (id, document_id, version_no, source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
            VALUES
                (1, 1, 1, 'test-object-key', ?, 'application/pdf', 100, 'test-content-hash', 'owner')
            """,
            "test.pdf".toByteArray(),
        )
    }

    @AfterEach
    fun cleanUp() {
        // document_version에 INSERT 트리거(trg_document_version_outbox)가 걸려 있어
        // outbox_event에도 행이 생긴다 — document_version을 지우려면 먼저 지워야 한다.
        jdbcTemplate.update("DELETE FROM indexing_job")
        jdbcTemplate.update("DELETE FROM outbox_event")
        jdbcTemplate.update("DELETE FROM document_version")
        jdbcTemplate.update("DELETE FROM document")
        jdbcTemplate.update("DELETE FROM tenant")
    }

    private fun seedJob(
        status: IndexingJobStatus,
        attemptCount: Int,
    ): Long {
        val job =
            indexingJobRepository.save(
                IndexingJobEntity(
                    sourceEventId = UUID.randomUUID(),
                    documentId = 1L,
                    documentVersionId = 1L,
                    status = status,
                    attemptCount = attemptCount,
                    nextRetryAt = null,
                    workerId = "worker-1",
                    lastErrorCode = null,
                    lastErrorMessage = null,
                    traceId = null,
                    startedAt = LocalDateTime.now(),
                    completedAt = if (status == IndexingJobStatus.COMPLETED) LocalDateTime.now() else null,
                    updatedAt = LocalDateTime.now(),
                ),
            )
        return job.id!!
    }

    @Test
    fun `recordFailure는 상한 미만이면 RETRY_WAIT로 전이시킨다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 1)
        val failedAt = LocalDateTime.now()

        val result =
            indexingFailureService.recordFailure(
                jobId = jobId,
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
                failedAt = failedAt,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        // §3.8 선형 백오프: attempt_count=1 → failedAt + base_delay * 1
        assertThat(reloaded.nextRetryAt).isEqualTo(failedAt.plus(BASE_DELAY))
        assertThat(reloaded.lastErrorCode).isEqualTo("SOME_ERROR")
    }

    // §3.8: next_retry_at = now() + base_delay * attempt_count — 상수 지연이 아니라 선형이어야 한다.
    @Test
    fun `recordFailure는 next_retry_at을 base_delay와 attempt_count의 곱으로 계산한다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 3)
        val failedAt = LocalDateTime.now()

        val result =
            indexingFailureService.recordFailure(
                jobId = jobId,
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
                failedAt = failedAt,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.nextRetryAt).isEqualTo(failedAt.plus(BASE_DELAY.multipliedBy(3)))
        // 상수 지연(= failedAt + base_delay 한 번)이 아님을 명시적으로 못 박는다.
        assertThat(reloaded.nextRetryAt).isNotEqualTo(failedAt.plus(BASE_DELAY))
    }

    @Test
    fun `recordFailure는 상한 도달이면 FAILED로 전이시킨다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 5)
        val failedAt = LocalDateTime.now()

        val result =
            indexingFailureService.recordFailure(
                jobId = jobId,
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
                failedAt = failedAt,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.FAILED)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(reloaded.nextRetryAt).isNull()
        assertThat(reloaded.completedAt).isNotNull
    }

    @Test
    fun `recordFailure는 PROCESSING이 아니면 조용히 무시한다`() {
        val jobId = seedJob(status = IndexingJobStatus.COMPLETED, attemptCount = 1)
        val failedAt = LocalDateTime.now()

        val result =
            indexingFailureService.recordFailure(
                jobId = jobId,
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
                failedAt = failedAt,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.COMPLETED)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(reloaded.lastErrorCode).isNull()
    }

    private companion object {
        val BASE_DELAY: Duration = Duration.ofSeconds(30)
    }
}
