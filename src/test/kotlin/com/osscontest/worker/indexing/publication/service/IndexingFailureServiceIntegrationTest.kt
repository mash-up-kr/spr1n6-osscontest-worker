package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
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
import java.time.Instant
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.support.QueueIntegrationConfiguration
import java.util.UUID

@Tag("integration")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("integration")
@Import(IndexingFailureService::class, QueueIntegrationConfiguration::class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class IndexingFailureServiceIntegrationTest(
    private val indexingFailureService: IndexingFailureService,
    private val indexingJobRepository: IndexingJobRepository,
    private val jdbcTemplate: JdbcTemplate,
) {
    @BeforeEach
    fun seedDocumentVersionFixture() {
        assertDedicatedIntegrationDatabase(jdbcTemplate)
        jdbcTemplate.update(
            "INSERT INTO tenant (id, name, created_at) VALUES (1, 'test-tenant', CURRENT_TIMESTAMP)",
        )
        jdbcTemplate.update(
            "INSERT INTO document (id, tenant_id, owner_principal_id, title) VALUES (1, 1, 'owner', 'test title')",
        )
        jdbcTemplate.update(
            """
            INSERT INTO document_version
                (id, document_id, version_no, embedding_version_no, source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
            VALUES
                (1, 1, 1, 1, 'test-object-key', ?, 'application/pdf', 100, 'test-content-hash', 'owner')
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
                    startedAt = Instant.now(),
                    completedAt = if (status == IndexingJobStatus.COMPLETED) Instant.now() else null,
                    updatedAt = Instant.now(),
                    executionEpoch = 1, leaseUntil = if (status == IndexingJobStatus.PROCESSING) indexingJobRepository.currentDbTimestamp().plusSeconds(60) else null,
                ),
            )
        return job.id!!
    }

    @Test
    fun `recordFailure는 permanent=false에 상한 미만이면 RETRY_WAIT로 전이시킨다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 1)
        val failedAt = indexingJobRepository.currentDbTimestamp()

        val result =
            indexingFailureService.recordFailure(
                token = ExecutionToken(jobId, "worker-1", 1),
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                permanent = false,
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        // 선형 backoff에 ±20% 지터를 적용한다.
        assertThat(reloaded.nextRetryAt).isBetween(failedAt.plusMillis(24_000), indexingJobRepository.currentDbTimestamp().plusMillis(36_000))
        assertThat(reloaded.lastErrorCode).isEqualTo("SOME_ERROR")
        assertThat(reloaded.workerId).isNull()
        assertThat(reloaded.leaseUntil).isNull()
        assertThat(reloaded.executionEpoch).isEqualTo(1)
        assertThat(reloaded.attemptCount).isEqualTo(1)
    }

    // 지터가 있어도 회차 증가에 비례하는 지연 범위여야 한다.
    @Test
    fun `recordFailure는 next_retry_at을 base_delay와 attempt_count의 곱으로 계산한다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 3)
        val failedAt = indexingJobRepository.currentDbTimestamp()

        val result =
            indexingFailureService.recordFailure(
                token = ExecutionToken(jobId, "worker-1", 1),
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                permanent = false,
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.nextRetryAt).isBetween(failedAt.plusMillis(72_000), indexingJobRepository.currentDbTimestamp().plusMillis(108_000))
        assertThat(reloaded.nextRetryAt).isNotEqualTo(failedAt.plus(BASE_DELAY))
    }

    @Test
    fun `recordFailure는 permanent=false여도 상한 도달이면 FAILED로 전이시킨다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 5)
        val failedAt = indexingJobRepository.currentDbTimestamp()

        val result =
            indexingFailureService.recordFailure(
                token = ExecutionToken(jobId, "worker-1", 1),
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                permanent = false,
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.FAILED)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(reloaded.nextRetryAt).isNull()
        assertThat(reloaded.completedAt).isNotNull
    }

    @Test
    fun `recordFailure는 permanent=true면 attempt_count가 상한 미만이어도 즉시 FAILED로 전이시킨다`() {
        val jobId = seedJob(status = IndexingJobStatus.PROCESSING, attemptCount = 1)
        val failedAt = indexingJobRepository.currentDbTimestamp()

        val result =
            indexingFailureService.recordFailure(
                token = ExecutionToken(jobId, "worker-1", 1),
                errorCode = "TENANT_MISMATCH",
                errorMessage = "some message",
                permanent = true,
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
            )

        assertThat(result).isEqualTo(IndexingJobStatus.FAILED)
        val reloaded = indexingJobRepository.findById(jobId).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(reloaded.attemptCount).isEqualTo(1) // 재시도 없이 1회 만에 종결됐다는 근거
        assertThat(reloaded.nextRetryAt).isNull()
        assertThat(reloaded.lastErrorCode).isEqualTo("TENANT_MISMATCH")
    }

    @Test
    fun `recordFailure는 PROCESSING이 아니면 조용히 무시한다`() {
        val jobId = seedJob(status = IndexingJobStatus.COMPLETED, attemptCount = 1)
        val failedAt = indexingJobRepository.currentDbTimestamp()

        val result =
            indexingFailureService.recordFailure(
                token = ExecutionToken(jobId, "worker-1", 1),
                errorCode = "SOME_ERROR",
                errorMessage = "some message",
                permanent = false,
                maxAttempts = 5,
                baseDelay = BASE_DELAY,
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
