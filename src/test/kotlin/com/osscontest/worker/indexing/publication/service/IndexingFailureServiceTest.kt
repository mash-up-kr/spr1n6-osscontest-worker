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
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.JobQueueStore
import com.osscontest.worker.support.testToken
import com.osscontest.worker.support.testNow
import java.time.Instant
import java.util.UUID

class IndexingFailureServiceTest {
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val store: JobQueueStore = mock<JobQueueStore>().apply { whenever(dbNow()).thenReturn(testNow) }
    private val service = IndexingFailureService(indexingJobRepository, meterRegistry, JobOwnershipGuard(indexingJobRepository, store))

    private fun processingJob(attemptCount: Int = 5) =
        IndexingJobEntity(
            id = 1L, sourceEventId = UUID.randomUUID(), documentId = 1L, documentVersionId = 1L,
            status = IndexingJobStatus.PROCESSING, attemptCount = attemptCount, nextRetryAt = null, workerId = testToken.workerId,
            lastErrorCode = null, lastErrorMessage = null, traceId = null, startedAt = null,
            completedAt = null, updatedAt = testNow,
            executionEpoch = testToken.epoch, leaseExpiresAt = testNow.plusSeconds(60),
        )

    @Test
    fun `영구 실패로 FAILED 종결되면 indexing_job_failed_total이 errorCode 태그로 증가한다`() {
        whenever(indexingJobRepository.currentDbTimestamp()).thenReturn(testNow)
        whenever(indexingJobRepository.findByIdForUpdate(1L)).thenReturn(processingJob())

        service.recordFailure(
            token = testToken, errorCode = "HASH_MISMATCH", errorMessage = "mismatch", permanent = true,
            maxAttempts = 5, baseDelay = Duration.ofSeconds(30),
        )

        assertThat(meterRegistry.get("indexing_job_failed_total").tag("errorCode", "HASH_MISMATCH").counter().count())
            .isEqualTo(1.0)
    }

    @Test
    fun `RETRY_WAIT으로 끝나면 indexing_job_failed_total이 증가하지 않는다`() {
        whenever(indexingJobRepository.currentDbTimestamp()).thenReturn(testNow)
        whenever(indexingJobRepository.findByIdForUpdate(1L)).thenReturn(processingJob(attemptCount = 1))

        service.recordFailure(
            token = testToken, errorCode = "TIMEOUT", errorMessage = "timeout", permanent = false,
            maxAttempts = 5, baseDelay = Duration.ofSeconds(30),
        )

        assertThat(meterRegistry.find("indexing_job_failed_total").counters()).isEmpty()
    }
}
