package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.Instant

@Service
class IndexingFailureService(
    private val indexingJobRepository: IndexingJobRepository,
    private val meterRegistry: MeterRegistry,
    private val ownership: JobOwnershipGuard,
) {
    @Transactional
    fun recordFailure(
        token: ExecutionToken,
        errorCode: String,
        errorMessage: String,
        permanent: Boolean,
        maxAttempts: Int,
        baseDelay: Duration,
        retryAt: Instant? = null,
    ): IndexingJobStatus {
        val job = ownership.lock(token)
        // Recalling after a lost commit response must not consume another attempt or move the deadline.
        if (job.status in setOf(IndexingJobStatus.COMPLETED, IndexingJobStatus.FAILED, IndexingJobStatus.RETRY_WAIT)) {
            return job.status
        }
        ownership.requireProcessing(job, token)
        val now = indexingJobRepository.currentDbTimestamp()
        job.lastErrorCode = errorCode.take(100)
        job.lastErrorMessage = errorMessage.take(1000)
        job.updatedAt = now
        return if (permanent || job.attemptCount >= maxAttempts) {
            job.status = IndexingJobStatus.FAILED
            job.nextRetryAt = null
            job.completedAt = now
            meterRegistry.counter("indexing_job_failed_total", "errorCode", job.lastErrorCode!!).increment()
            IndexingJobStatus.FAILED
        } else {
            job.status = IndexingJobStatus.RETRY_WAIT
            job.nextRetryAt = retryAt
                ?: now.plus(baseDelay.multipliedBy(job.attemptCount.coerceAtLeast(1).toLong()))
            job.completedAt = null
            IndexingJobStatus.RETRY_WAIT
        }
    }
}
