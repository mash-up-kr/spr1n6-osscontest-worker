package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryPolicy
import com.osscontest.worker.indexing.queue.ExecutionLostException
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.LeaseExpiredException
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.time.Duration

@Service
class IndexingFailureService(
    private val indexingJobRepository: IndexingJobRepository,
    private val meterRegistry: MeterRegistry,
    private val ownership: JobOwnershipGuard,
    private val retryPolicy: RetryPolicy,
) {
    @Transactional
    fun recordFailure(
        token: ExecutionToken,
        errorCode: String,
        errorMessage: String,
        permanent: Boolean,
        maxAttempts: Int,
        baseDelay: Duration,
        retryAfter: String? = null,
    ): IndexingJobStatus {
        val job = ownership.lockForResult(token)
        if (job.executionEpoch != token.epoch) throw ExecutionLostException(token)
        // The failure commit clears the owner. Its unchanged epoch proves this execution's release.
        if (job.workerId == null && job.leaseUntil == null &&
            job.status in setOf(IndexingJobStatus.RETRY_WAIT, IndexingJobStatus.FAILED)) return job.status
        if (job.workerId != token.workerId) throw ExecutionLostException(token)
        if (job.status == IndexingJobStatus.COMPLETED) return job.status
        ownership.requireProcessing(job, token)
        val decision = retryPolicy.decideFailure(permanent, job.attemptCount, maxAttempts, baseDelay,
            indexingJobRepository.currentDbTimestamp(), retryAfter)
        if (indexingJobRepository.fail(token.jobId, token.workerId, token.epoch, decision.status.name,
                decision.nextRetryAt, errorCode.take(100), errorMessage.take(1000)) != 1) {
            throw LeaseExpiredException()
        }
        val metric = if (decision.status == IndexingJobStatus.FAILED) "indexing_job_failed_total"
            else "indexing_retry_scheduled_total"
        val count = { meterRegistry.counter(metric, "errorCode", errorCode.take(100)).increment() }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCommit() = count()
            })
        } else count()
        return decision.status
    }
}
