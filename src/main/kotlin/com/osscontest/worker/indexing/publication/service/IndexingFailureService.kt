package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryPolicy
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.queue.ExecutionLostException
import com.osscontest.worker.indexing.queue.ExecutionResults
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.FailureRecord
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.LeaseExpiredException
import com.osscontest.worker.indexing.queue.isCompletedExecution
import io.micrometer.core.instrument.MeterRegistry
import java.time.Duration
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager

@Service
class IndexingFailureService(
    private val indexingJobRepository: IndexingJobRepository,
    private val meterRegistry: MeterRegistry,
    private val ownership: JobOwnershipGuard,
    private val retryPolicy: RetryPolicy,
    private val results: ExecutionResults,
) {
    /**
     * 실패를 RETRY_WAIT 또는 FAILED로 기록하고 Worker·lease를 비운다.
     * 같은 실행으로 다시 호출하면 처음 내린 결정을 재사용하며, 메트릭은 커밋 뒤에 한 번만 센다.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun recordFailure(
        token: ExecutionToken,
        errorCode: String,
        errorMessage: String,
        permanent: Boolean,
        maxAttempts: Int,
        baseDelay: Duration,
        retryAfter: String? = null,
    ): IndexingJobStatus {
        val execution = ownership.lockForResult(token)
        val job = execution.job
        if (job.executionEpoch != token.epoch) throw ExecutionLostException(token)
        if (job.isCompletedExecution(token)) return job.status
        val previous = results.failure(token)
        if (previous?.matches(job, token) == true) {
            previous.confirmed.set(true)
            return job.status
        }
        ownership.requireProcessing(execution, token)
        // 코드는 Server 스키마 last_error_code VARCHAR(100)에 맞춘다. 메시지는 조회 API로 그대로 노출되므로 1000자로 자른다.
        val record = results.prepare(token) {
            FailureRecord(retryPolicy.decideFailure(permanent, job.attemptCount, maxAttempts, baseDelay,
                indexingJobRepository.currentDbTimestamp(), retryAfter), errorCode.take(100), errorMessage.take(1000))
        }
        val decision = record.decision
        if (indexingJobRepository.fail(token.jobId, token.workerId, token.epoch, decision.status.name,
                decision.nextRetryAt, record.code, record.message) != 1) {
            throw LeaseExpiredException()
        }
        val metric = if (decision.status == IndexingJobStatus.FAILED) "indexing_job_failed_total"
            else "indexing_retry_scheduled_total"
        val count = {
            if (record.confirmed.compareAndSet(false, true)) meterRegistry.counter(metric, "errorCode", record.code).increment()
        }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCommit() = count()
            })
        } else count()
        return decision.status
    }
}
