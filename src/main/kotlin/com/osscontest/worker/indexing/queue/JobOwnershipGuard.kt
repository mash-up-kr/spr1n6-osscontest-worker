package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.stereotype.Component

/** All callers retain the document and Job locks until their write transaction finishes. */
@Component
class JobOwnershipGuard(private val locks: JobExecutionLocks) {
    fun lockForResult(token: ExecutionToken): LockedJobExecution =
        locks.lock(token.jobId) ?: throw ExecutionLostException(token)

    fun lock(token: ExecutionToken): IndexingJobEntity {
        val job = lockForResult(token).job
        if (job.workerId != token.workerId || job.executionEpoch != token.epoch) throw ExecutionLostException(token)
        return job
    }

    fun requireProcessing(job: IndexingJobEntity, token: ExecutionToken) {
        check(job.id == token.jobId)
        requireProcessing(lockForResult(token), token)
    }

    /** Reuse the locked projection only within its transaction; final writes recheck the DB clock. */
    fun requireProcessing(execution: LockedJobExecution, token: ExecutionToken) {
        locks.requireTransaction()
        if (!owns(execution, token)) throw ExecutionLostException(token)
        if (!canWrite(execution, token)) throw LeaseExpiredException()
    }

    fun owns(execution: LockedJobExecution, token: ExecutionToken): Boolean =
        execution.job.id == token.jobId && execution.snapshot().ownsProcessing(token)

    fun canWrite(execution: LockedJobExecution, token: ExecutionToken): Boolean =
        execution.job.id == token.jobId && execution.snapshot().canWrite(token)
}

/** Used with alias j and ExecutionToken parameters. Renew permits an expired, unrecovered lease. */
internal object JobOwnershipSql {
    const val OWNED_PROCESSING = """
        j.id = :id AND j.worker_id = :workerId AND j.execution_epoch = :epoch
        AND j.status = 'PROCESSING' AND j.lease_until IS NOT NULL
        AND EXISTS (SELECT 1 FROM document d WHERE d.id = j.document_id AND d.execution_epoch = :epoch)
    """
    const val WRITABLE_PROCESSING = "$OWNED_PROCESSING AND j.lease_until > clock_timestamp()"
}
