package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.stereotype.Component

/** All callers retain the document and Job locks until their write transaction finishes. */
@Component
class JobOwnershipGuard(private val locks: JobExecutionLocks) {
    fun lockForResult(token: ExecutionToken): IndexingJobEntity =
        locks.lock(token.jobId)?.job ?: throw ExecutionLostException(token)

    fun lock(token: ExecutionToken): IndexingJobEntity {
        val job = lockForResult(token)
        if (job.workerId != token.workerId || job.executionEpoch != token.epoch) throw ExecutionLostException(token)
        return job
    }

    fun requireProcessing(job: IndexingJobEntity, token: ExecutionToken) {
        check(job.id == token.jobId)
        val current = locks.lock(token.jobId) ?: throw ExecutionLostException(token)
        if (!owns(current, token)) throw ExecutionLostException(token)
        if (!current.job.leaseUntil!!.isAfter(current.now)) throw LeaseExpiredException()
    }

    fun owns(execution: LockedJobExecution, token: ExecutionToken): Boolean = execution.job.let {
        it.id == token.jobId && it.status == IndexingJobStatus.PROCESSING && it.workerId == token.workerId &&
            it.executionEpoch == token.epoch && execution.documentEpoch == token.epoch && it.leaseUntil != null
    }

    fun canWrite(execution: LockedJobExecution, token: ExecutionToken): Boolean =
        owns(execution, token) && execution.job.leaseUntil!!.isAfter(execution.now)
}
