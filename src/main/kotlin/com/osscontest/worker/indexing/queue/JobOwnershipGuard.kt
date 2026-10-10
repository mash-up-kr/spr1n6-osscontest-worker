package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.springframework.stereotype.Component

/** Used inside a write transaction: the Job lock fences every write in that transaction. */
@Component
class JobOwnershipGuard(private val jobs: IndexingJobRepository, private val store: JobQueueStore) {
    fun lock(token: ExecutionToken): IndexingJobEntity {
        val job = jobs.findByIdForUpdate(token.jobId) ?: throw ExecutionLostException(token)
        if (job.workerId != token.workerId || job.executionEpoch != token.epoch) throw ExecutionLostException(token)
        return job
    }

    fun requireProcessing(job: IndexingJobEntity, token: ExecutionToken) {
        if (job.status != IndexingJobStatus.PROCESSING || job.leaseUntil == null) throw ExecutionLostException(token)
        if (!job.leaseUntil!!.isAfter(store.dbNow())) throw LeaseExpiredException()
    }
}
