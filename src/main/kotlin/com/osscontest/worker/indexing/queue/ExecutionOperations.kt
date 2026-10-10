package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import org.springframework.stereotype.Component

@Component
class ExecutionOperations(
    private val database: DatabaseOperations,
    private val store: JobQueueStore,
) {
    fun checkpoint(token: ExecutionToken, allowCompleted: Boolean = false) {
        database.execute {
            var job = store.snapshot(token) ?: throw ExecutionLostException(token)
            if (!job.matches(token)) throw ExecutionLostException(token)
            if (job.status == IndexingJobStatus.COMPLETED) {
                if (allowCompleted) return@execute
                throw ExecutionCompletedException(token)
            }
            if (job.status !in ownedStatuses || job.leaseUntil == null) throw ExecutionLostException(token)
            if (job.leaseUntil <= job.now) {
                if (store.renew(token) != 1) throw ExecutionLostException(token)
                job = store.snapshot(token) ?: throw ExecutionLostException(token)
                if (!job.matches(token) || job.status !in ownedStatuses) throw ExecutionLostException(token)
                if (job.leaseUntil == null || job.leaseUntil <= job.now) throw LeaseExpiredException()
            }
        }
    }

    fun <T> database(token: ExecutionToken, allowCompleted: Boolean = false, action: () -> T): T {
        while (true) {
            try {
                return database.execute(afterRecovery = { checkpoint(token, allowCompleted) }, action = action)
            } catch (_: LeaseExpiredException) {
                // The failed transaction has unwound before renewing and recalling it.
                checkpoint(token, allowCompleted)
            }
        }
    }

    /** Resolve a released failure before the normal recovery checkpoint rejects its cleared owner. */
    fun <T> failure(token: ExecutionToken, action: () -> T): T {
        while (true) {
            try {
                return database.execute(action = action)
            } catch (_: LeaseExpiredException) {
                checkpoint(token, allowCompleted = true)
            }
        }
    }
}
