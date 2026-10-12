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
            if (job.completed(token)) {
                if (allowCompleted) return@execute
                throw ExecutionCompletedException(token)
            }
            if (!job.ownsProcessing(token)) throw ExecutionLostException(token)
            if (!job.canWrite(token)) {
                if (store.renew(token) != 1) throw ExecutionLostException(token)
                job = store.snapshot(token) ?: throw ExecutionLostException(token)
                if (!job.ownsProcessing(token)) throw ExecutionLostException(token)
                if (!job.canWrite(token)) throw LeaseExpiredException()
            }
        }
    }

    /** Rechecks the primary receipt on every retry, before invoking retained result writes. */
    fun <T> completion(token: ExecutionToken, identity: ExecutionIdentity, completed: () -> T, action: () -> T): T {
        while (true) {
            try {
                return database.execute {
                    val snapshot = store.snapshot(token) ?: throw ExecutionLostException(token)
                    if (!identity.matches(snapshot)) throw ExecutionLostException(token)
                    if (snapshot.completed(token)) completed()
                    else {
                        checkpoint(token)
                        action()
                    }
                }
            } catch (_: LeaseExpiredException) {
                checkpoint(token, allowCompleted = true)
            }
        }
    }

    fun <T> database(token: ExecutionToken, action: () -> T): T {
        while (true) {
            try {
                return database.execute(afterRecovery = { checkpoint(token) }, action = action)
            } catch (_: LeaseExpiredException) {
                // The failed transaction has unwound before renewing and recalling it.
                checkpoint(token)
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

data class ExecutionIdentity(val type: JobType, val documentId: Long, val documentVersionId: Long?) {
    constructor(job: ClaimedJob) : this(job.type, job.documentId, job.documentVersionId)
    fun matches(snapshot: JobSnapshot) = snapshot.type == type && snapshot.documentId == documentId &&
        snapshot.documentVersionId == documentVersionId
}
