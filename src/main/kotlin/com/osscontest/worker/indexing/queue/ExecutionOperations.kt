package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class ExecutionOperations(
    private val database: DatabaseOperations,
    private val store: JobQueueStore,
    private val waiter: RetryWaiter,
    private val properties: DatabaseWaitProperties,
    @Value("\${indexing.retry.max-attempts:5}") private val maxAttempts: Int,
) {
    fun checkpoint(token: ExecutionToken, allowCompleted: Boolean = false) {
        database.execute {
            var job = store.snapshot(token) ?: throw ExecutionLostException(token)
            if (!job.matches(token)) throw ExecutionLostException(token)
            if (job.status == IndexingJobStatus.COMPLETED) {
                if (allowCompleted) return@execute
                throw ExecutionCompletedException(token)
            }
            if (job.status !in ownedStatuses || job.leaseExpiresAt == null) throw ExecutionLostException(token)
            if (job.leaseExpiresAt <= job.now) {
                if (store.renew(token) != 1) throw ExecutionLostException(token)
                job = store.snapshot(token) ?: throw ExecutionLostException(token)
                if (!job.matches(token) || job.status !in ownedStatuses) throw ExecutionLostException(token)
                if (job.leaseExpiresAt == null || job.leaseExpiresAt <= job.now) throw LeaseExpiredException()
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

    fun awaitRetry(token: ExecutionToken) {
        while (true) {
            checkpoint(token)
            val snapshot = database(token) { checkNotNull(store.snapshot(token)) }
            if (snapshot.status == IndexingJobStatus.PROCESSING) return // prior resume committed
            if (snapshot.status != IndexingJobStatus.RETRY_WAIT) throw ExecutionLostException(token)
            val deadline = checkNotNull(snapshot.retryAt)
            val remaining = Duration.between(snapshot.now, deadline)
            if (remaining.isPositive) {
                waiter.waitFor(minOf(remaining, properties.checkInterval))
            } else if (database(token) { store.resumeRetry(token, maxAttempts) } == 1) {
                return
            }
        }
    }
}
