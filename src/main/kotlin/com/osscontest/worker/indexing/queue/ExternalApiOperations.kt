package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore

@Component
class ExternalApiOperations(
    private val execution: ExecutionOperations,
    private val database: DatabaseOperations,
    private val store: JobQueueStore,
    private val failures: IndexingFailureService,
    private val waiter: RetryWaiter,
    private val clock: Clock,
    private val properties: DatabaseWaitProperties,
    @Value("\${indexing.retry.max-attempts:5}") private val maxAttempts: Int,
    @Value("\${indexing.retry.base-delay:PT30S}") private val baseDelay: Duration,
) {
    private val pending = ConcurrentHashMap<Long, Instant>()
    private val retryPermit = Semaphore(1, true)

    fun retryOnly(): Boolean = pending.isNotEmpty()

    fun refreshTargets() {
        pending.keys.forEach { id ->
            val snapshot = database.execute { store.snapshot(ExecutionToken(id, "", 0)) }
            if (snapshot == null || snapshot.status in setOf(IndexingJobStatus.COMPLETED, IndexingJobStatus.FAILED)) {
                pending.remove(id)
            }
        }
    }

    fun <T> execute(token: ExecutionToken, action: () -> T): T {
        while (true) {
            execution.checkpoint(token)
            database.awaitAvailable()
            val remaining = cooldownRemaining()
            if (remaining.isPositive) {
                waiter.waitFor(minOf(remaining, properties.checkInterval))
                continue
            }
            val serialize = retryOnly() || database.execute { store.hasRetries() }
            if (serialize) retryPermit.acquire()
            try {
                execution.checkpoint(token)
                // A different request may have returned 429 while this one waited for its permit.
                if (cooldownRemaining().isPositive) continue
                try {
                    return action()
                } catch (error: EmbeddingRateLimitedException) {
                    val initialDeadline = retryDeadline(error.retryAfter, clock.instant(), baseDelay)
                    pending.merge(token.jobId, initialDeadline) { old, new -> maxOf(old, new) }
                    val now = execution.database(token, allowCompleted = true) { store.dbNow() }
                    val deadline = retryDeadline(error.retryAfter, now, baseDelay)
                    pending[token.jobId] = deadline
                    val status = execution.database(token, allowCompleted = true) {
                        failures.recordFailure(token, "RATE_LIMITED", error.message.orEmpty(), false,
                            maxAttempts, baseDelay, deadline)
                    }
                    if (status == IndexingJobStatus.COMPLETED) throw ExecutionCompletedException(token)
                    if (status != IndexingJobStatus.RETRY_WAIT) throw ExecutionLostException(token)
                }
            } finally {
                if (serialize) retryPermit.release()
            }
            // Same call stack and batch inputs survive backoff; successful batches are untouched.
            execution.awaitRetry(token)
        }
    }

    private fun cooldownRemaining(): Duration {
        val remote = database.execute { store.cooldown() }
        val local = pending.values.maxOrNull()
        val deadline = listOfNotNull(remote, local).maxOrNull() ?: return Duration.ZERO
        val now = database.execute { store.dbNow() }
        return Duration.between(now, deadline)
    }

    companion object {
        fun retryDeadline(value: String?, now: Instant, fallback: Duration): Instant {
            val raw = value?.trim()
            val seconds = raw?.toLongOrNull()
            if (seconds != null && seconds >= 0) {
                return runCatching { now.plusSeconds(seconds) }.getOrDefault(now.plus(fallback))
            }
            return runCatching { ZonedDateTime.parse(raw, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant() }
                .getOrNull()?.let { maxOf(now, it) } ?: now.plus(fallback)
        }
    }
}
