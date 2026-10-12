package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import com.osscontest.worker.indexing.embedding.service.EmbeddingQuotaExceededException
import com.osscontest.worker.indexing.embedding.service.EmbeddingConfigurationException
import com.osscontest.worker.indexing.pipeline.service.IndexingErrorClassifier
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

/** Retains actual responses/errors through circuit DB waits; never retries an actual API failure inline. */
@Component
class ExternalApiOperations(
    private val execution: ExecutionOperations,
    private val database: DatabaseOperations,
    private val breaker: CircuitBreaker,
    private val waiter: RetryWaiter,
    private val properties: DatabaseWaitProperties,
    private val publication: IndexingPublicationService,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val callIds = AtomicLong()

    fun <T> execute(token: ExecutionToken, attempt: Int = 1, action: () -> T): T {
        val callId = "${token.workerId}-${callIds.updateAndGet { Math.incrementExact(it) }}"
        var recorded = false
        try {
            while (true) {
                database.awaitAvailable()
                checkpoint(token)
                val admission = database.execute(afterRecovery = { checkpoint(token) }) { breaker.acquire(token, callId) }
                if (admission is CallAdmission.Blocked) {
                    waiter.waitFor(minOf(admission.delay, properties.checkInterval))
                    continue
                }
                val permit = (admission as CallAdmission.Allowed).permit
                checkpoint(token)
                if (!database.execute(afterRecovery = { checkpoint(token) }) { breaker.valid(permit) }) continue
                // Only exceptions from the actual external action enter provider error classification.
                val result = try { action() } catch (error: Exception) {
                    if (error is InterruptedException || Thread.currentThread().isInterrupted ||
                        error is ExecutionLostException || error is ExecutionCompletedException || error is LeaseExpiredException) throw error
                    val outcome = when (error) {
                        is EmbeddingRateLimitedException -> CircuitOutcome.RATE_LIMIT
                        is EmbeddingQuotaExceededException, is EmbeddingConfigurationException -> CircuitOutcome.CONFIGURATION
                        else -> if (IndexingErrorClassifier.classify(error).retryable) CircuitOutcome.FAILURE else CircuitOutcome.PERMANENT
                    }
                    val record = CircuitResult(outcome, attempt, (error as? EmbeddingRateLimitedException)?.retryAfter)
                    if (outcome == CircuitOutcome.CONFIGURATION || outcome == CircuitOutcome.PERMANENT) {
                        log.error("EMBEDDING_CALL_REJECTED jobId={} cause={} message={}", token.jobId,
                            IndexingErrorClassifier.classify(error).code, error.message)
                    }
                    database.execute { breaker.record(permit, record) }
                    recorded = true
                    throw error
                }
                val record = CircuitResult(CircuitOutcome.SUCCESS)
                database.execute { breaker.record(permit, record) }
                recorded = true
                return result
            }
        } finally {
            if (!recorded) {
                try { database.execute { breaker.release(token, callId) } }
                catch (_: InterruptedException) { Thread.currentThread().interrupt() }
            }
        }
    }

    private fun checkpoint(token: ExecutionToken) {
        execution.checkpoint(token)
        if (execution.database(token) { publication.preflight(token) }) throw ExecutionCompletedException(token)
    }
}
