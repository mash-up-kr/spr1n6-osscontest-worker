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

/**
 * 외부 API 호출을 breaker 허가 아래에서 실행한다. breaker 기록 중 DB를 기다려도 실제 응답·오류를 잃지 않으며,
 * 실패한 API 호출은 이 자리에서 다시 보내지 않는다.
 */
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
                    // 열림 기한까지 한 번에 자지 않는다. DB 확인 간격마다 실행권·문서 삭제·breaker 상태를 다시 확인한다.
                    waiter.waitFor(minOf(admission.delay, properties.checkInterval))
                    continue
                }
                val permit = (admission as CallAdmission.Allowed).permit
                checkpoint(token)
                if (!database.execute(afterRecovery = { checkpoint(token) }) { breaker.valid(permit) }) continue
                // 실제 외부 호출에서 난 예외만 공급자 오류로 분류한다.
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
            // 결과를 기록하지 못하고 빠져나가면 받은 시험 호출을 반납한다.
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
