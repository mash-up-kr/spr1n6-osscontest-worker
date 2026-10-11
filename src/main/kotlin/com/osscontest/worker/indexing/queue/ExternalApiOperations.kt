package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import com.osscontest.worker.indexing.pipeline.service.IndexingErrorClassifier
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class ExternalApiOperations(
    private val execution: ExecutionOperations,
    private val database: DatabaseOperations,
    private val breaker: CircuitBreaker,
    private val waiter: RetryWaiter,
    private val properties: DatabaseWaitProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun <T> execute(token: ExecutionToken, action: () -> T): T {
        while (true) {
            database.awaitAvailable()
            execution.checkpoint(token)
            // OPEN이면 호출하지 않고 기다린다. 실패가 만들어지지 않으므로 시도 횟수가 소비되지 않는다.
            val breakerWait = database.execute { breaker.acquire() }
            if (breakerWait.isPositive) {
                waiter.waitFor(minOf(breakerWait, properties.checkInterval))
                continue
            }
            try {
                val result = action()
                database.execute { breaker.recordSuccess() }
                return result
            } catch (error: Exception) {
                if (!isBreakerFailure(error)) throw error
                // 열렸다면 이 실패를 삼키고 위로 돌아가 열려 있는 동안 기다린 뒤 같은 호출을 재시도한다.
                if (!database.execute { breaker.recordTransientFailure() }) throw error
                log.warn("EMBEDDING_CALL_DEFERRED_BY_CIRCUIT_BREAKER jobId={} error={}", token.jobId, error.javaClass.simpleName)
            }
        }
    }

    /**
     * 재시도로 풀릴 수 있는 임베딩 호출 실패만 센다. 실행권 상실·중단·Rate Limit은 API 장애가 아니다.
     * 결제 한도 초과(`EmbeddingQuotaExceededException`)는 분류기에서 일시 오류이므로 여기서 센다.
     */
    private fun isBreakerFailure(error: Exception): Boolean =
        error !is InterruptedException && !Thread.currentThread().isInterrupted &&
            error !is ExecutionLostException && error !is ExecutionCompletedException && error !is LeaseExpiredException &&
            error !is EmbeddingRateLimitedException &&
            IndexingErrorClassifier.classify(error).retryable
}