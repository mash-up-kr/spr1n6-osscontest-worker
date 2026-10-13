package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

// 같은 시각에 실패한 Job들이 한꺼번에 다시 claim되지 않도록 지연을 ±20% 흩는다.
private val JITTER_RANGE = 0.8..1.2

fun interface RetryJitter {
    fun factor(): Double
}

@Component
class RandomRetryJitter : RetryJitter {
    override fun factor(): Double = Random.nextDouble(JITTER_RANGE.start, JITTER_RANGE.endInclusive)
}

data class FailureDecision(val status: IndexingJobStatus, val nextRetryAt: Instant?)

@Component
class RetryPolicy(private val jitter: RetryJitter) {
    fun decideFailure(
        permanent: Boolean,
        attemptCount: Int,
        maxAttempts: Int,
        baseDelay: Duration,
        dbNow: Instant,
        retryAfter: String? = null,
    ): FailureDecision {
        require(maxAttempts > 0)
        require(baseDelay.toMillis() > 0)
        if (permanent || attemptCount >= maxAttempts) {
            return FailureDecision(IndexingJobStatus.FAILED, null)
        }
        val factor = jitter.factor()
        require(factor in JITTER_RANGE)
        val delayMillis = (baseDelay.toMillis().toDouble() * attemptCount.coerceAtLeast(1) * factor)
            .toLong().coerceAtLeast(1)
        val linearDeadline = dbNow.plusMillis(delayMillis)
        // Retry-After는 공급자가 정한 최소 대기다. Job backoff를 줄이는 데 쓰지 않는다.
        val providerDeadline = retryAfterDeadline(retryAfter, dbNow)
        return FailureDecision(IndexingJobStatus.RETRY_WAIT,
            providerDeadline?.let { maxOf(linearDeadline, it) } ?: linearDeadline)
    }

    private fun retryAfterDeadline(value: String?, now: Instant): Instant? {
        val raw = value?.trim() ?: return null
        val seconds = raw.toLongOrNull()
        if (seconds != null) {
            return seconds.takeIf { it >= 0 }?.let { runCatching { now.plusSeconds(it) }.getOrNull() }
        }
        return runCatching { ZonedDateTime.parse(raw, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant() }.getOrNull()
    }
}
