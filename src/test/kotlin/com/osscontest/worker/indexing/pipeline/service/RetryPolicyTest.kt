package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.support.testNow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class RetryPolicyTest {
    private val base = Duration.ofSeconds(30)

    @Test
    fun `각 회차의 선형 지연은 지터 양쪽 경계를 포함한다`() {
        for (attempt in 1..4) {
            for (factor in listOf(0.8, 1.0, 1.2)) {
                val result = RetryPolicy(RetryJitter { factor }).decideFailure(false, attempt, 5, base, testNow)
                assertThat(result.status).isEqualTo(IndexingJobStatus.RETRY_WAIT)
                assertThat(result.nextRetryAt).isEqualTo(testNow.plusMillis((30_000 * attempt * factor).toLong()))
            }
        }
    }

    @Test
    fun `영구 실패와 한도 소진은 난수나 재시도 시각을 만들지 않는다`() {
        val policy = RetryPolicy(RetryJitter { error("must not sample") })
        assertThat(policy.decideFailure(true, 1, 5, base, testNow))
            .isEqualTo(FailureDecision(IndexingJobStatus.FAILED, null))
        assertThat(policy.decideFailure(false, 5, 5, base, testNow))
            .isEqualTo(FailureDecision(IndexingJobStatus.FAILED, null))
    }

    @Test
    fun `Retry After는 선형 지연을 줄이지 않고 더 늦은 공급자 시각을 존중한다`() {
        val policy = RetryPolicy(RetryJitter { 1.0 })
        listOf(null, "garbage", "-1", "10", "9223372036854775807").forEach {
            assertThat(policy.decideFailure(false, 1, 5, base, testNow, it).nextRetryAt)
                .isEqualTo(testNow.plusSeconds(30))
        }
        assertThat(policy.decideFailure(false, 1, 5, base, testNow, "60").nextRetryAt).isEqualTo(testNow.plusSeconds(60))
        assertThat(policy.decideFailure(false, 1, 5, base, testNow, "Sat, 10 Oct 2026 00:02:00 GMT").nextRetryAt)
            .isEqualTo(testNow.plusSeconds(120))
    }
}
