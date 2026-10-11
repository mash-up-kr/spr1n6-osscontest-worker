package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.support.testNow
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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

    @Test
    fun `Retry After의 0 동일 기한 과거 날짜는 지연을 줄이지 않고 앞뒤 공백은 허용한다`() {
        val policy = RetryPolicy(RetryJitter { 1.0 })
        for (value in listOf("0", "30", "Fri, 09 Oct 2026 00:00:00 GMT")) {
            assertThat(policy.decideFailure(false, 1, 5, base, testNow, value).nextRetryAt).isEqualTo(testNow.plusSeconds(30))
        }
        assertThat(policy.decideFailure(false, 1, 5, base, testNow, " 60 ").nextRetryAt).isEqualTo(testNow.plusSeconds(60))
    }

    @Test
    fun `범위를 벗어나거나 유한하지 않은 지터는 임의 deadline으로 저장하지 않는다`() {
        for (factor in listOf(0.79, 1.21, Double.NaN, Double.POSITIVE_INFINITY)) {
            assertThatThrownBy { RetryPolicy(RetryJitter { factor }).decideFailure(false, 1, 5, base, testNow) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
