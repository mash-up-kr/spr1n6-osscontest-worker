/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.osscontest.worker.support.testNow
import com.osscontest.worker.support.testToken
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.Duration

class CircuitBreakerTest {
    private val store: CircuitBreakerStore = mock()
    private val breaker = CircuitBreaker(store, CircuitBreakerProperties(), SimpleMeterRegistry())
    @Test fun `CLOSED면 claim 정상이며 호출권을 요청 ID와 함께 저장소에서 얻는다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(BreakerSnapshot(BreakerState.CLOSED, null, testNow))
        val permission = CallAdmission.Allowed(CircuitPermit("embedding-api", 0, "1"))
        whenever(store.acquire("embedding-api", testToken, "1", 90_000)).thenReturn(permission)
        assertThat(breaker.admission()).isEqualTo(ClaimAdmission.NORMAL)
        assertThat(breaker.acquire(testToken, "1")).isEqualTo(permission)
    }
    @Test fun `필수 서킷 행이 없으면 서킷 없이 실행하지 않는다`() {
        whenever(store.snapshot("embedding-api")).thenThrow(IllegalStateException("missing"))
        assertThatThrownBy { breaker.admission() }.isInstanceOf(IllegalStateException::class.java)
    }
    @Test fun `OPEN과 HALF_OPEN 유효 시간에는 새 활성 INDEX claim을 막고 만료 뒤 LIMITED다`() {
        for (state in listOf(BreakerState.OPEN, BreakerState.HALF_OPEN)) {
            whenever(store.snapshot("embedding-api")).thenReturn(BreakerSnapshot(state, testNow.plusSeconds(30), testNow,
                probeUntil = testNow.plusSeconds(30)))
            assertThat(breaker.admission()).isEqualTo(ClaimAdmission.BLOCKED)
            whenever(store.snapshot("embedding-api")).thenReturn(BreakerSnapshot(state, testNow.minusSeconds(1), testNow,
                probeUntil = testNow.minusSeconds(1)))
            assertThat(breaker.admission()).isEqualTo(ClaimAdmission.LIMITED)
        }
    }
    @Test fun `호출권과 결과는 같은 세대 식별자로 저장소에 전달한다`() {
        val permit = CircuitPermit("embedding-api", 5, "7", testToken.workerId)
        val result = CircuitResult(CircuitOutcome.FAILURE)
        whenever(store.record(eq(permit), eq(result), eq(5), eq(60_000L), any())).thenReturn(BreakerState.OPEN)
        breaker.record(permit, result)
        verify(store).record(eq(permit), eq(result), eq(5), eq(60_000L), any())
    }
    @Test fun `공유 시험의 429 시각은 결정적 지터와 유효한 RetryAfter 중 늦은 값을 사용한다`() {
        val actual=CircuitBreaker(store,CircuitBreakerProperties(),SimpleMeterRegistry(),
            com.osscontest.worker.indexing.pipeline.service.RetryPolicy { 0.8 })
        for((header,seconds) in listOf(null to 48L,"7" to 48L,"120" to 120L,"invalid" to 48L)) {
            val result=CircuitResult(CircuitOutcome.RATE_LIMIT,2,header)
            whenever(store.record(any(),eq(result),any(),any(),any())).thenAnswer {
                val deadline=it.getArgument<(java.time.Instant)->java.time.Instant>(4)(testNow)
                assertThat(deadline).isEqualTo(testNow.plusSeconds(seconds)); BreakerState.OPEN
            }
            actual.record(CircuitPermit("embedding-api",1,"rate-$header",testToken.workerId),result)
        }
    }
    @Test fun `호출 TTL은 임베딩 timeout보다 길어야 한다`() {
        assertThatThrownBy { CircuitBreaker(store, CircuitBreakerProperties(callDuration = Duration.ofSeconds(60)), SimpleMeterRegistry()) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
