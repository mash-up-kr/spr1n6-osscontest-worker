package com.osscontest.worker.indexing.queue

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.Duration
import java.time.Instant

class CircuitBreakerTest {
    private val now = Instant.parse("2026-10-11T00:00:00Z")
    private val store: CircuitBreakerStore = mock()
    private val properties = CircuitBreakerProperties(failureThreshold = 5, openDuration = Duration.ofSeconds(60))
    private val breaker = CircuitBreaker(store, properties, SimpleMeterRegistry())

    private fun snapshot(state: BreakerState, openedUntil: Instant? = null) = BreakerSnapshot(state, openedUntil, now)

    @Test
    fun `CLOSED면 기다리지 않고 claim도 정상이다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(snapshot(BreakerState.CLOSED))
        assertThat(breaker.acquire()).isEqualTo(Duration.ZERO)
        assertThat(breaker.admission()).isEqualTo(ClaimAdmission.NORMAL)
    }

    @Test
    fun `행이 없으면 breaker 없이 동작한다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(null)
        assertThat(breaker.acquire()).isEqualTo(Duration.ZERO)
        assertThat(breaker.admission()).isEqualTo(ClaimAdmission.NORMAL)
        whenever(store.recordFailure(any(), any(), any())).thenReturn(null)
        assertThat(breaker.recordTransientFailure()).isFalse()
    }

    @Test
    fun `OPEN 유지 시간이 남았으면 남은 시간만큼 기다리고 claim을 막는다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(snapshot(BreakerState.OPEN, now.plusSeconds(20)))
        assertThat(breaker.acquire()).isEqualTo(Duration.ofSeconds(20))
        assertThat(breaker.admission()).isEqualTo(ClaimAdmission.BLOCKED)
        verify(store, never()).tryStartProbe(any(), any())
    }

    @Test
    fun `HALF_OPEN이고 시험 호출 시간이 남았으면 다른 호출은 기다린다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(snapshot(BreakerState.HALF_OPEN, now.plusSeconds(30)))
        assertThat(breaker.acquire()).isEqualTo(Duration.ofSeconds(30))
        assertThat(breaker.admission()).isEqualTo(ClaimAdmission.BLOCKED)
    }

    @Test
    fun `유지 시간이 지났고 시험 호출권을 얻으면 바로 호출한다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(snapshot(BreakerState.OPEN, now.minusSeconds(1)))
        whenever(store.tryStartProbe("embedding-api", 60_000)).thenReturn(true)
        assertThat(breaker.acquire()).isEqualTo(Duration.ZERO)
        assertThat(breaker.admission()).describedAs("시험 호출 자격이 있으면 claim은 1건으로 제한한다")
            .isEqualTo(ClaimAdmission.LIMITED)
    }

    @Test
    fun `시험 호출권을 얻지 못하면 기다린다`() {
        whenever(store.snapshot("embedding-api")).thenReturn(snapshot(BreakerState.OPEN, now.minusSeconds(1)))
        whenever(store.tryStartProbe("embedding-api", 60_000)).thenReturn(false)
        assertThat(breaker.acquire()).isEqualTo(Duration.ofSeconds(60))
    }

    @Test
    fun `일시 실패 기록은 OPEN이 되었을 때만 true를 돌려준다`() {
        whenever(store.recordFailure("embedding-api", 5, 60_000))
            .thenReturn(BreakerState.CLOSED).thenReturn(BreakerState.OPEN)
        assertThat(breaker.recordTransientFailure()).isFalse()
        assertThat(breaker.recordTransientFailure()).isTrue()
    }

    @Test
    fun `성공 기록은 저장소에 위임한다`() {
        whenever(store.recordSuccess("embedding-api")).thenReturn(1)
        breaker.recordSuccess()
        verify(store).recordSuccess("embedding-api")
    }
}
