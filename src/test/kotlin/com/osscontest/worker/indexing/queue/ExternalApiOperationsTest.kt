package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.embedding.service.EmbeddingQuotaExceededException
import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import com.osscontest.worker.indexing.embedding.service.EmbeddingRequestRejectedException
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.Duration

class ExternalApiOperationsTest {
    private fun simpleApi(breaker: CircuitBreaker, waiter: RetryWaiter = RetryWaiter {}): ExternalApiOperations =
        ExternalApiOperations(directExecution(), directDatabase(), breaker, waiter, DatabaseWaitProperties())

    @Test
    fun `429는 같은 호출을 반복하지 않고 runner로 전파한다`() {
        val execution = directExecution()
        val database = directDatabase()
        val breaker: CircuitBreaker = mock { on { acquire() } doReturn Duration.ZERO }
        val api = ExternalApiOperations(execution, database, breaker, RetryWaiter {}, DatabaseWaitProperties())
        val action: () -> String = mock()
        whenever(action()).thenThrow(EmbeddingRateLimitedException("10", RuntimeException("429")))
        assertThatThrownBy { api.execute(testToken, action) }.isInstanceOf(EmbeddingRateLimitedException::class.java)
        verify(action).invoke()
        verify(execution).checkpoint(testToken)
        verify(database).awaitAvailable()
    }

    @Test
    fun `실행권을 잃으면 외부 요청을 시작하지 않는다`() {
        val execution = directExecution()
        doThrow(ExecutionLostException(testToken)).whenever(execution).checkpoint(testToken)
        val breaker: CircuitBreaker = mock { on { acquire() } doReturn Duration.ZERO }
        val action: () -> String = mock()
        assertThatThrownBy { ExternalApiOperations(execution, directDatabase(), breaker, RetryWaiter {}, DatabaseWaitProperties()).execute(testToken, action) }
            .isInstanceOf(ExecutionLostException::class.java)
        verifyNoInteractions(action)
    }

    @Test
    fun `결제 한도 초과는 breaker 실패로 세어 열리면 기다린 뒤 같은 호출을 재시도한다`() {
        val breaker: CircuitBreaker = mock()
        whenever(breaker.acquire()).thenReturn(Duration.ZERO, Duration.ofSeconds(30), Duration.ZERO)
        whenever(breaker.recordTransientFailure()).thenReturn(true)
        var calls = 0
        val result = simpleApi(breaker).execute(testToken) {
            if (++calls == 1) throw EmbeddingQuotaExceededException(RuntimeException("insufficient_quota"))
            "ok"
        }
        assertThat(result).isEqualTo("ok")
        assertThat(calls).describedAs("시도 횟수를 쓰지 않고 같은 호출을 다시 한다").isEqualTo(2)
        verify(breaker).recordTransientFailure()
    }

    @Test
    fun `일시 실패가 breaker를 열면 예외를 삼키고 열려 있는 동안 기다린 뒤 같은 호출을 재시도한다`() {
        val breaker: CircuitBreaker = mock()
        whenever(breaker.acquire()).thenReturn(Duration.ZERO, Duration.ofSeconds(30), Duration.ZERO)
        whenever(breaker.recordTransientFailure()).thenReturn(true)
        var waited = Duration.ZERO
        val waiter = RetryWaiter { waited = waited.plus(it) }
        val api = ExternalApiOperations(directExecution(), directDatabase(), breaker, waiter, DatabaseWaitProperties())
        var calls = 0
        val result = api.execute(testToken) {
            if (++calls == 1) throw RuntimeException("503")
            "ok"
        }
        assertThat(result).isEqualTo("ok")
        assertThat(calls).isEqualTo(2)
        assertThat(waited).describedAs("대기는 확인 간격(5초)으로 끊어서 한다").isEqualTo(Duration.ofSeconds(5))
        verify(breaker).recordSuccess()
    }

    @Test
    fun `breaker가 열리지 않은 일시 실패는 그대로 던져 기존 재시도 경로를 탄다`() {
        val breaker: CircuitBreaker = mock()
        whenever(breaker.acquire()).thenReturn(Duration.ZERO)
        whenever(breaker.recordTransientFailure()).thenReturn(false)
        assertThatThrownBy { simpleApi(breaker).execute(testToken) { throw RuntimeException("503") } }
            .isInstanceOf(RuntimeException::class.java).hasMessage("503")
        verify(breaker, never()).recordSuccess()
    }

    @Test
    fun `영구 오류는 breaker에 기록하지 않고 던진다`() {
        val breaker: CircuitBreaker = mock()
        whenever(breaker.acquire()).thenReturn(Duration.ZERO)
        assertThatThrownBy { simpleApi(breaker).execute(testToken) { throw EmbeddingRequestRejectedException(RuntimeException("400")) } }
            .isInstanceOf(EmbeddingRequestRejectedException::class.java)
        verify(breaker, never()).recordTransientFailure()
    }

    @Test
    fun `호출이 성공하면 breaker에 성공을 기록한다`() {
        val breaker: CircuitBreaker = mock()
        whenever(breaker.acquire()).thenReturn(Duration.ZERO)
        assertThat(simpleApi(breaker).execute(testToken) { 42 }).isEqualTo(42)
        verify(breaker).recordSuccess()
        verify(breaker, never()).recordTransientFailure()
    }
}
