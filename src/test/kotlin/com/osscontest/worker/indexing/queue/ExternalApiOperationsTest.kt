package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.embedding.service.*
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import com.osscontest.worker.support.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.*
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ExternalApiOperationsTest {
    private val permit = CircuitPermit("embedding-api", 1, "call")
    private val publication: IndexingPublicationService = mock()
    private fun api(breaker: CircuitBreaker = closedCircuitBreaker(), execution: ExecutionOperations = directExecution(),
                    database: DatabaseOperations = directDatabase(), waiter: RetryWaiter = RetryWaiter {}) =
        ExternalApiOperations(execution, database, breaker, waiter, DatabaseWaitProperties(), publication)

    @Test
    fun `claim 뒤 OPEN은 호출 실패를 만들지 않고 대기 후 실제 LLM 호출 한 번만 실행한다`() {
        val execution = directExecution()
        val breaker = closedCircuitBreaker()
        whenever(breaker.acquire(any(), any())).thenReturn(CallAdmission.Blocked(Duration.ofSeconds(30)),
            CallAdmission.Blocked(Duration.ofSeconds(20)), CallAdmission.Allowed(permit))
        val action: () -> String = mock { on { invoke() } doReturn "ok" }
        val waits = mutableListOf<Duration>()
        assertThat(api(breaker, execution, waiter = RetryWaiter { waits.add(it) }).execute(testToken, action = action)).isEqualTo("ok")
        assertThat(waits).containsExactly(Duration.ofSeconds(5), Duration.ofSeconds(5))
        verify(execution, times(4)).checkpoint(testToken)
        verify(action).invoke()
        val results = argumentCaptor<CircuitResult>()
        verify(breaker).record(eq(permit), results.capture())
        assertThat(results.firstValue.outcome).isEqualTo(CircuitOutcome.SUCCESS)
    }

    @Test
    fun `OPEN 대기 중 실행권 상실은 호출이나 서킷 성공 실패 기록 없이 종료한다`() {
        val execution = directExecution()
        doNothing().doThrow(ExecutionLostException(testToken)).whenever(execution).checkpoint(testToken)
        val breaker = closedCircuitBreaker()
        whenever(breaker.acquire(any(), any())).thenReturn(CallAdmission.Blocked(Duration.ofSeconds(30)))
        val action: () -> String = mock()
        assertThatThrownBy { api(breaker, execution).execute(testToken, action = action) }.isInstanceOf(ExecutionLostException::class.java)
        verifyNoInteractions(action)
        verify(breaker, never()).record(any(), any())
    }

    @Test
    fun `OPEN 대기 중 삭제를 주기적으로 확인하고 skip 완료하면 외부 호출 없이 종료한다`() {
        val breaker = closedCircuitBreaker()
        whenever(breaker.acquire(any(), any())).thenReturn(CallAdmission.Blocked(Duration.ofSeconds(30)))
        whenever(publication.preflight(testToken)).thenReturn(false, true)
        val action: () -> String = mock()
        assertThatThrownBy { api(breaker).execute(testToken, action = action) }.isInstanceOf(ExecutionCompletedException::class.java)
        verify(publication, times(2)).preflight(testToken)
        verifyNoInteractions(action)
        verify(breaker, never()).record(any(), any())
        verify(breaker).release(eq(testToken), any())
    }

    @Test
    fun `실행권을 잃으면 외부 요청을 시작하지 않는다`() {
        val execution = directExecution()
        doThrow(ExecutionLostException(testToken)).whenever(execution).checkpoint(testToken)
        val action: () -> String = mock()
        assertThatThrownBy { api(execution = execution).execute(testToken, action = action) }.isInstanceOf(ExecutionLostException::class.java)
        verifyNoInteractions(action)
    }

    @ParameterizedTest
    @ValueSource(strings = ["429", "quota", "configuration", "503", "permanent"])
    fun `실제 오류는 서킷 결과를 기록한 뒤 원오류를 runner로 전파하고 호출을 반복하지 않는다`(kind: String) {
        val breaker = closedCircuitBreaker()
        val error = when (kind) {
            "429" -> EmbeddingRateLimitedException("10", RuntimeException("429"))
            "quota" -> EmbeddingQuotaExceededException(RuntimeException("insufficient_quota"))
            "configuration" -> EmbeddingConfigurationException("AUTHENTICATION", RuntimeException("401"))
            "permanent" -> EmbeddingRequestRejectedException(RuntimeException("400"))
            else -> RuntimeException("503")
        }
        val action: () -> String = mock { on { invoke() } doThrow error }
        assertThatThrownBy { api(breaker).execute(testToken, attempt = 5, action = action) }.isSameAs(error)
        verify(action).invoke()
        val results = argumentCaptor<CircuitResult>()
        verify(breaker).record(any(), results.capture())
        assertThat(results.firstValue.outcome).isEqualTo(when (kind) {
            "429" -> CircuitOutcome.RATE_LIMIT
            "quota", "configuration" -> CircuitOutcome.CONFIGURATION
            "permanent" -> CircuitOutcome.PERMANENT
            else -> CircuitOutcome.FAILURE
        })
        assertThat(results.firstValue.attempt).isEqualTo(5)
    }

    @Test
    fun `허용권 확인 전 실행권을 잃으면 외부 요청과 성공 집계 없이 종료한다`() {
        val execution = directExecution()
        doNothing().doThrow(ExecutionLostException(testToken)).whenever(execution).checkpoint(testToken)
        val action: () -> String = mock()
        val breaker = closedCircuitBreaker()
        assertThatThrownBy { api(breaker, execution).execute(testToken, action = action) }.isInstanceOf(ExecutionLostException::class.java)
        verifyNoInteractions(action)
        verify(breaker, never()).record(any(), any())
    }

    @Test
    fun `호출 직전 세대가 바뀐 허용권은 사용하지 않고 다시 대기한다`() {
        val breaker = closedCircuitBreaker()
        whenever(breaker.valid(any())).thenReturn(false, true)
        val action: () -> String = mock { on { invoke() } doReturn "ok" }
        assertThat(api(breaker).execute(testToken, action = action)).isEqualTo("ok")
        verify(action).invoke()
        verify(breaker, times(2)).acquire(eq(testToken), any())
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `호출 전 취소와 실제 호출 중단은 LLM 성공 실패로 집계하지 않는다`(beforeCall: Boolean) {
        val breaker = closedCircuitBreaker()
        val execution = directExecution()
        if (beforeCall) doAnswer { throw InterruptedException("cancelled before call") }.whenever(execution).checkpoint(testToken)
        var calls = 0
        val action: () -> String = { calls++; throw InterruptedException("cancelled") }
        assertThatThrownBy { api(breaker, execution).execute(testToken, action = action) }.isInstanceOf(InterruptedException::class.java)
        assertThat(calls).isEqualTo(if (beforeCall) 0 else 1)
        verify(breaker, never()).record(any(), any())
        verify(breaker).release(eq(testToken), any())
    }

    private fun database(waiter: RetryWaiter) = DatabaseOperations(
        JdbcTemplate(DriverManagerDataSource("jdbc:h2:mem:circuit-recovery;DB_CLOSE_DELAY=-1", "sa", "")),
        waiter, DatabaseWaitProperties(), SimpleMeterRegistry())

    @Test
    fun `다른 경로가 DB 장애를 감지하면 외부 호출을 시작하지 않고 복구 뒤 한 번 호출한다`() {
        val waits = AtomicInteger()
        val firstWait = CountDownLatch(1)
        val release = CountDownLatch(1)
        val database = database(RetryWaiter {
            waits.incrementAndGet(); firstWait.countDown()
            check(release.await(5, TimeUnit.SECONDS))
        })
        val breaker = closedCircuitBreaker()
        val action: () -> String = mock { on { invoke() } doReturn "ok" }
        val pool = Executors.newFixedThreadPool(2)
        try {
            // heartbeat 경로의 연결 장애 감지
            val heartbeat = pool.submit<Int> {
                var calls = 0
                database.execute { if (++calls == 1) throw SQLException("heartbeat connection lost", "08006"); calls }
            }
            check(firstWait.await(5, TimeUnit.SECONDS))
            val call = pool.submit<String> { api(breaker, database = database).execute(testToken, action = action) }
            val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
            while (waits.get() < 2) { check(System.nanoTime() < until) { "external call did not wait" }; Thread.sleep(1) }
            verifyNoInteractions(action)
            verify(breaker, never()).acquire(any(), any())
            // 복구 확인 뒤 호출
            release.countDown()
            assertThat(call.get(5, TimeUnit.SECONDS)).isEqualTo("ok")
            assertThat(heartbeat.get(5, TimeUnit.SECONDS)).isEqualTo(2)
            verify(action).invoke()
        } finally {
            release.countDown()
            pool.shutdownNow()
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["expired", "RETRY_WAIT", "FAILED", "epoch", "worker"])
    fun `획득 복구 후 미회수 만료만 실제 실행권 경계가 갱신하고 다른 실행은 호출하지 않는다`(condition: String) {
        val store: JobQueueStore = mock()
        var snapshot = JobSnapshot(testToken.workerId, testToken.epoch,
            com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.PROCESSING, testNow.plusSeconds(60),
            null, testNow, testToken.epoch)
        whenever(store.snapshot(testToken)).thenAnswer { snapshot }
        whenever(store.renew(testToken)).thenAnswer { snapshot = snapshot.copy(leaseUntil = testNow.plusSeconds(60)); 1 }
        val database = database(RetryWaiter {
            snapshot = when (condition) {
                "expired" -> snapshot.copy(leaseUntil = testNow.minusSeconds(1))
                "epoch" -> snapshot.copy(documentEpoch = testToken.epoch + 1)
                "worker" -> snapshot.copy(workerId = "new-worker")
                else -> snapshot.copy(status = com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.valueOf(condition),
                    workerId = null, leaseUntil = null)
            }
        })
        val execution = ExecutionOperations(database, store)
        val breaker = closedCircuitBreaker()
        whenever(breaker.acquire(any(), any())).thenThrow(DataAccessResourceFailureException("probe acquisition down"))
            .thenReturn(CallAdmission.Allowed(permit))
        val action: () -> String = mock { on { invoke() } doReturn "ok" }
        val actual = api(breaker, execution, database)
        if (condition == "expired") {
            assertThat(actual.execute(testToken, action = action)).isEqualTo("ok")
            verify(store).renew(testToken)
            verify(action).invoke()
            assertThat(snapshot.epoch).isEqualTo(testToken.epoch)
        } else {
            assertThatThrownBy { actual.execute(testToken, action = action) }.isInstanceOf(ExecutionLostException::class.java)
            verifyNoInteractions(action)
            verify(store, never()).renew(any())
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `서킷 획득 DB 복구는 호출 전에 실행권을 재검증하고 회수면 차단한다`(lost: Boolean) {
        val execution = directExecution()
        val breaker = closedCircuitBreaker()
        var recovered = false
        doAnswer { if (recovered && lost) throw ExecutionLostException(testToken); null }.whenever(execution).checkpoint(testToken)
        whenever(breaker.acquire(any(), any())).thenThrow(DataAccessResourceFailureException("DB down"))
            .thenReturn(CallAdmission.Allowed(permit))
        val action: () -> String = mock { on { invoke() } doReturn "ok" }
        val actual = api(breaker, execution, database(RetryWaiter { recovered = true }))
        if (lost) {
            assertThatThrownBy { actual.execute(testToken, action = action) }.isInstanceOf(ExecutionLostException::class.java)
            verifyNoInteractions(action)
            verify(breaker, never()).record(any(), any())
        } else {
            assertThat(actual.execute(testToken, action = action)).isEqualTo("ok")
            verify(action).invoke()
            inOrder(execution, breaker, action).apply {
                verify(execution).checkpoint(testToken)
                verify(breaker).acquire(eq(testToken), any())
                verify(execution).checkpoint(testToken)
                verify(breaker).acquire(eq(testToken), any())
                verify(execution).checkpoint(testToken)
                verify(breaker).valid(permit)
                verify(action).invoke()
            }
        }
        assertThat(recovered).isTrue()
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `서킷 기록 DB 장애는 받은 성공 결과 또는 오류를 보존하고 API를 반복하지 않는다`(failure: Boolean) {
        val breaker = closedCircuitBreaker()
        var records = 0
        val retained = mutableListOf<CircuitResult>()
        doAnswer {
            retained.add(it.getArgument(1))
            if (++records == 1) throw DataAccessResourceFailureException("ack lost")
            null
        }.whenever(breaker).record(any(), any())
        val error = RuntimeException("503")
        val action: () -> String = mock()
        if (failure) whenever(action()).thenThrow(error) else whenever(action()).thenReturn("retained")
        val actual = api(breaker, database = database(RetryWaiter {}))
        if (failure) assertThatThrownBy { actual.execute(testToken, action = action) }.isSameAs(error)
        else assertThat(actual.execute(testToken, action = action)).isEqualTo("retained")
        verify(action).invoke()
        assertThat(retained).hasSize(2)
        assertThat(retained[0]).isSameAs(retained[1])
    }
}
