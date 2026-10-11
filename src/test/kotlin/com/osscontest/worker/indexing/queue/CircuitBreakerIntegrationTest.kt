package com.osscontest.worker.indexing.queue

import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Tag("integration")
@SpringBootTest(properties = ["indexing.queue.enabled=false"])
@ActiveProfiles("integration")
class CircuitBreakerIntegrationTest(
    private val store: CircuitBreakerStore,
    private val jdbc: JdbcTemplate,
) {
    private val name = "embedding-api"
    private val openMillis = 60_000L

    @BeforeEach
    fun reset() {
        assertDedicatedIntegrationDatabase(jdbc)
        val updated = jdbc.update(
            "UPDATE external_circuit_breaker SET state = 'CLOSED', consecutive_failures = 0, opened_until = NULL WHERE name = ?", name)
        assertThat(updated).describedAs("embedding-api 행이 있어야 한다").isEqualTo(1)
    }

    private fun failures(count: Int) = (1..count).map { store.recordFailure(name, 5, openMillis) }

    @Test
    fun `임계치 직전까지는 CLOSED로 남고 임계치에 도달하면 OPEN이 된다`() {
        assertThat(failures(4)).containsOnly(BreakerState.CLOSED)
        assertThat(store.recordFailure(name, 5, openMillis)).isEqualTo(BreakerState.OPEN)
        val snapshot = checkNotNull(store.snapshot(name))
        assertThat(snapshot.state).isEqualTo(BreakerState.OPEN)
        assertThat(snapshot.openedUntil).isAfter(snapshot.now)
    }

    @Test
    fun `성공은 연속 실패 카운트를 리셋한다`() {
        failures(3)
        assertThat(store.recordSuccess(name)).isEqualTo(1)
        assertThat(failures(4)).containsOnly(BreakerState.CLOSED)
        assertThat(store.recordSuccess(name)).isEqualTo(1)
        assertThat(store.recordSuccess(name)).describedAs("바꿀 것이 없으면 쓰지 않는다").isZero()
    }

    @Test
    fun `OPEN 유지 시간이 지나기 전에는 시험 호출권을 주지 않는다`() {
        failures(5)
        assertThat(store.tryStartProbe(name, openMillis)).isFalse()
    }

    @Test
    fun `OPEN 유지 시간이 지나면 동시에 요청해도 시험 호출권은 한 곳에만 간다`() {
        failures(5)
        jdbc.update("UPDATE external_circuit_breaker SET opened_until = clock_timestamp() - INTERVAL '1 second' WHERE name = ?", name)
        val threads = 8
        val pool = Executors.newFixedThreadPool(threads)
        val ready = CountDownLatch(threads)
        val go = CountDownLatch(1)
        val wins = AtomicInteger()
        try {
            repeat(threads) {
                pool.execute {
                    ready.countDown()
                    go.await()
                    if (store.tryStartProbe(name, openMillis)) wins.incrementAndGet()
                }
            }
            check(ready.await(5, TimeUnit.SECONDS))
            go.countDown()
        } finally {
            pool.shutdown()
            check(pool.awaitTermination(10, TimeUnit.SECONDS))
        }
        assertThat(wins.get()).isEqualTo(1)
        assertThat(checkNotNull(store.snapshot(name)).state).isEqualTo(BreakerState.HALF_OPEN)
    }

    @Test
    fun `시험 호출이 실패하면 즉시 OPEN으로 돌아간다`() {
        failures(5)
        jdbc.update("UPDATE external_circuit_breaker SET opened_until = clock_timestamp() - INTERVAL '1 second' WHERE name = ?", name)
        assertThat(store.tryStartProbe(name, openMillis)).isTrue()
        assertThat(store.recordFailure(name, 5, openMillis)).isEqualTo(BreakerState.OPEN)
        val snapshot = checkNotNull(store.snapshot(name))
        assertThat(snapshot.openedUntil).isAfter(snapshot.now)
    }

    @Test
    fun `시험 호출이 성공하면 CLOSED로 돌아가고 카운트가 0이 된다`() {
        failures(5)
        jdbc.update("UPDATE external_circuit_breaker SET opened_until = clock_timestamp() - INTERVAL '1 second' WHERE name = ?", name)
        assertThat(store.tryStartProbe(name, openMillis)).isTrue()
        assertThat(store.recordSuccess(name)).isEqualTo(1)
        val row = jdbc.queryForMap("SELECT state, consecutive_failures, opened_until FROM external_circuit_breaker WHERE name = ?", name)
        assertThat(row["state"]).isEqualTo("CLOSED")
        assertThat(row["consecutive_failures"]).isEqualTo(0)
        assertThat(row["opened_until"]).isNull()
    }

    @Test
    fun `행이 없으면 조회와 기록 모두 null이거나 0건이다`() {
        assertThat(store.snapshot("missing")).isNull()
        assertThat(store.recordFailure("missing", 5, openMillis)).isNull()
        assertThat(store.recordSuccess("missing")).isZero()
        assertThat(store.tryStartProbe("missing", openMillis)).isFalse()
    }
}
