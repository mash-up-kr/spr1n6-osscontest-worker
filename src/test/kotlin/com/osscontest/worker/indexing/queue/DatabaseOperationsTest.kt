package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

class DatabaseOperationsTest {
    private val source = DriverManagerDataSource("jdbc:h2:mem:wait-test;DB_CLOSE_DELAY=-1", "sa", "")

    @Test
    fun `실패한 트랜잭션이 롤백되고 끝난 뒤 현재 DB 작업만 재호출한다`() {
        val jdbc = JdbcTemplate(source)
        jdbc.execute("CREATE TABLE IF NOT EXISTS saved_result (result_value INT)")
        jdbc.update("DELETE FROM saved_result")
        val transaction = TransactionTemplate(DataSourceTransactionManager(source))
        var waits = 0
        var recovered = 0
        val database = DatabaseOperations(jdbc, RetryWaiter {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse()
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM saved_result", Int::class.java)).isZero()
            waits++
        }, properties(), SimpleMeterRegistry())
        var attempts = 0
        val retained = 42
        database.execute(afterRecovery = { recovered++ }) {
            transaction.execute {
                jdbc.update("INSERT INTO saved_result VALUES (?)", retained)
                if (++attempts == 1) throw SQLException("Connection lost", "08006")
            }
        }
        assertThat(attempts).isEqualTo(2)
        assertThat(waits).isEqualTo(1)
        assertThat(recovered).isEqualTo(1)
        assertThat(jdbc.queryForObject("SELECT result_value FROM saved_result", Int::class.java)).isEqualTo(retained)
    }

    @Test
    fun `SQL 제약 위반은 DB 장애 무한 대기로 바꾸지 않는다`() {
        val waits = AtomicInteger()
        val database = DatabaseOperations(JdbcTemplate(source), RetryWaiter { waits.incrementAndGet() }, properties(), SimpleMeterRegistry())
        assertThatThrownBy { database.execute { throw SQLException("constraint", "23505") } }.isInstanceOf(SQLException::class.java)
        assertThat(waits.get()).isZero()
    }

    @Test
    fun `트랜잭션 내부에서 장애 대기를 시작할 수 없다`() {
        val database = DatabaseOperations(JdbcTemplate(source), RetryWaiter {}, properties(), SimpleMeterRegistry())
        val transaction = TransactionTemplate(DataSourceTransactionManager(source))
        assertThatThrownBy { transaction.execute { database.execute { 1 } } }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `오래된 헬스체크 성공은 새 장애 상태를 해제하지 않는다`() {
        val oldProbe = CountDownLatch(1)
        val releaseOld = CountDownLatch(1)
        val newProbe = CountDownLatch(1)
        val releaseNew = CountDownLatch(1)
        val probes = AtomicInteger()
        val controlled = object : DriverManagerDataSource("jdbc:h2:mem:probe-test", "sa", "") {
            override fun getConnection(): java.sql.Connection {
                if (probes.incrementAndGet() == 1) {
                    oldProbe.countDown()
                    check(releaseOld.await(3, TimeUnit.SECONDS))
                } else {
                    newProbe.countDown()
                    check(releaseNew.await(3, TimeUnit.SECONDS))
                }
                return super.getConnection()
            }
        }
        val database = DatabaseOperations(JdbcTemplate(controlled), RetryWaiter {}, properties(), SimpleMeterRegistry())
        val pool = Executors.newFixedThreadPool(2)
        try {
            fun operation(): Int {
                var count = 0
                return database.execute { if (++count == 1) throw SQLException("offline", "08006"); count }
            }
            val inFlight = CountDownLatch(1)
            val releaseFault = CountDownLatch(1)
            val second = pool.submit<Int> {
                var count = 0
                database.execute {
                    if (++count == 1) {
                        inFlight.countDown()
                        check(releaseFault.await(3, TimeUnit.SECONDS))
                        throw SQLException("new fault", "08006")
                    }
                    count
                }
            }
            check(inFlight.await(3, TimeUnit.SECONDS))
            val first = pool.submit<Int> { operation() }
            check(oldProbe.await(3, TimeUnit.SECONDS))
            releaseFault.countDown()
            check(newProbe.await(3, TimeUnit.SECONDS))
            releaseOld.countDown()
            assertThatThrownBy { first.get(30, TimeUnit.MILLISECONDS) }.isInstanceOf(TimeoutException::class.java)
            releaseNew.countDown()
            assertThat(first.get(3, TimeUnit.SECONDS)).isEqualTo(2)
            assertThat(second.get(3, TimeUnit.SECONDS)).isEqualTo(2)
        } finally {
            releaseOld.countDown()
            releaseNew.countDown()
            pool.shutdownNow()
        }
    }

    private fun properties() = DatabaseWaitProperties(Duration.ofMillis(1), Duration.ofSeconds(1))
}
