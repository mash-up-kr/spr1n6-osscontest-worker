package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.sql.SQLException
import java.sql.SQLRecoverableException
import java.sql.SQLTransientConnectionException
import java.util.concurrent.atomic.AtomicReference

/** Call outside the transaction proxy: exceptions must unwind and release DB resources first. */
@Component
class DatabaseOperations(
    jdbcTemplate: JdbcTemplate,
    private val waiter: RetryWaiter,
    private val properties: DatabaseWaitProperties,
    private val meters: MeterRegistry,
) {
    private data class Availability(val generation: Long, val offline: Boolean)
    private val state = AtomicReference(Availability(0, false))
    private val health = JdbcTemplate(checkNotNull(jdbcTemplate.dataSource)).apply {
        queryTimeout = properties.queryTimeout.seconds.toInt()
    }
    private val log = LoggerFactory.getLogger(javaClass)

    fun <T> execute(afterRecovery: () -> Unit = {}, action: () -> T): T {
        check(!TransactionSynchronizationManager.isActualTransactionActive()) {
            "Database wait must run outside a transaction"
        }
        var recovered = false
        while (true) {
            checkInterrupted()
            if (state.get().offline) {
                awaitAvailable()
                recovered = true
            }
            try {
                if (recovered) {
                    afterRecovery()
                    recovered = false
                }
                return action()
            } catch (error: Exception) {
                if (!isUnavailable(error)) throw error
                state.updateAndGet { Availability(it.generation + 1, true) }
                meters.counter("indexing_db_wait_total").increment()
                log.warn("DB unavailable; waiting at the failed operation: {}", error.javaClass.simpleName)
                recovered = true
            }
        }
    }

    fun awaitAvailable() {
        check(!TransactionSynchronizationManager.isActualTransactionActive())
        while (true) {
            checkInterrupted()
            val observed = state.get()
            if (!observed.offline) return
            waiter.waitFor(properties.checkInterval)
            try {
                check(health.queryForObject("SELECT 1", Int::class.java) == 1)
                // A probe started before a newer fault must not clear that fault.
                state.compareAndSet(observed, Availability(observed.generation, false))
            } catch (error: Exception) {
                if (!isUnavailable(error)) throw error
            }
        }
    }

    companion object {
        fun isUnavailable(error: Throwable): Boolean {
            var cause: Throwable? = error
            val seen = mutableSetOf<Throwable>()
            while (cause != null && seen.add(cause)) {
                if (cause is DataAccessResourceFailureException || cause is QueryTimeoutException || cause is SQLRecoverableException ||
                    cause is SQLTransientConnectionException
                ) return true
                if (cause is SQLException) {
                    val sqlState = cause.sqlState.orEmpty()
                    if (sqlState.startsWith("08") || sqlState in setOf("57P01", "57P02", "57P03", "53300")) return true
                }
                cause = cause.cause
            }
            return false
        }

        fun checkInterrupted() {
            if (Thread.currentThread().isInterrupted) throw InterruptedException("Worker stopping")
        }
    }
}
