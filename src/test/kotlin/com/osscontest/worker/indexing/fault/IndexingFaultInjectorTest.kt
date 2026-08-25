package com.osscontest.worker.indexing.fault

import com.osscontest.worker.indexing.consumer.KafkaRecordIdentity
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class IndexingFaultInjectorTest {
    private val context =
        FaultInjectionContext(
            sourceEventId = UUID.randomUUID(),
            workerId = "worker-1",
            recordIdentity = KafkaRecordIdentity("indexing", 2, 137L),
        )

    @Test
    fun `비활성 상태에서는 block하지 않는다`() {
        val injector = IndexingFaultInjector(FaultInjectionProperties(enabled = false))

        injector.blockIfNeeded(5001L, "EMBEDDING", context)
    }

    @Test
    fun `phase가 다르면 block하지 않는다`() {
        val injector =
            IndexingFaultInjector(
                FaultInjectionProperties(
                    enabled = true,
                    phase = "EMBEDDING",
                ),
            )

        injector.blockIfNeeded(5001L, "PARSING", context)
    }

    @Test
    fun `설정한 document version과 phase에서는 thread를 block한다`() {
        val injector =
            IndexingFaultInjector(
                FaultInjectionProperties(
                    enabled = true,
                    phase = "EMBEDDING",
                ),
            )
        val entered = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()

        try {
            val future =
                executor.submit {
                    entered.countDown()
                    injector.blockIfNeeded(5001L, "EMBEDDING", context)
                }

            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue()
            org.junit.jupiter.api.assertThrows<TimeoutException> {
                future.get(100, TimeUnit.MILLISECONDS)
            }
            future.cancel(true)
        } finally {
            executor.shutdownNow()
        }
    }
}
