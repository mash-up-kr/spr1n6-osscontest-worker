package com.osscontest.worker.indexing.fault

import com.osscontest.worker.indexing.queue.ExecutionToken
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class IndexingFaultInjectorTest {
    private val context =
        FaultInjectionContext(
            sourceEventId = UUID.randomUUID(),
            executionToken = ExecutionToken(137L, "worker-1", 2),
        )

    @Test
    fun `비활성 상태에서는 block하지 않는다`() {
        val injector = IndexingFaultInjector(FaultInjectionProperties(enabled = false))

        // block되면 테스트가 멈추지 않고 실패하도록 시간을 제한한다.
        assertTimeoutPreemptively(Duration.ofSeconds(1)) { injector.blockIfNeeded(5001L, "EMBEDDING", context) }
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

        // block되면 테스트가 멈추지 않고 실패하도록 시간을 제한한다.
        assertTimeoutPreemptively(Duration.ofSeconds(1)) { injector.blockIfNeeded(5001L, "PARSING", context) }
    }

    @Test
    fun `설정한 phase에서는 thread를 block한다`() {
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
