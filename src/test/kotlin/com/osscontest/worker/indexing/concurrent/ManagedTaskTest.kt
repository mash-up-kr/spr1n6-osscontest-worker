package com.osscontest.worker.indexing.concurrent

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ManagedTaskTest {
    @Test fun `미시작 취소는 정리하고 실행 취소 경합에서도 한 번만 정리한다`() {
        val cleaned = AtomicInteger()
        val pending = ManagedTask({ error("Cancelled work ran") }, { cleaned.incrementAndGet() })
        pending.cancel(true); pending.cancel(true); pending.run()
        assertThat(cleaned.get()).isEqualTo(1)
        val pool = Executors.newFixedThreadPool(2)
        try {
            repeat(100) {
                val start = CountDownLatch(1); val count = AtomicInteger()
                val task = ManagedTask({}, { count.incrementAndGet() })
                val run = pool.submit { start.await(); task.run() }
                val cancel = pool.submit { start.await(); task.cancel(true) }
                start.countDown(); run.get(3, TimeUnit.SECONDS); cancel.get(3, TimeUnit.SECONDS)
                assertThat(count.get()).isEqualTo(1)
            }
        } finally { pool.shutdownNow() }
    }

    @Test fun `running future 취소 완료는 실제 종료 전 자원을 반환하지 않는다`() {
        val entered = CountDownLatch(1); val finish = CountDownLatch(1); val exited = CountDownLatch(1)
        val cleaned = AtomicInteger()
        val task = ManagedTask({
            entered.countDown()
            while (finish.count > 0) {
                try { finish.await() } catch (_: InterruptedException) { }
            }
        }, { cleaned.incrementAndGet(); exited.countDown() })
        val thread = Thread(task)
        thread.start()
        try {
            check(entered.await(3, TimeUnit.SECONDS)); task.cancel(true)
            assertThat(task.isDone).isTrue()
            assertThat(cleaned.get()).isZero()
            finish.countDown(); check(exited.await(3, TimeUnit.SECONDS))
            assertThat(cleaned.get()).isEqualTo(1)
        } finally { finish.countDown(); thread.join(3000) }
    }
}
