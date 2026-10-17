/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import com.osscontest.worker.indexing.retrieval.TemporaryDocumentFile
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.InputStream
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ParsingCancellationTest {
    private class Fixture : AutoCloseable {
        val meters = SimpleMeterRegistry()
        val guard = ParsingTimeoutGuard(Duration.ofSeconds(10), 1, meters)
        val file = file()
        override fun close() { guard.shutdown(); file.close() }
    }
    private fun parser(action: (InputStream) -> Sequence<ParsedBlock>) = object : DocumentParser {
        override val supportedMimeTypes = setOf("text/plain")
        override fun parse(input: InputStream) = action(input)
    }
    private fun await(latch: CountDownLatch) { check(latch.await(3, TimeUnit.SECONDS)) }
    private fun until(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (!condition()) { check(System.nanoTime() < deadline); Thread.sleep(1) }
    }

    @Test fun `호출자 interrupt는 parser 취소와 interrupt 보존으로 전파한다`() {
        Fixture().use { f ->
            val entered = CountDownLatch(1); val stopped = CountDownLatch(1)
            val interrupted = AtomicBoolean()
            val caller = Thread {
                try {
                    f.guard.parse(parser { sequence {
                        entered.countDown()
                        try { CountDownLatch(1).await() } finally { stopped.countDown() }
                    } }, f.file, "text/plain")
                } catch (_: InterruptedException) { interrupted.set(Thread.currentThread().isInterrupted) }
            }
            caller.start()
            try {
                await(entered); caller.interrupt(); caller.join(3000); await(stopped)
                assertThat(caller.isAlive).isFalse()
                assertThat(interrupted.get()).isTrue()
                assertThat(f.meters.find("parse_timeout_total").counter()).isNull()
            } finally { caller.interrupt(); caller.join(3000) }
        }
    }

    @Test fun `취소를 무시하는 parser는 실제 종료 전 파일을 지우지 않는다`() {
        Fixture().use { f ->
            val entered = CountDownLatch(1); val cancelled = CountDownLatch(1); val finish = CountDownLatch(1)
            val caller = Thread {
                try { f.guard.parse(parser { sequence {
                    entered.countDown()
                    while (finish.count > 0) {
                        try { finish.await() } catch (_: InterruptedException) { cancelled.countDown() }
                    }
                } }, f.file, "text/plain") } catch (_: InterruptedException) { }
            }
            caller.start()
            try {
                await(entered); caller.interrupt(); caller.join(3000); await(cancelled)
                f.file.close()
                assertThat(caller.isAlive).isFalse()
                assertThat(Files.exists(f.file.path)).isTrue()
                finish.countDown(); until { !Files.exists(f.file.path) }
            } finally { finish.countDown(); caller.interrupt(); caller.join(3000) }
        }
    }

    @Test fun `대기 작업 취소는 parser를 실행하지 않고 사용권과 큐를 반환한다`() {
        Fixture().use { f ->
            val entered = CountDownLatch(1); val finish = CountDownLatch(1)
            val first = Thread { try { f.guard.parse(parser { sequence {
                entered.countDown(); finish.await()
            } }, f.file, "text/plain") } catch (_: InterruptedException) { } }
            val queued = file(); val invoked = AtomicBoolean()
            val second = Thread { try { f.guard.parse(parser { invoked.set(true); emptySequence() }, queued, "text/plain") }
                catch (_: InterruptedException) { } }
            first.start()
            try {
                await(entered); second.start(); until { second.state == Thread.State.TIMED_WAITING }
                second.interrupt(); second.join(3000); queued.close()
                assertThat(second.isAlive).isFalse()
                assertThat(Files.exists(queued.path)).isFalse()
                assertThat(invoked.get()).isFalse()
                finish.countDown(); first.join(3000)
            } finally { finish.countDown(); second.interrupt(); first.interrupt(); queued.close(); first.join(3000); second.join(3000) }
        }
    }

    @Test fun `포화 거절과 shutdown은 미시작 작업의 파일 사용권을 반환한다`() {
        Fixture().use { f ->
            val entered = CountDownLatch(1); val finish = CountDownLatch(1)
            val first = Thread { try { f.guard.parse(parser { sequence {
                entered.countDown(); finish.await()
            } }, f.file, "text/plain") } catch (_: InterruptedException) { } }
            val queued = file(); val rejected = file(); val invoked = AtomicBoolean()
            val second = Thread { try { f.guard.parse(parser { invoked.set(true); emptySequence() }, queued, "text/plain") }
                catch (_: InterruptedException) { } }
            first.start()
            try {
                await(entered); second.start(); until { second.state == Thread.State.TIMED_WAITING }
                assertThrows<ParseCapacityException> { f.guard.parse(parser { emptySequence() }, rejected, "text/plain") }
                rejected.close(); assertThat(Files.exists(rejected.path)).isFalse()
                f.guard.shutdown(); second.join(3000); first.join(3000)
                queued.close(); assertThat(Files.exists(queued.path)).isFalse()
                assertThat(second.isAlive).isFalse()
                assertThat(first.isAlive).isFalse()
                assertThat(invoked.get()).isFalse()
                assertThrows<InterruptedException> { f.guard.parse(parser { emptySequence() }, f.file, "text/plain") }
            } finally {
                finish.countDown(); first.interrupt(); second.interrupt(); queued.close(); rejected.close()
                first.join(3000); second.join(3000)
            }
        }
    }

    @Test fun `파일 사용권은 여러 번 닫아도 마지막 사용자 이후 한 번만 정리한다`() {
        val file = file(); val use = file.retain()
        file.close(); file.close()
        assertThat(Files.exists(file.path)).isTrue()
        assertThrows<IllegalStateException> { file.retain() }
        use.close(); use.close()
        assertThat(Files.exists(file.path)).isFalse()
    }

    companion object {
        private fun file() = TemporaryDocumentFile(Files.createTempFile("parse-test-", ".tmp").also { Files.writeString(it, "x") })
    }
}
