package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import com.osscontest.worker.support.directDatabase
import com.osscontest.worker.support.testNow
import com.osscontest.worker.support.testToken
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.*
import org.slf4j.MDC
import org.springframework.dao.DataAccessResourceFailureException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class DbJobWorkerTest {
    private class Harness(
        concurrency: Int = 1,
        pollInterval: Duration = Duration.ofDays(1),
        heartbeatInterval: Duration = Duration.ofSeconds(10),
        workerIdleTimeout: Duration = Duration.ofSeconds(60),
        database: DatabaseOperations = directDatabase(),
        waiter: QueueLoopWaiter = ThreadSleepQueueLoopWaiter(),
        val store: JobQueueStore = mock(),
    ) : AutoCloseable {
        val runner: IndexingPipelineRunner = mock()
        val results = ExecutionResults()
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = concurrency,
            pollInterval = pollInterval, heartbeatInterval = heartbeatInterval, workerIdleTimeout = workerIdleTimeout),
            database, store, runner, waiter, 5, results)
        override fun close() { worker.stop() }
    }

    @Test
    fun `OPEN에서도 공통 claim이 반환한 DELETE와 삭제 INDEX를 실행하고 LLM 성공을 기록하지 않는다`() {
        val breaker = com.osscontest.worker.support.closedCircuitBreaker()
        whenever(breaker.admission()).thenReturn(ClaimAdmission.BLOCKED)
        val claims: JobClaimTransactions = mock()
        val delete = job().copy(type = JobType.DELETE, documentVersionId = null)
        val deletedIndex = job(2)
        whenever(claims.claim(any(), any(), any(), any(), eq(false), any()))
            .thenReturn(ClaimAttempt.Acquired(delete), ClaimAttempt.Acquired(deletedIndex), ClaimAttempt.Empty)
        val store = JobQueueStore(mock(), QueueProperties(), claims, mock(), mock(), breaker)
        Harness(store = store).use { h ->
            val processed = CountDownLatch(2)
            val finish = CountDownLatch(1)
            doAnswer {
                processed.countDown()
                if (it.getArgument<ClaimedJob>(0) == deletedIndex) finish.await()
                null
            }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(processed)
                verify(h.runner).run(delete)
                verify(h.runner).run(deletedIndex)
                verify(claims, times(2)).claim(any(), any(), any(), any(), eq(false), any())
                verify(claims, never()).claim(any(), any(), any(), any(), eq(true), any())
                verify(breaker, never()).acquire(any(), any())
                verify(breaker, never()).record(any(), any())
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `병렬 루프의 claim 소유 ID는 같은 boot 인스턴스와 중복 없는 숫자를 사용한다`() {
        Harness(concurrency = 3).use { h ->
            val completed = CountDownLatch(20)
            val owners = java.util.concurrent.ConcurrentLinkedQueue<String>()
            val count = AtomicInteger()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                val owner = it.getArgument<String>(1)
                owners.add(owner)
                val number = count.incrementAndGet()
                if (number > 20) null else job(number.toLong()).copy(token = ExecutionToken(number.toLong(), owner, 1))
            }
            doAnswer { completed.countDown(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(completed)
                val ids = owners.toList()
                assertThat(ids.map { it.substringBeforeLast('-') }.toSet()).hasSize(1)
                assertThat(ids.map { it.substringAfterLast('-').toLong() }.toSet()).hasSize(ids.size)
                assertThat(ids.all { it.startsWith("worker.") }).isTrue()
            } finally { h.worker.stop() }
        }
    }

    @Test
    fun `stop start는 동일 인스턴스 claim 번호를 초기화하지 않는다`() {
        Harness().use { h ->
            val owners = java.util.concurrent.ConcurrentLinkedQueue<String>()
            val first = CountDownLatch(1)
            val second = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                owners.add(it.getArgument(1))
                if (owners.size == 1) first.countDown() else second.countDown()
                null
            }
            h.worker.start(); await(first); h.worker.stop()
            h.worker.start(); wake(h.worker, second); await(second)
            val ids = owners.toList()
            assertThat(ids.map { it.substringBeforeLast('-') }.toSet()).hasSize(1)
            assertThat(ids.map { it.substringAfterLast('-').toLong() }).isSorted().doesNotHaveDuplicates()
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = [false, true])
    fun `외부 해제는 실패 계획 유무와 무관하게 같은 epoch 실행을 중단한다`(planned: Boolean) {
        Harness().use { h ->
            val started = CountDownLatch(1)
            val interrupted = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), null)
            whenever(h.store.snapshot(testToken)).thenReturn(JobSnapshot(null, 1, IndexingJobStatus.RETRY_WAIT,
                null, testNow.plusSeconds(30), testNow, 1))
            if (planned) h.results.prepare(testToken) { FailureRecord(
                com.osscontest.worker.indexing.pipeline.service.FailureDecision(IndexingJobStatus.RETRY_WAIT,
                    testNow.plusSeconds(30)), "TEMP", "message") }
            doAnswer {
                started.countDown()
                try { CountDownLatch(1).await() } catch (_: InterruptedException) { interrupted.countDown() }
                null
            }.whenever(h.runner).run(job())
            h.worker.start(); await(started); h.worker.heartbeat(); await(interrupted)
        }
    }

    @Test
    fun `claim 성공에 따라 N개까지 확장하고 가득 차면 추가 claim하지 않는다`() {
        Harness(concurrency = 3).use { h ->
            val started = CountDownLatch(3)
            val finish = CountDownLatch(1)
            val count = AtomicInteger()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                assertThat(Thread.currentThread().name).startsWith("job-worker-")
                job(count.incrementAndGet().toLong())
            }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            h.worker.start()
            try {
                await(started)
                assertThat(h.worker.probe()).isFalse()
                assertThat(count.get()).isEqualTo(3)
                verify(h.store, times(3)).claim(eq(5), any(), any())
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `빈 큐는 하나만 탐색하고 다음 탐색에서 idle 스레드를 재사용한다`() {
        Harness(concurrency = 5).use { h ->
            val empty = CountDownLatch(1)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val firstThread = AtomicReference<Thread>()
            val count = AtomicInteger()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                when (count.incrementAndGet()) {
                    1 -> { firstThread.set(Thread.currentThread()); empty.countDown(); null }
                    2 -> job()
                    else -> null
                }
            }
            doAnswer {
                assertThat(Thread.currentThread()).isSameAs(firstThread.get())
                started.countDown(); finish.await(); null
            }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(empty)
                verify(h.store, times(1)).claim(any(), any(), any())
                val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
                while (firstThread.get().state != Thread.State.TIMED_WAITING) {
                    check(System.nanoTime() < until) { "Thread did not return to the idle pool" }
                    Thread.sleep(1)
                }
                wake(h.worker, started)
                await(started)
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `idle timeout으로 스레드가 해제돼도 다음 탐색은 새 스레드에서 실행한다`() {
        Harness(workerIdleTimeout = Duration.ofMillis(20)).use { h ->
            val empty = CountDownLatch(1)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val firstThread = AtomicReference<Thread>()
            val count = AtomicInteger()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                if (count.incrementAndGet() == 1) {
                    firstThread.set(Thread.currentThread()); empty.countDown(); null
                } else job()
            }
            doAnswer {
                assertThat(Thread.currentThread()).isNotSameAs(firstThread.get())
                started.countDown(); finish.await(); null
            }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(empty)
                firstThread.get().join(3000)
                assertThat(firstThread.get().isAlive).isFalse()
                wake(h.worker, started); await(started)
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `탐색 타이머가 idle 풀에서 새 Job을 발견한다`() {
        Harness(pollInterval = Duration.ofMillis(20)).use { h ->
            val empty = CountDownLatch(1)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val pending = AtomicReference<ClaimedJob?>()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                pending.getAndSet(null).also { if (it == null) empty.countDown() }
            }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try { await(empty); pending.set(job()); await(started) }
            finally { h.worker.stop(); finish.countDown() }
        }
    }

    enum class Exit { NORMAL, LOST, COMPLETED, UNEXPECTED, INTERRUPTED }

    @ParameterizedTest
    @EnumSource(Exit::class)
    fun `실행 종료 뒤 같은 스레드가 대기 없이 다음 claim을 하며 MDC와 interrupt를 정리한다`(exit: Exit) {
        Harness().use { h ->
            val first = job()
            val next = job(2)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val firstThread = AtomicReference<Thread>()
            whenever(h.store.claim(any(), any(), any())).thenReturn(first, next, null)
            doAnswer {
                firstThread.set(Thread.currentThread())
                assertThat(MDC.get("traceId")).isEqualTo(first.traceId)
                when (exit) {
                    Exit.NORMAL -> null
                    Exit.LOST -> throw ExecutionLostException(first.token)
                    Exit.COMPLETED -> throw ExecutionCompletedException(first.token)
                    Exit.UNEXPECTED -> throw IllegalStateException("unexpected")
                    Exit.INTERRUPTED -> { Thread.currentThread().interrupt(); throw InterruptedException() }
                }
            }.whenever(h.runner).run(first)
            doAnswer {
                assertThat(Thread.currentThread()).isSameAs(firstThread.get())
                assertThat(Thread.currentThread().isInterrupted).isFalse()
                assertThat(MDC.get("traceId")).isEqualTo(next.traceId)
                started.countDown(); finish.await(); null
            }.whenever(h.runner).run(next)
            h.worker.start()
            try { await(started); verify(h.store, times(2)).claim(any(), any(), any()) }
            finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `claim 정상 반환 전에는 runner를 시작하지 않는다`() {
        Harness().use { h ->
            val claiming = CountDownLatch(1)
            val committed = CountDownLatch(1)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenAnswer { claiming.countDown(); committed.await(); job() }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(claiming)
                verifyNoInteractions(h.runner)
                assertThat(h.worker.probe()).isFalse()
                committed.countDown(); await(started)
            } finally { h.worker.stop(); committed.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `claim 응답 유실과 resolve DB 장애 동안 준비 토큰을 유지하고 신규 claim하지 않는다`() {
        val waiting = CountDownLatch(1)
        val recover = CountDownLatch(1)
        val faults = AtomicInteger()
        Harness(database = retryDatabase {
            if (faults.incrementAndGet() == 2) { waiting.countDown(); recover.await() }
        }).use { h ->
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val claimed = job()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                it.getArgument<(ExecutionToken) -> Unit>(2)(claimed.token)
                throw DataAccessResourceFailureException("commit response lost")
            }
            whenever(h.store.resolveClaim(claimed.token)).thenThrow(DataAccessResourceFailureException("still offline"))
                .thenReturn(claimed)
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(claimed)
            h.worker.start()
            try {
                await(waiting)
                verify(h.store, times(1)).claim(any(), any(), any())
                verifyNoInteractions(h.runner)
                assertThat(h.worker.probe()).isFalse()
                recover.countDown(); await(started)
                verify(h.store, times(2)).resolveClaim(claimed.token)
                verify(h.runner, times(1)).run(claimed)
            } finally { h.worker.stop(); recover.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `resolve null이면 실행하지 않고 다음 탐색의 새 claim에 이전 토큰을 넘기지 않는다`() {
        Harness(database = retryDatabase {}).use { h ->
            val resolved = CountDownLatch(1)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val count = AtomicInteger()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                if (count.incrementAndGet() == 1) {
                    it.getArgument<(ExecutionToken) -> Unit>(2)(testToken)
                    throw DataAccessResourceFailureException("response lost")
                }
                job(2)
            }
            whenever(h.store.resolveClaim(testToken)).thenAnswer { resolved.countDown(); null }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(resolved); verifyNoInteractions(h.runner)
                wake(h.worker, started); await(started)
                verify(h.store, times(1)).resolveClaim(testToken)
                verify(h.store, times(2)).claim(any(), any(), any())
                verify(h.runner).run(job(2))
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `예상 밖 claim 오류는 지정 간격만큼 대기하고 같은 루프에서 재개한다`() {
        val waiting = CountDownLatch(1)
        val resume = CountDownLatch(1)
        Harness(waiter = QueueLoopWaiter {
            assertThat(it).isEqualTo(Duration.ofDays(1)); waiting.countDown(); resume.await()
        }).use { h ->
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenThrow(IllegalStateException("bad query")).thenReturn(job())
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(waiting); verify(h.store, times(1)).claim(any(), any(), any())
                assertThat(h.worker.probe()).isFalse()
                resume.countDown(); await(started)
            } finally { h.worker.stop(); resume.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `claim 오류 대기 중 stop은 대기를 중단하고 신규 claim을 하지 않는다`() {
        val waiting = CountDownLatch(1)
        val stopped = CountDownLatch(1)
        Harness(waiter = QueueLoopWaiter {
            waiting.countDown()
            try { CountDownLatch(1).await() } finally { stopped.countDown() }
        }).use { h ->
            whenever(h.store.claim(any(), any(), any())).thenThrow(IllegalStateException("bad query"))
            h.worker.start(); await(waiting)
            h.worker.stop(); await(stopped)
            assertThat(h.worker.probe()).isFalse()
            verify(h.store, times(1)).claim(any(), any(), any())
            verifyNoInteractions(h.runner)
        }
    }

    @Test
    fun `DB 복구 대기 중 stop은 준비 토큰의 resolve나 신규 claim을 시작하지 않는다`() {
        val waiting = CountDownLatch(1)
        val stopped = CountDownLatch(1)
        Harness(database = retryDatabase {
            waiting.countDown()
            try { CountDownLatch(1).await() } finally { stopped.countDown() }
        }).use { h ->
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                it.getArgument<(ExecutionToken) -> Unit>(2)(testToken)
                throw DataAccessResourceFailureException("offline")
            }
            h.worker.start(); await(waiting)
            h.worker.stop(); await(stopped)
            verify(h.store, times(1)).claim(any(), any(), any())
            verify(h.store, never()).resolveClaim(any())
            verifyNoInteractions(h.runner)
        }
    }

    @Test
    fun `느린 heartbeat는 다른 Job 갱신과 다음 tick을 막지 않고 중복 제출하지 않는다`() {
        Harness(concurrency = 2, heartbeatInterval = Duration.ofMillis(20)).use { h ->
            val started = CountDownLatch(2); val blocked = CountDownLatch(1); val other = CountDownLatch(3)
            val finish = CountDownLatch(1); val release = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), job(2), null)
            whenever(h.store.renew(job().token)).thenAnswer { blocked.countDown(); release.await(); 1 }
            whenever(h.store.renew(job(2).token)).thenAnswer { other.countDown(); 1 }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(started); await(blocked); await(other)
                verify(h.store, times(1)).renew(job().token)
                assertThat(h.worker.probe()).isFalse()
            } finally { release.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `restart는 취소를 무시하는 이전 heartbeat가 끝날 때까지 예약을 유지한다`() {
        Harness().use { h ->
            val first = CountDownLatch(1); val next = CountDownLatch(1); val renewing = CountDownLatch(1)
            val release = CountDownLatch(1); val renewedNext = CountDownLatch(1); val finish = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), job(2), null)
            whenever(h.store.renew(job().token)).thenAnswer { renewing.countDown(); awaitIgnoringInterrupt(release); 0 }
            whenever(h.store.renew(job(2).token)).thenAnswer { renewedNext.countDown(); 1 }
            doAnswer {
                first.countDown()
                try { CountDownLatch(1).await() } catch (_: InterruptedException) { }
                null
            }.whenever(h.runner).run(job())
            doAnswer { next.countDown(); finish.await(); null }.whenever(h.runner).run(job(2))
            h.worker.start()
            try {
                await(first); h.worker.heartbeat(); await(renewing)
                h.worker.stop(); h.worker.start(); wake(h.worker, next); await(next)
                assertThat(h.worker.heartbeat()).isEmpty()
                verify(h.store, never()).renew(job(2).token)
                release.countDown()
                until { h.worker.heartbeat().isNotEmpty() }
                await(renewedNext)
                verify(h.store, never()).snapshot(job().token)
            } finally { release.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `실행 종료는 heartbeat DB 복구 대기를 중단하고 다음 heartbeat에 interrupt를 남기지 않는다`() {
        val waiting = CountDownLatch(1); val cancelled = CountDownLatch(1)
        val heartbeatThread = AtomicReference<Thread>()
        Harness(database = retryDatabase {
            heartbeatThread.set(Thread.currentThread()); waiting.countDown()
            try { CountDownLatch(1).await() } finally { cancelled.countDown() }
        }).use { h ->
            val started = CountDownLatch(1); val next = CountDownLatch(1); val leave = CountDownLatch(1)
            val finish = CountDownLatch(1); val renewedNext = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), job(2), null)
            whenever(h.store.renew(job().token)).thenThrow(DataAccessResourceFailureException("offline"))
            whenever(h.store.renew(job(2).token)).thenAnswer {
                assertThat(Thread.currentThread()).isSameAs(heartbeatThread.get())
                assertThat(Thread.currentThread().isInterrupted).isFalse()
                renewedNext.countDown(); 1
            }
            doAnswer { started.countDown(); leave.await(); null }.whenever(h.runner).run(job())
            doAnswer { next.countDown(); finish.await(); null }.whenever(h.runner).run(job(2))
            h.worker.start()
            try {
                await(started); h.worker.heartbeat(); await(waiting)
                leave.countDown(); await(cancelled); await(next)
                until { h.worker.heartbeat().isNotEmpty() }; await(renewedNext)
                verify(h.store, never()).snapshot(job().token)
            } finally { leave.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `별도 heartbeat는 모든 runner가 대기 중이어도 활성 Job을 갱신한다`() {
        Harness(concurrency = 2, heartbeatInterval = Duration.ofMillis(20)).use { h ->
            val started = CountDownLatch(2)
            val renewed = CountDownLatch(2)
            val finish = CountDownLatch(1)
            val count = AtomicInteger()
            val tokens = java.util.concurrent.ConcurrentHashMap.newKeySet<ExecutionToken>()
            whenever(h.store.claim(any(), any(), any())).thenAnswer { job(count.incrementAndGet().toLong()) }
            whenever(h.store.renew(any())).thenAnswer {
                assertThat(Thread.currentThread().name).startsWith("job-heartbeat-")
                if (tokens.add(it.getArgument(0))) renewed.countDown()
                1
            }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try { await(started); await(renewed) } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `한 heartbeat의 예상 밖 오류가 다른 Job의 갱신을 막지 않는다`() {
        Harness(concurrency = 2).use { h ->
            val started = CountDownLatch(2)
            val finish = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), job(2))
            whenever(h.store.renew(job().token)).thenThrow(IllegalStateException("bad update"))
            whenever(h.store.renew(job(2).token)).thenReturn(1)
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(any<ClaimedJob>())
            h.worker.start()
            try {
                await(started); h.worker.heartbeat()
                verify(h.store, timeout(1000)).renew(job().token)
                verify(h.store, timeout(1000)).renew(job(2).token)
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `heartbeat 실행권 상실 뒤 같은 스레드가 interrupt 없이 다음 Job을 처리한다`() {
        Harness().use { h ->
            val started = CountDownLatch(1)
            val nextStarted = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val firstThread = AtomicReference<Thread>()
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), job(2), null)
            doAnswer {
                firstThread.set(Thread.currentThread()); started.countDown()
                try { CountDownLatch(1).await() } catch (_: InterruptedException) { Thread.currentThread().interrupt() }
                null
            }.whenever(h.runner).run(job())
            doAnswer {
                assertThat(Thread.currentThread()).isSameAs(firstThread.get())
                assertThat(Thread.currentThread().isInterrupted).isFalse()
                nextStarted.countDown(); finish.await(); null
            }.whenever(h.runner).run(job(2))
            h.worker.start()
            try { await(started); h.worker.heartbeat(); await(nextStarted) }
            finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `이전 Job의 늦은 heartbeat는 같은 스레드의 다음 Job을 중단하지 않는다`() {
        Harness().use { h ->
            val firstStarted = CountDownLatch(1)
            val returnFirst = CountDownLatch(1)
            val renewing = CountDownLatch(1)
            val returnRenew = CountDownLatch(1)
            val nextStarted = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val interrupted = AtomicBoolean(false)
            val firstThread = AtomicReference<Thread>()
            val pool = Executors.newSingleThreadExecutor()
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), job(2), null)
            whenever(h.store.renew(any())).thenReturn(1)
            whenever(h.store.renew(job().token)).thenAnswer { renewing.countDown(); awaitIgnoringInterrupt(returnRenew); 0 }
            doAnswer {
                firstThread.set(Thread.currentThread()); firstStarted.countDown(); returnFirst.await(); null
            }.whenever(h.runner).run(job())
            doAnswer {
                assertThat(Thread.currentThread()).isSameAs(firstThread.get())
                nextStarted.countDown()
                try { finish.await() } catch (_: InterruptedException) { interrupted.set(true) }
                null
            }.whenever(h.runner).run(job(2))
            h.worker.start()
            try {
                await(firstStarted)
                val heartbeat = pool.submit { finishHeartbeats(h.worker) }
                await(renewing); returnFirst.countDown(); await(nextStarted)
                returnRenew.countDown(); heartbeat.get(3, TimeUnit.SECONDS)
                assertThat(interrupted.get()).isFalse()
                finish.countDown()
            } finally {
                returnFirst.countDown(); returnRenew.countDown(); finish.countDown(); pool.shutdownNow()
            }
        }
    }

    @Test
    fun `owner가 해제된 완료 증거는 heartbeat가 cleanup을 중단하지 않는다`() {
        Harness().use { h ->
            val started = CountDownLatch(1); val finish = CountDownLatch(1); val cleaned = CountDownLatch(1)
            val interrupted = AtomicBoolean()
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), null)
            whenever(h.store.snapshot(testToken)).thenReturn(JobSnapshot(null, 1, IndexingJobStatus.COMPLETED,
                null, null, testNow, 2, "INDEXED", JobType.INDEX, job().documentId, job().documentVersionId))
            doAnswer {
                started.countDown()
                try { finish.await() } catch (_: InterruptedException) { interrupted.set(true) }
                finally { cleaned.countDown() }
                null
            }.whenever(h.runner).run(job())
            h.worker.start()
            try {
                await(started); finishHeartbeats(h.worker); finish.countDown(); await(cleaned)
                assertThat(interrupted.get()).isFalse()
            } finally { finish.countDown() }
        }
    }

    @Test
    fun `실패로 자발적 해제된 실행은 heartbeat가 정리 중인 스레드를 중단하지 않는다`() {
        Harness().use { h ->
            val released = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val cleaned = CountDownLatch(1)
            val interrupted = AtomicBoolean(false)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job(), null)
            whenever(h.store.snapshot(testToken)).thenReturn(JobSnapshot(null, testToken.epoch,
                IndexingJobStatus.RETRY_WAIT, null, testNow.plusSeconds(30), testNow, testToken.epoch,
                outcome = "RETRY_SCHEDULED", errorCode = "TEMP", errorMessage = "message"))
            h.results.prepare(testToken) { FailureRecord(com.osscontest.worker.indexing.pipeline.service.FailureDecision(
                IndexingJobStatus.RETRY_WAIT, testNow.plusSeconds(30)), "TEMP", "message") }
            doAnswer {
                released.countDown()
                try { finish.await() } catch (_: InterruptedException) { interrupted.set(true) }
                finally { cleaned.countDown() }
                null
            }.whenever(h.runner).run(job())
            h.worker.start()
            try {
                await(released); finishHeartbeats(h.worker); finish.countDown(); await(cleaned)
                assertThat(interrupted.get()).isFalse()
            } finally { h.worker.stop(); finish.countDown() }
        }
    }

    @Test
    fun `stop 뒤 반환한 claim은 실행하지 않고 새 세대도 이전 루프 종료 전 상한을 넘지 않는다`() {
        Harness().use { h ->
            val claiming = CountDownLatch(1)
            val returnClaim = CountDownLatch(1)
            val started = CountDownLatch(1)
            val finish = CountDownLatch(1)
            val count = AtomicInteger()
            whenever(h.store.claim(any(), any(), any())).thenAnswer {
                if (count.incrementAndGet() == 1) {
                    claiming.countDown(); awaitIgnoringInterrupt(returnClaim); job()
                } else job(2)
            }
            doAnswer { started.countDown(); finish.await(); null }.whenever(h.runner).run(job(2))
            h.worker.start()
            try {
                await(claiming); h.worker.stop(); h.worker.stop()
                assertThat(h.worker.probe()).isFalse()
                h.worker.start(); h.worker.start()
                assertThat(h.worker.probe()).isFalse()
                assertThat(count.get()).isEqualTo(1)
                returnClaim.countDown(); wake(h.worker, started); await(started)
                verify(h.runner, never()).run(job())
                verify(h.runner, times(1)).run(job(2))
                verify(h.store, times(2)).claim(any(), any(), any())
            } finally { h.worker.stop(); returnClaim.countDown(); finish.countDown() }
        }
    }

    @Test
    fun `stop은 실행 중인 runner를 중단하고 다음 claim을 하지 않는다`() {
        Harness().use { h ->
            val started = CountDownLatch(1)
            val interrupted = CountDownLatch(1)
            whenever(h.store.claim(any(), any(), any())).thenReturn(job())
            doAnswer {
                started.countDown()
                try { CountDownLatch(1).await() } catch (_: InterruptedException) {
                    interrupted.countDown(); Thread.currentThread().interrupt()
                }
                null
            }.whenever(h.runner).run(job())
            h.worker.start(); await(started)
            h.worker.stop(); await(interrupted)
            assertThat(h.worker.isRunning).isFalse()
            assertThat(h.worker.probe()).isFalse()
            verify(h.store, times(1)).claim(any(), any(), any())
        }
    }

    private fun job(id: Long = 1) = ClaimedJob(testToken.copy(jobId = id), JobType.INDEX,
        UUID.nameUUIDFromBytes("event-$id".toByteArray()), id + 10, id + 20, "trace-$id")

    private fun finishHeartbeats(worker: DbJobWorker) {
        worker.heartbeat().forEach {
            try { it.get(3, TimeUnit.SECONDS) } catch (_: java.util.concurrent.CancellationException) { }
        }
    }

    private fun until(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (!condition()) { check(System.nanoTime() < deadline); Thread.sleep(1) }
    }

    private fun await(latch: CountDownLatch) { check(latch.await(3, TimeUnit.SECONDS)) { "Worker did not reach expected state" } }

    /**
     * 루프 하나를 깨운다. probe 타이머도 같은 슬롯을 노리므로, 타이머가 먼저 깨운 루프가 [reached]에 닿았으면
     * 그것으로 충분하다. 그 루프가 슬롯을 쥐고 있으면 이 probe는 계속 실패한다.
     */
    private fun wake(worker: DbJobWorker, reached: CountDownLatch) {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (reached.count > 0 && !worker.probe()) {
            check(System.nanoTime() < until) { "Worker did not release its loop reservation" }
            Thread.sleep(1)
        }
    }

    private fun awaitIgnoringInterrupt(latch: CountDownLatch) {
        while (true) {
            try { latch.await(); return } catch (_: InterruptedException) { /* stop 뒤에도 JDBC 호출이 끝나 돌아오는 상황을 흉내 낸다. */ }
        }
    }

    /** DB 없이 execute의 재시도만 흉내 내 Worker의 준비 토큰 처리를 검증한다. */
    private fun retryDatabase(beforeRetry: () -> Unit): DatabaseOperations = mock<DatabaseOperations>().apply {
        whenever(execute<Any?>(any(), any())).thenAnswer {
            retryAction(it.getArgument(1), beforeRetry)
        }
    }

    private fun retryAction(action: () -> Any?, beforeRetry: () -> Unit): Any? {
        while (true) {
            DatabaseOperations.checkInterrupted()
            try { return action() } catch (_: DataAccessResourceFailureException) { beforeRetry() }
        }
    }
}
