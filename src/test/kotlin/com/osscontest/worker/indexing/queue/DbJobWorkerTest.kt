package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.support.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class DbJobWorkerTest {
    private fun breakerWith(admission: ClaimAdmission): CircuitBreaker =
        mock { on { admission() } doReturn admission }

    @Test
    fun `활성 슬롯이 차면 추가 claim하지 않는다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val started = CountDownLatch(2)
        val hold = CountDownLatch(1)
        val count = AtomicInteger()
        whenever(store.claim(any(), anyOrNull(), any())).thenAnswer {
            ClaimedJob(testToken.copy(jobId = count.incrementAndGet().toLong()), JobType.INDEX, UUID.randomUUID(), 2, 3, null)
        }
        doAnswer { started.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(any<ClaimedJob>())
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 2, pollInterval = Duration.ofDays(1)),
            directDatabase(), store, breakerWith(ClaimAdmission.NORMAL), runner, 5, Duration.ofSeconds(30))
        try {
            worker.start()
            check(started.await(3, TimeUnit.SECONDS))
            worker.poll()
            assertThat(count.get()).isEqualTo(2)
        } finally {
            worker.stop()
            hold.countDown()
        }
    }

    @Test
    fun `claim 커밋 응답 유실은 같은 준비 토큰을 확인하고 추가 claim하지 않는다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val started = CountDownLatch(1)
        val hold = CountDownLatch(1)
        val job = ClaimedJob(testToken, JobType.INDEX, UUID.randomUUID(), 2, 3, null)
        whenever(store.claim(any(), anyOrNull(), any())).thenAnswer {
            it.getArgument<(ExecutionToken) -> Unit>(2)(testToken)
            throw DataAccessResourceFailureException("commit response lost")
        }
        whenever(store.resolveClaim(testToken)).thenReturn(job)
        doAnswer { started.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(job)
        val db = DatabaseOperations(JdbcTemplate(DriverManagerDataSource("jdbc:h2:mem:claim-loss", "sa", "")),
            RetryWaiter {}, DatabaseWaitProperties(Duration.ofMillis(1), Duration.ofSeconds(1)), SimpleMeterRegistry())
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 1, pollInterval = Duration.ofDays(1)),
            db, store, breakerWith(ClaimAdmission.NORMAL), runner, 5, Duration.ofSeconds(30))
        try {
            worker.start()
            check(started.await(3, TimeUnit.SECONDS))
            verify(store).claim(any(), anyOrNull(), any())
            verify(store).resolveClaim(testToken)
            verify(runner).run(job)
        } finally {
            worker.stop()
            hold.countDown()
        }
    }

    @Test
    fun `breaker가 열려 있으면 임베딩을 쓰지 않는 DELETE Job만 claim한다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val started = CountDownLatch(2)
        val hold = CountDownLatch(1)
        val count = AtomicInteger()
        whenever(store.claim(any(), eq(JobType.DELETE), any())).thenAnswer {
            if (count.get() == 2) null
            else ClaimedJob(testToken.copy(jobId = count.incrementAndGet().toLong()), JobType.DELETE, UUID.randomUUID(), 2, null, null)
        }
        doAnswer { started.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(any<ClaimedJob>())
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 3, pollInterval = Duration.ofDays(1)),
            directDatabase(), store, breakerWith(ClaimAdmission.BLOCKED), runner, 5, Duration.ofSeconds(30))
        try {
            worker.start()
            check(started.await(3, TimeUnit.SECONDS))
            assertThat(count.get()).describedAs("DELETE 후보가 남아 있는 동안 빈 슬롯마다 claim한다").isEqualTo(2)
            verify(store, never()).claim(any(), isNull(), any())
            verify(store, atLeastOnce()).recover(any(), any())
        } finally {
            worker.stop()
            hold.countDown()
        }
    }

    @Test
    fun `시험 호출 자격만 있으면 poll마다 한 건만 claim한다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val started = CountDownLatch(1)
        val hold = CountDownLatch(1)
        val count = AtomicInteger()
        whenever(store.claim(any(), anyOrNull(), any())).thenAnswer {
            ClaimedJob(testToken.copy(jobId = count.incrementAndGet().toLong()), JobType.INDEX, UUID.randomUUID(), 2, 3, null)
        }
        doAnswer { started.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(any<ClaimedJob>())
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 3, pollInterval = Duration.ofDays(1)),
            directDatabase(), store, breakerWith(ClaimAdmission.LIMITED), runner, 5, Duration.ofSeconds(30))
        try {
            worker.start()
            check(started.await(3, TimeUnit.SECONDS))
            assertThat(count.get()).describedAs("슬롯이 남아 있어도 한 건만 claim한다").isEqualTo(1)
        } finally {
            worker.stop()
            hold.countDown()
        }
    }

    @Test
    fun `실패 실행 종료 후 슬롯을 반환해 다음 문서를 처리한다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val firstDone = CountDownLatch(1)
        val nextStarted = CountDownLatch(1)
        val hold = CountDownLatch(1)
        val first = ClaimedJob(testToken, JobType.INDEX, UUID.randomUUID(), 2, 3, null)
        val next = first.copy(token = testToken.copy(jobId = 2), documentId = 4)
        whenever(store.claim(any(), anyOrNull(), any())).thenReturn(first, next, null)
        doAnswer { firstDone.countDown(); null }.whenever(runner).run(first)
        doAnswer { nextStarted.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(next)
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 1,
            pollInterval = Duration.ofDays(1)), directDatabase(), store, breakerWith(ClaimAdmission.NORMAL), runner, 5, Duration.ofSeconds(30))
        try {
            worker.start()
            check(firstDone.await(3, TimeUnit.SECONDS))
            val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
            while (nextStarted.count > 0 && System.nanoTime() < until) {
                worker.poll()
                nextStarted.await(10, TimeUnit.MILLISECONDS)
            }
            assertThat(nextStarted.count).isZero()
            verify(runner).run(first)
            verify(runner).run(next)
        } finally { worker.stop(); hold.countDown() }
    }

    @Test
    fun `실패로 자발적 해제된 실행은 heartbeat가 정리 중인 스레드를 중단하지 않는다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val released = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val cleaned = CountDownLatch(1)
        val interrupted = java.util.concurrent.atomic.AtomicBoolean(false)
        val job = ClaimedJob(testToken, JobType.INDEX, UUID.randomUUID(), 2, 3, null)
        whenever(store.claim(any(), anyOrNull(), any())).thenReturn(job, null)
        whenever(store.snapshot(testToken)).thenReturn(JobSnapshot(null, testToken.epoch,
            com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.RETRY_WAIT, null, testNow.plusSeconds(30), testNow, testToken.epoch))
        doAnswer {
            released.countDown()
            try { finish.await(3, TimeUnit.SECONDS) }
            catch (_: InterruptedException) { interrupted.set(true) }
            finally { cleaned.countDown() }
            null
        }.whenever(runner).run(job)
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 1,
            pollInterval = Duration.ofDays(1)), directDatabase(), store, breakerWith(ClaimAdmission.NORMAL), runner, 5, Duration.ofSeconds(30))
        try {
            worker.start()
            check(released.await(3, TimeUnit.SECONDS))
            worker.heartbeat()
            finish.countDown()
            check(cleaned.await(3, TimeUnit.SECONDS))
            assertThat(interrupted.get()).isFalse()
        } finally { finish.countDown(); worker.stop() }
    }
}
