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
    @Test
    fun `활성 슬롯이 차면 추가 claim하지 않는다`() {
        val store: JobQueueStore = mock()
        val runner: IndexingPipelineRunner = mock()
        val started = CountDownLatch(2)
        val hold = CountDownLatch(1)
        val count = AtomicInteger()
        whenever(store.claim(any(), any(), any())).thenAnswer {
            ClaimedJob(testToken.copy(jobId = count.incrementAndGet().toLong()), JobType.INDEX, UUID.randomUUID(), 2, 3, null)
        }
        doAnswer { started.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(any<ClaimedJob>())
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 2, pollInterval = Duration.ofDays(1)),
            directDatabase(), store, mock(), runner, 5)
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
        whenever(store.claim(any(), any(), any())).thenAnswer {
            it.getArgument<(ExecutionToken) -> Unit>(2)(testToken)
            throw DataAccessResourceFailureException("commit response lost")
        }
        whenever(store.resolveClaim(testToken)).thenReturn(job)
        doAnswer { started.countDown(); hold.await(3, TimeUnit.SECONDS); null }.whenever(runner).run(job)
        val db = DatabaseOperations(JdbcTemplate(DriverManagerDataSource("jdbc:h2:mem:claim-loss", "sa", "")),
            RetryWaiter {}, DatabaseWaitProperties(Duration.ofMillis(1), Duration.ofSeconds(1)), SimpleMeterRegistry())
        val worker = DbJobWorker(QueueProperties(workerId = testToken.workerId, concurrency = 1, pollInterval = Duration.ofDays(1)),
            db, store, mock(), runner, 5)
        try {
            worker.start()
            check(started.await(3, TimeUnit.SECONDS))
            verify(store).claim(any(), any(), any())
            verify(store).resolveClaim(testToken)
            verify(runner).run(job)
        } finally {
            worker.stop()
            hold.countDown()
        }
    }
}
