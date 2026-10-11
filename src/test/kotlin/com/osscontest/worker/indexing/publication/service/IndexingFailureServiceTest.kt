package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryJitter
import com.osscontest.worker.indexing.pipeline.service.RetryPolicy
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.support.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.Duration

class IndexingFailureServiceTest {
    private val jobs: IndexingJobRepository = mock()
    private val locks: JobExecutionLocks = mock()
    private val meters = SimpleMeterRegistry()
    private val policy = RetryPolicy(RetryJitter { 1.0 })
    private val service = IndexingFailureService(jobs, meters, JobOwnershipGuard(locks), policy)

    private fun fail(permanent: Boolean = false) = service.recordFailure(testToken, "TIMEOUT", "message",
        permanent, 5, Duration.ofSeconds(30))

    private fun setup(attempts: Int = 1) {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob(attempt = attempts)))
        whenever(jobs.currentDbTimestamp()).thenReturn(testNow)
        whenever(jobs.fail(any(), any(), any(), any(), anyOrNull(), any(), any())).thenReturn(1)
    }

    @Test
    fun `문서 epoch 불일치는 실패 상태와 메트릭을 기록하지 않는다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(documentEpoch = 2))
        assertThatThrownBy { fail() }.isInstanceOf(ExecutionLostException::class.java)
        verify(jobs, never()).fail(any(), any(), any(), any(), anyOrNull(), any(), any())
        assertThat(meters.meters).isEmpty()
    }

    @Test
    fun `일시 실패는 DB 시각의 선형 지연을 기록하고 재시도 예약으로 집계한다`() {
        setup(3)
        assertThat(fail()).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        verify(jobs).fail(1, testToken.workerId, 1, "RETRY_WAIT", testNow.plusSeconds(90), "TIMEOUT", "message")
        assertThat(meters.get("indexing_retry_scheduled_total").counter().count()).isEqualTo(1.0)
        assertThat(meters.find("indexing_job_failed_total").counters()).isEmpty()
    }

    @Test
    fun `영구 실패와 마지막 시도는 재시도 시각 없이 최종 실패로 집계한다`() {
        setup(5)
        assertThat(fail()).isEqualTo(IndexingJobStatus.FAILED)
        verify(jobs).fail(1, testToken.workerId, 1, "FAILED", null, "TIMEOUT", "message")
        assertThat(meters.get("indexing_job_failed_total").counter().count()).isEqualTo(1.0)
    }

    @Test
    fun `실패 커밋 뒤 소유자가 해제돼도 같은 epoch 재호출은 deadline을 다시 쓰지 않는다`() {
        val released = processingJob(status = IndexingJobStatus.RETRY_WAIT).apply {
            workerId = null; leaseUntil = null; nextRetryAt = testNow.plusSeconds(30)
        }
        whenever(locks.lock(1)).thenReturn(lockedJob(released, documentEpoch = 2))
        assertThat(fail()).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        verify(jobs, never()).fail(any(), any(), any(), any(), anyOrNull(), any(), any())
        assertThat(meters.meters).isEmpty()
    }

    @Test
    fun `다음 claim의 epoch는 실패 재호출로 덮어쓰지 않는다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob(testToken.copy(epoch = 2))))
        assertThatThrownBy { fail() }.isInstanceOf(ExecutionLostException::class.java)
        verify(jobs, never()).fail(any(), any(), any(), any(), anyOrNull(), any(), any())
    }

    @Test
    fun `검증 이후 lease가 만료돼 UPDATE가 거절되면 메트릭도 기록하지 않는다`() {
        setup()
        whenever(jobs.fail(any(), any(), any(), any(), anyOrNull(), any(), any())).thenReturn(0)
        assertThatThrownBy { fail() }.isInstanceOf(LeaseExpiredException::class.java)
        assertThat(meters.meters).isEmpty()
    }
}
