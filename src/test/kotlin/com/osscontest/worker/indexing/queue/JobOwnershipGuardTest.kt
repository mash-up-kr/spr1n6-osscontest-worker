package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*

class JobOwnershipGuardTest {
    private val locks: JobExecutionLocks = mock()
    private val guard = JobOwnershipGuard(locks)

    @Test
    fun `일반 쓰기는 만료를 거절하지만 동일 실행 heartbeat는 만료 lease를 갱신할 수 있다`() {
        val expired = lockedJob(processingJob().apply { leaseUntil = testNow.minusSeconds(1) })
        whenever(locks.lock(1)).thenReturn(expired)
        assertThat(guard.owns(expired, testToken)).isTrue()
        assertThat(guard.canWrite(expired, testToken)).isFalse()
        assertThatThrownBy { guard.requireProcessing(expired.job, testToken) }.isInstanceOf(LeaseExpiredException::class.java)
    }

    @Test
    fun `Worker Job epoch 문서 epoch 상태 lease 존재 조건은 모두 필요하다`() {
        val executions = listOf(
            lockedJob(processingJob(testToken.copy(workerId = "other"))),
            lockedJob(processingJob(testToken.copy(epoch = 2))),
            lockedJob(documentEpoch = 2),
            lockedJob(processingJob(status = IndexingJobStatus.RETRY_WAIT)),
            lockedJob(processingJob().apply { workerId = null; leaseUntil = null }),
            lockedJob(processingJob().apply { leaseUntil = null }),
        )
        executions.forEach {
            whenever(locks.lock(1)).thenReturn(it)
            assertThat(guard.owns(it, testToken)).isFalse()
            assertThat(guard.canWrite(it, testToken)).isFalse()
            assertThatThrownBy { guard.requireProcessing(it.job, testToken) }.isInstanceOf(ExecutionLostException::class.java)
        }
    }

    @Test
    fun `이미 완료된 실행 조회는 후속 문서 epoch를 요구하지 않는다`() {
        val completed = lockedJob(processingJob(status = IndexingJobStatus.COMPLETED), documentEpoch = 2)
        whenever(locks.lock(1)).thenReturn(completed)
        assertThat(guard.lock(testToken)).isSameAs(completed.job)
        assertThat(guard.canWrite(completed, testToken)).isFalse()
    }
}
