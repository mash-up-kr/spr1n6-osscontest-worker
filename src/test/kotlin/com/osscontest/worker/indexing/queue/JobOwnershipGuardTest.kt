/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

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
        assertThatThrownBy { guard.requireProcessing(expired, testToken) }.isInstanceOf(LeaseExpiredException::class.java)
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
            lockedJob(processingJob(testToken.copy(jobId = 99))),
        )
        executions.forEach {
            whenever(locks.lock(1)).thenReturn(it)
            assertThat(guard.owns(it, testToken)).isFalse()
            assertThat(guard.canWrite(it, testToken)).isFalse()
            assertThatThrownBy { guard.requireProcessing(it, testToken) }.isInstanceOf(ExecutionLostException::class.java)
        }
    }

    @Test
    fun `이미 완료된 실행 조회는 후속 문서 epoch를 요구하지 않는다`() {
        val completed = lockedJob(processingJob(status = IndexingJobStatus.COMPLETED), documentEpoch = 2)
        whenever(locks.lock(1)).thenReturn(completed)
        assertThat(guard.lock(testToken)).isSameAs(completed.job)
        assertThat(guard.canWrite(completed, testToken)).isFalse()
    }

    @Test
    fun `잠금 후 DB 시각이 lease와 같으면 일반 쓰기를 거절한다`() {
        val execution = lockedJob(now = testNow.plusSeconds(60))
        assertThat(guard.owns(execution, testToken)).isTrue()
        assertThatThrownBy { guard.requireProcessing(execution, testToken) }.isInstanceOf(LeaseExpiredException::class.java)
    }

    @Test
    fun `이미 잠근 실행 검증은 같은 행을 다시 잠그지 않는다`() {
        val execution = lockedJob()
        whenever(locks.lock(1)).thenReturn(execution)
        val locked = guard.lockForResult(testToken)
        guard.requireProcessing(locked, testToken)
        verify(locks, times(1)).lock(1)
    }

    @Test
    fun `Entity 진입점도 잠금 뒤 최신 실행 상태로 검증한다`() {
        val previous = processingJob()
        whenever(locks.lock(1)).thenReturn(lockedJob(documentEpoch = 2))
        assertThatThrownBy { guard.requireProcessing(previous, testToken) }.isInstanceOf(ExecutionLostException::class.java)
    }

    @Test
    fun `잠금 projection이 있어도 트랜잭션이 끝난 뒤 일반 가드를 사용하지 못한다`() {
        doThrow(IllegalStateException("Execution locks require a write transaction")).whenever(locks).requireTransaction()
        assertThatThrownBy { guard.requireProcessing(lockedJob(), testToken) }.isInstanceOf(IllegalStateException::class.java)
    }
}
