package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*

class ExecutionOperationsTest {
    private val store: JobQueueStore = mock()
    private val execution = ExecutionOperations(directDatabase(), store)
    private fun snapshot(epoch: Long = testToken.epoch, expired: Boolean = false) = JobSnapshot(testToken.workerId, epoch,
        IndexingJobStatus.PROCESSING, testNow.plusSeconds(if (expired) -1 else 60), null, testNow)

    @Test
    fun `아직 회수되지 않은 동일 실행의 만료 lease는 조건부 연장 후 재개한다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot(expired = true), snapshot())
        whenever(store.renew(testToken)).thenReturn(1)
        execution.checkpoint(testToken)
        verify(store).renew(testToken)
    }

    @Test
    fun `회수가 먼저 끝난 이전 실행은 연장하거나 결과를 쓰지 않는다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot(epoch = testToken.epoch + 1, expired = true))
        assertThatThrownBy { execution.checkpoint(testToken) }.isInstanceOf(ExecutionLostException::class.java)
        verify(store, never()).renew(any())
    }

    @Test
    fun `저장 트랜잭션 종료 후 연장하고 같은 결과 입력으로만 저장을 재호출한다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot(expired = true), snapshot())
        whenever(store.renew(testToken)).thenReturn(1)
        var calls = 0
        val result = execution.database(testToken) { if (++calls == 1) throw LeaseExpiredException(); "retained" }
        assertThat(result).isEqualTo("retained")
        assertThat(calls).isEqualTo(2)
    }
    @Test
    fun `실패 커밋 응답 유실 후 해제된 실행은 일반 checkpoint 없이 실패 결과를 확인한다`() {
        val database = DatabaseOperations(org.springframework.jdbc.core.JdbcTemplate(
            org.springframework.jdbc.datasource.DriverManagerDataSource("jdbc:h2:mem:failure-loss", "sa", "")),
            RetryWaiter {}, DatabaseWaitProperties(java.time.Duration.ofMillis(1), java.time.Duration.ofSeconds(1)),
            io.micrometer.core.instrument.simple.SimpleMeterRegistry())
        var calls = 0
        val result = ExecutionOperations(database, store).failure(testToken) {
            if (++calls == 1) throw org.springframework.dao.DataAccessResourceFailureException("commit response lost")
            IndexingJobStatus.RETRY_WAIT
        }
        assertThat(result).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        assertThat(calls).isEqualTo(2)
        verifyNoInteractions(store)
    }

}
