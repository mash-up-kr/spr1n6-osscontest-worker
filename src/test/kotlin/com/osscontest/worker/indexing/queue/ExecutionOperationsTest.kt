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
        IndexingJobStatus.PROCESSING, testNow.plusSeconds(if (expired) -1 else 60), null, testNow, testToken.epoch)

    @Test
    fun `완료 쓰기 lease 만료는 트랜잭션 종료 후 갱신하고 보관한 입력만 재시도한다`() {
        var snapshot = snapshot().copy(type = JobType.INDEX, documentId = 2, documentVersionId = 3)
        whenever(store.snapshot(testToken)).thenAnswer { snapshot }
        whenever(store.renew(testToken)).thenAnswer { snapshot = snapshot.copy(leaseUntil = testNow.plusSeconds(60)); 1 }
        var calls = 0
        val result = execution.completion(testToken, ExecutionIdentity(JobType.INDEX, 2, 3), { "completed" }) {
            if (++calls == 1) {
                snapshot = snapshot.copy(leaseUntil = testNow.minusSeconds(1))
                throw LeaseExpiredException()
            }
            "retained-result"
        }
        assertThat(result).isEqualTo("retained-result")
        assertThat(calls).isEqualTo(2)
        verify(store).renew(testToken)
    }

    @Test
    fun `완료 전용 경계는 owner 해제와 문서 epoch 증가 후 action을 생략한다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot().copy(workerId = null, leaseUntil = null,
            status = IndexingJobStatus.COMPLETED, documentEpoch = 2, outcome = "INDEXED", type = JobType.INDEX,
            documentId = 2, documentVersionId = 3))
        assertThat(execution.completion(testToken, ExecutionIdentity(JobType.INDEX, 2, 3), { "confirmed" }) {
            error("must not replay")
        }).isEqualTo("confirmed")
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["missing-outcome", "wrong-epoch", "wrong-type", "owner-retained", "wrong-document"])
    fun `잘못된 완료 증거로 저장 작업을 재개하지 않는다`(reason: String) {
        var receipt = snapshot().copy(workerId = null, leaseUntil = null, status = IndexingJobStatus.COMPLETED,
            outcome = "INDEXED", type = JobType.INDEX, documentId = 2, documentVersionId = 3)
        receipt = when (reason) {
            "missing-outcome" -> receipt.copy(outcome = null)
            "wrong-epoch" -> receipt.copy(epoch = 2)
            "wrong-type" -> receipt.copy(type = JobType.DELETE, outcome = "DELETED")
            "owner-retained" -> receipt.copy(workerId = "worker")
            else -> receipt.copy(documentId = 99)
        }
        whenever(store.snapshot(testToken)).thenReturn(receipt)
        assertThatThrownBy {
            execution.completion(testToken, ExecutionIdentity(JobType.INDEX, 2, 3), { "done" }) { error("action invoked") }
        }.isInstanceOf(ExecutionLostException::class.java)
    }

    @Test
    fun `문서 epoch만 바뀌어도 이전 실행은 갱신하거나 DB 작업을 재개하지 않는다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot().copy(documentEpoch = 2))
        assertThatThrownBy { execution.checkpoint(testToken) }.isInstanceOf(ExecutionLostException::class.java)
        verify(store, never()).renew(any())
    }

    @Test
    fun `lease 갱신 뒤에도 전체 실행권을 다시 확인한다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot(expired = true), snapshot().copy(documentEpoch = 2))
        whenever(store.renew(testToken)).thenReturn(1)
        assertThatThrownBy { execution.checkpoint(testToken) }.isInstanceOf(ExecutionLostException::class.java)
    }

    @Test
    fun `완료 확인은 후속 Job으로 증가한 문서 epoch와 무관하다`() {
        whenever(store.snapshot(testToken)).thenReturn(snapshot().copy(status = IndexingJobStatus.COMPLETED,
            documentEpoch = 2, leaseUntil = null, workerId = null, outcome = "INDEXED", type = JobType.INDEX))
        execution.checkpoint(testToken, allowCompleted = true)
        verify(store, never()).renew(any())
    }

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
    @org.junit.jupiter.api.Tag("database")
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
