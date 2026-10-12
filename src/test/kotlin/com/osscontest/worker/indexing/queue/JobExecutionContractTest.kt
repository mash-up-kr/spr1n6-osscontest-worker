package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.FailureDecision
import com.osscontest.worker.support.lockedJob
import com.osscontest.worker.support.processingJob
import com.osscontest.worker.support.testNow
import com.osscontest.worker.support.testToken
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.mock

class JobExecutionContractTest {
    private val guard = JobOwnershipGuard(mock())

    @ParameterizedTest
    @ValueSource(strings = ["valid", "expired", "deadline", "worker", "job-epoch", "document-epoch", "pending",
        "retry-wait", "completed", "failed", "no-worker", "no-lease"])
    fun `조회와 잠금 가드는 같은 실행권 정책을 적용하고 만료만 heartbeat 예외로 둔다`(condition: String) {
        val job = processingJob()
        var documentEpoch = testToken.epoch
        when (condition) {
            "expired" -> job.leaseUntil = testNow.minusSeconds(1)
            "deadline" -> job.leaseUntil = testNow
            "worker" -> job.workerId = "other"
            "job-epoch" -> job.executionEpoch++
            "document-epoch" -> documentEpoch++
            "pending" -> job.status = IndexingJobStatus.PENDING
            "retry-wait" -> job.status = IndexingJobStatus.RETRY_WAIT
            "completed" -> job.status = IndexingJobStatus.COMPLETED
            "failed" -> job.status = IndexingJobStatus.FAILED
            "no-worker" -> job.workerId = null
            "no-lease" -> job.leaseUntil = null
        }
        val execution = lockedJob(job, documentEpoch)
        val snapshot = execution.snapshot()
        val owned = condition in setOf("valid", "expired", "deadline")
        val writable = condition == "valid"
        assertThat(snapshot.ownsProcessing(testToken)).isEqualTo(owned)
        assertThat(guard.owns(execution, testToken)).isEqualTo(owned)
        assertThat(snapshot.canWrite(testToken)).isEqualTo(writable)
        assertThat(guard.canWrite(execution, testToken)).isEqualTo(writable)
    }

    @ParameterizedTest
    @ValueSource(strings = ["indexed", "skipped", "deleted", "wrong-type", "missing-outcome", "unknown-outcome",
        "retry-outcome", "wrong-epoch", "owner-retained", "lease-retained", "processing", "failed"])
    fun `완료 증거는 조회와 Entity에서 같은 규칙으로 확인하며 활성 쓰기 권한을 주지 않는다`(condition: String) {
        val job = processingJob(status = IndexingJobStatus.COMPLETED).apply {
            workerId = null
            leaseUntil = null
            outcome = JobOutcome.INDEXED.name
        }
        when (condition) {
            "skipped" -> job.outcome = JobOutcome.SKIPPED_DOCUMENT_DELETED.name
            "deleted" -> { job.jobType = JobType.DELETE; job.documentVersionId = null; job.outcome = JobOutcome.DELETED.name }
            "wrong-type" -> job.jobType = JobType.DELETE
            "missing-outcome" -> job.outcome = null
            "unknown-outcome" -> job.outcome = "UNKNOWN"
            "retry-outcome" -> job.outcome = JobOutcome.RETRY_SCHEDULED.name
            "wrong-epoch" -> job.executionEpoch++
            "owner-retained" -> job.workerId = testToken.workerId
            "lease-retained" -> job.leaseUntil = testNow.plusSeconds(60)
            "processing" -> job.status = IndexingJobStatus.PROCESSING
            "failed" -> job.status = IndexingJobStatus.FAILED
        }
        val execution = lockedJob(job, documentEpoch = testToken.epoch + 10)
        val expected = condition in setOf("indexed", "skipped", "deleted")
        assertThat(execution.snapshot().completed(testToken)).isEqualTo(expected)
        assertThat(job.isCompletedExecution(testToken)).isEqualTo(expected)
        assertThat(guard.owns(execution, testToken)).isFalse()
        assertThat(guard.canWrite(execution, testToken)).isFalse()
    }

    @Test
    fun `완료와 실패 Entity 증거는 다른 Job의 동일 epoch와 결과를 인정하지 않는다`() {
        val other = processingJob(testToken.copy(jobId = 99), IndexingJobStatus.COMPLETED).apply {
            workerId = null; leaseUntil = null; outcome = JobOutcome.INDEXED.name
        }
        assertThat(other.isCompletedExecution(testToken)).isFalse()
        other.status = IndexingJobStatus.FAILED
        other.outcome = JobOutcome.FAILED_PROCESSING.name
        other.lastErrorCode = "ERROR"
        other.lastErrorMessage = "test"
        val record = FailureRecord(FailureDecision(IndexingJobStatus.FAILED, null), "ERROR", "test")
        assertThat(record.matches(other, testToken)).isFalse()
        assertThat(record.matches(other, testToken.copy(jobId = 99))).isTrue()
    }
}
