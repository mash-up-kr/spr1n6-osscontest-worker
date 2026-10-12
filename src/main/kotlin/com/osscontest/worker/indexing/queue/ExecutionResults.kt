package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.FailureDecision
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

enum class JobOutcome {
    INDEXED, SKIPPED_DOCUMENT_DELETED, DELETED, RETRY_SCHEDULED, FAILED_PROCESSING;

    fun completes(type: JobType) = when (type) {
        JobType.INDEX -> this == INDEXED || this == SKIPPED_DOCUMENT_DELETED
        JobType.DELETE -> this == DELETED
    }
}

/** A read-only receipt never grants permission to write or renew. */
fun IndexingJobEntity.isCompletedExecution(token: ExecutionToken) = id == token.jobId &&
    snapshot().completed(token)

data class FailureRecord(val decision: FailureDecision, val code: String, val message: String) {
    val confirmed = AtomicBoolean()
    fun matches(snapshot: JobSnapshot, token: ExecutionToken) = snapshot.epoch == token.epoch &&
        snapshot.workerId == null && snapshot.leaseUntil == null && snapshot.status == decision.status &&
        snapshot.retryAt == decision.nextRetryAt && snapshot.errorCode == code && snapshot.errorMessage == message &&
        snapshot.outcome == (if (decision.status == IndexingJobStatus.RETRY_WAIT) JobOutcome.RETRY_SCHEDULED
            else JobOutcome.FAILED_PROCESSING).name

    fun matches(job: IndexingJobEntity, token: ExecutionToken) = job.id == token.jobId && matches(job.snapshot(), token)
}

/** Retains the original decision through rollback, database waits and an unknown commit result. */
@Component
class ExecutionResults {
    private val failures = ConcurrentHashMap<ExecutionToken, FailureRecord>()
    fun prepare(token: ExecutionToken, factory: () -> FailureRecord): FailureRecord = failures.computeIfAbsent(token) { factory() }
    fun failure(token: ExecutionToken) = failures[token]
    fun isOwnFailure(snapshot: JobSnapshot, token: ExecutionToken) = failures[token]?.matches(snapshot, token) == true
    fun forget(token: ExecutionToken) { failures.remove(token) }
}
