package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobType
import java.time.Instant
import java.util.UUID

typealias JobType = IndexingJobType

data class ExecutionToken(val jobId: Long, val workerId: String, val epoch: Long)

data class ClaimedJob(
    val token: ExecutionToken,
    val type: JobType,
    val sourceEventId: UUID,
    val documentId: Long,
    val documentVersionId: Long?,
    val traceId: String?,
)

data class JobSnapshot(
    val workerId: String?,
    val epoch: Long,
    val status: IndexingJobStatus,
    val leaseUntil: Instant?,
    val retryAt: Instant?,
    val now: Instant,
) {
    fun matches(token: ExecutionToken) = workerId == token.workerId && epoch == token.epoch
}

class ExecutionLostException(val token: ExecutionToken) : RuntimeException("Execution no longer owned: $token")
class ExecutionCompletedException(val token: ExecutionToken) : RuntimeException("Execution already completed: $token")
class LeaseExpiredException : RuntimeException("Execution lease expired")

internal val ownedStatuses = setOf(IndexingJobStatus.PROCESSING, IndexingJobStatus.RETRY_WAIT)
