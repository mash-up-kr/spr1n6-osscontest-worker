/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobType
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
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
    val attemptCount: Int = 0,
)

data class JobSnapshot(
    val workerId: String?,
    val epoch: Long,
    val status: IndexingJobStatus,
    val leaseUntil: Instant?,
    val retryAt: Instant?,
    val now: Instant,
    val documentEpoch: Long?,
    val outcome: String? = null,
    val type: JobType? = null,
    val documentId: Long? = null,
    val documentVersionId: Long? = null,
    val errorCode: String? = null,
    val errorMessage: String? = null,
) {
    fun matches(token: ExecutionToken) = workerId == token.workerId && epoch == token.epoch
    fun ownsProcessing(token: ExecutionToken) = matches(token) && documentEpoch == token.epoch &&
        status == IndexingJobStatus.PROCESSING && leaseUntil != null
    fun canWrite(token: ExecutionToken) = ownsProcessing(token) && leaseUntil!!.isAfter(now)
    fun completed(token: ExecutionToken) = epoch == token.epoch && status == IndexingJobStatus.COMPLETED &&
        workerId == null && leaseUntil == null && type != null &&
        JobOutcome.entries.any { it.name == outcome && it.completes(type) }
    fun identifies(job: ClaimedJob) = ExecutionIdentity(job).matches(this)
}

/**
 * 잠근 쓰기와 읽기 전용 완료 확인이 같은 실행권 규칙을 쓰도록 엔티티를 스냅샷으로 바꾼다.
 * [now]를 넘기지 않으면 updatedAt을 현재 시각으로 쓰므로 그 결과로 lease 만료([JobSnapshot.canWrite])를 판단하지 않는다.
 */
internal fun IndexingJobEntity.snapshot(documentEpoch: Long? = null, now: Instant = updatedAt) =
    JobSnapshot(workerId, executionEpoch, status, leaseUntil, nextRetryAt, now, documentEpoch,
        outcome, jobType, documentId, documentVersionId, lastErrorCode, lastErrorMessage)

internal fun LockedJobExecution.snapshot() = job.snapshot(documentEpoch, now)

class ExecutionLostException(val token: ExecutionToken) : RuntimeException("Execution no longer owned: $token")
class ExecutionCompletedException(val token: ExecutionToken) : RuntimeException("Execution already completed: $token")
class LeaseExpiredException : RuntimeException("Execution lease expired")
