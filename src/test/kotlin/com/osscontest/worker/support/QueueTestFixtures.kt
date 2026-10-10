package com.osscontest.worker.support

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.queue.DatabaseOperations
import com.osscontest.worker.indexing.queue.ExecutionOperations
import com.osscontest.worker.indexing.queue.ExecutionToken
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID

val testNow: Instant = Instant.parse("2026-10-10T00:00:00Z")
val testToken = ExecutionToken(1, "worker", 1)

fun processingJob(token: ExecutionToken = testToken, status: IndexingJobStatus = IndexingJobStatus.PROCESSING,
                  attempt: Int = 1) = IndexingJobEntity(
    id = token.jobId, sourceEventId = UUID.randomUUID(), documentId = 2, documentVersionId = 3,
    status = status, attemptCount = attempt, nextRetryAt = null, workerId = token.workerId,
    lastErrorCode = null, lastErrorMessage = null, traceId = null, startedAt = testNow,
    completedAt = null, updatedAt = testNow, executionEpoch = token.epoch, leaseUntil = testNow.plusSeconds(60),
)

fun directDatabase(): DatabaseOperations = mock<DatabaseOperations>().apply {
    whenever(execute<Any?>(any(), any())).thenAnswer { it.getArgument<() -> Any?>(1)() }
}

fun directExecution(): ExecutionOperations = mock<ExecutionOperations>().apply {
    whenever(database<Any?>(any(), any(), any())).thenAnswer { it.getArgument<() -> Any?>(2)() }
}
