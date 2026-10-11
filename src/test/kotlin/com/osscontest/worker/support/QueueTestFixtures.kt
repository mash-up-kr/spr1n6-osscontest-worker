package com.osscontest.worker.support

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.queue.DatabaseOperations
import com.osscontest.worker.indexing.queue.ExecutionOperations
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.LockedJobExecution
import com.osscontest.worker.indexing.queue.JobQueueStore
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

val testNow: Instant = Instant.parse("2026-10-10T00:00:00Z")
val testToken = ExecutionToken(1, "worker", 1)

private val fixtureClaimIds = AtomicLong()

/** Fixture convenience only; every claim still uses the production API and a distinct owner. */
fun JobQueueStore.claimFixture(maxAttempts: Int = 5, prepared: (ExecutionToken) -> Unit = {}) =
    claim(maxAttempts, "fixture-${fixtureClaimIds.updateAndGet { Math.incrementExact(it) }}", prepared)

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

/** Retry orchestration without opening even an embedded DB; not a commit simulation. */
fun retryingDatabase(): DatabaseOperations = mock<DatabaseOperations>().apply {
    whenever(execute<Any?>(any(), any())).thenAnswer {
        val action = it.getArgument<() -> Any?>(1)
        var result: Any? = null
        var done = false
        while (!done) {
            try { result = action(); done = true }
            catch (error: Exception) { if (!DatabaseOperations.isUnavailable(error)) throw error }
        }
        result
    }
}

fun directExecution(): ExecutionOperations = mock<ExecutionOperations>().apply {
    whenever(completion<Any?>(any(), any(), any(), any())).thenAnswer { it.getArgument<() -> Any?>(3)() }
    whenever(failure<Any?>(any(), any())).thenAnswer { it.getArgument<() -> Any?>(1)() }
    whenever(database<Any?>(any(), any())).thenAnswer { it.getArgument<() -> Any?>(1)() }
}

fun lockedJob(job: IndexingJobEntity = processingJob(), documentEpoch: Long = job.executionEpoch,
              now: Instant = testNow) = LockedJobExecution(job, documentEpoch, now)

/** Closed breaker without DB reads for fixtures testing other Worker contracts. */
fun closedCircuitBreaker() = mock<com.osscontest.worker.indexing.queue.CircuitBreaker>().apply {
    whenever(admission()).thenReturn(com.osscontest.worker.indexing.queue.ClaimAdmission.NORMAL)
    whenever(acquire()).thenReturn(java.time.Duration.ZERO)
}
