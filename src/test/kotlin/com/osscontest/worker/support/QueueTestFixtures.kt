package com.osscontest.worker.support

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.queue.DatabaseOperations
import com.osscontest.worker.indexing.queue.ExecutionOperations
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.LockedJobExecution
import com.osscontest.worker.indexing.queue.JobQueueStore
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

val testNow: Instant = Instant.parse("2026-10-10T00:00:00Z")
val testToken = ExecutionToken(1, "worker", 1)

private val fixtureClaimIds = AtomicLong()

/** fixture 편의 함수다. 모든 claim은 운영 API와 서로 다른 owner를 쓴다. */
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

/** 내장 DB 없이 재시도 흐름만 실행한다. 커밋을 흉내 내지 않는다. */
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

/** 공유 breaker SQL이 아니라 큐·파이프라인을 검증하는 테스트용 닫힌 breaker다. */
fun closedCircuitBreaker() = mock<com.osscontest.worker.indexing.queue.CircuitBreaker>().also { allowCircuitCalls(it) }

fun allowCircuitCalls(breaker: com.osscontest.worker.indexing.queue.CircuitBreaker) {
    // 공유 mock은 다른 스레드가 호출하는 중에도 스텁하므로 메서드를 호출하지 않는 doReturn 형태를 쓴다.
    doReturn(com.osscontest.worker.indexing.queue.ClaimAdmission.NORMAL).whenever(breaker).admission()
    doReturn(com.osscontest.worker.indexing.queue.BreakerSnapshot(
        com.osscontest.worker.indexing.queue.BreakerState.CLOSED, null, testNow)).whenever(breaker).lockForClaim()
    doAnswer {
        com.osscontest.worker.indexing.queue.CallAdmission.Allowed(com.osscontest.worker.indexing.queue.CircuitPermit(
            "embedding-api", 0, it.getArgument<String>(1)))
    }.whenever(breaker).acquire(any(), any())
    doReturn(true).whenever(breaker).valid(any())
}
