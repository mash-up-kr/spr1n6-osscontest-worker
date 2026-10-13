package com.osscontest.worker.indexing.queue

import java.time.Duration
import java.time.Instant

enum class BreakerState { CLOSED, OPEN, HALF_OPEN }
enum class ClaimAdmission { NORMAL, LIMITED, BLOCKED }
enum class CircuitOutcome { SUCCESS, FAILURE, RATE_LIMIT, PERMANENT, CONFIGURATION }

data class BreakerSnapshot(
    val state: BreakerState, val openedUntil: Instant?, val now: Instant,
    val generation: Long = 0, val probeOwner: String? = null, val probeJobId: Long? = null,
    val probeEpoch: Long? = null, val probeUntil: Instant? = null, val callId: String? = null,
) {
    fun deadline() = if (state == BreakerState.HALF_OPEN) probeUntil else openedUntil
    fun available() = state == BreakerState.CLOSED || deadline()?.let { !it.isAfter(now) } == true
    fun owns(token: ExecutionToken) = probeOwner == token.workerId && probeJobId == token.jobId && probeEpoch == token.epoch
}

data class CircuitPermit(val name: String, val generation: Long, val callId: String, val probeOwner: String? = null)
sealed interface CallAdmission {
    data class Allowed(val permit: CircuitPermit) : CallAdmission
    data class Blocked(val delay: Duration) : CallAdmission
}

/** 롤백이나 커밋 응답 유실 뒤 같은 객체로 다시 기록하므로, 한 번 계산한 [retryAt]이 바뀌지 않는다. */
class CircuitResult(val outcome: CircuitOutcome, val attempt: Int = 1, val retryAfter: String? = null) {
    var retryAt: Instant? = null
}
