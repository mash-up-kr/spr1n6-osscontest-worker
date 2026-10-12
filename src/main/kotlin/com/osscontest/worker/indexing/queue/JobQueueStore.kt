package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.time.Instant
import java.util.UUID

/** lease 판단은 모두 DB 시각으로 한다. 트랜잭션 안에서는 행 잠금 대기 외의 sleep이나 재시도 대기를 하지 않는다. */
@Repository
class JobQueueStore(
    private val jdbc: NamedParameterJdbcTemplate,
    private val properties: QueueProperties,
    private val claims: JobClaimTransactions,
    private val locks: JobExecutionLocks,
    private val ownership: JobOwnershipGuard,
    private val breaker: CircuitBreaker,
) {
    /** 운영 claim의 유일한 진입점이다. 호출자는 claim 시도마다 고유한 [ownerId]를 넘겨야 한다. */
    fun claim(maxAttempts: Int, ownerId: String, prepared: (ExecutionToken) -> Unit = {}): ClaimedJob? {
        require(ownerId.isNotBlank() && maxAttempts > 0)
        val excluded = mutableSetOf<Long>()
        repeat(CLAIM_CANDIDATE_LIMIT) {
            // admission은 후보를 거르기만 한다. 걸러진 INDEX Job도 같은 문서의 뒤 Job을 계속 막는다.
            val allowLlm = breaker.admission() != ClaimAdmission.BLOCKED
            when (val attempt = claims.claim(ownerId, properties.leaseDuration.toMillis(), maxAttempts, excluded,
                allowLlm, prepared)) {
                ClaimAttempt.Empty -> return null
                is ClaimAttempt.Acquired -> return attempt.job
                is ClaimAttempt.Skipped -> {
                    // 바깥 트랜잭션 안에서 불렸으면 그 트랜잭션이 끝나야 문서 잠금이 풀리므로 여기서 멈춘다.
                    if (TransactionSynchronizationManager.isActualTransactionActive()) return null
                    excluded.add(attempt.documentId)
                }
            }
        }
        return null
    }

    fun resolveClaim(token: ExecutionToken): ClaimedJob? = jdbc.query(
        """
        SELECT j.* FROM indexing_job j WHERE ${JobOwnershipSql.OWNED_PROCESSING}
        """.trimIndent(), token.params(), JobExecutionLocks.JOB_MAPPER,
    ).firstOrNull()?.claimed()

    fun snapshot(token: ExecutionToken): JobSnapshot? = jdbc.query(
        """
        SELECT j.worker_id, j.execution_epoch, j.status, j.lease_until, d.execution_epoch AS document_epoch,
               j.next_retry_at AS retry_at, clock_timestamp() AS db_now, j.outcome, j.job_type,
               j.document_id, j.document_version_id, j.last_error_code, j.last_error_message
        FROM indexing_job j LEFT JOIN document d ON d.id = j.document_id WHERE j.id = :id
        """.trimIndent(), token.params(),
    ) { rs, _ ->
        JobSnapshot(rs.getString("worker_id"), rs.getLong("execution_epoch"),
            IndexingJobStatus.valueOf(rs.getString("status")), rs.getTimestamp("lease_until")?.toInstant(),
            rs.getTimestamp("retry_at")?.toInstant(), rs.getTimestamp("db_now").toInstant(),
            (rs.getObject("document_epoch") as Number?)?.toLong(), rs.getString("outcome"),
            JobType.valueOf(rs.getString("job_type")), rs.getLong("document_id"),
            (rs.getObject("document_version_id") as Number?)?.toLong(),
            rs.getString("last_error_code"), rs.getString("last_error_message"))
    }.firstOrNull()

    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun renew(token: ExecutionToken): Int {
        val execution = locks.lock(token.jobId) ?: return 0
        if (!ownership.owns(execution, token)) return 0
        return jdbc.update(
            """
            UPDATE indexing_job j SET lease_until = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond',
                updated_at = clock_timestamp()
            WHERE ${JobOwnershipSql.OWNED_PROCESSING}
            """.trimIndent(), token.params() + ("leaseMillis" to properties.leaseDuration.toMillis()),
        )
    }

    /** 통합 테스트가 DB 시각 기준으로 재시도 시각을 만들 때 쓴다. 운영 경로는 [JobExecutionLocks]의 시각을 쓴다. */
    fun dbNow(): Instant = checkNotNull(jdbc.queryForObject("SELECT clock_timestamp()", emptyMap<String, Any>(),
        RowMapper { rs, _ -> rs.getTimestamp(1).toInstant() }))

    /**
     * 문서 행을 잠그고 Job을 등록한다. 같은 source_event_id가 이미 있으면 아무것도 하지 않는다.
     * 등록 SQL과 통합 테스트의 진입점이며, 운영 중 Job은 Server가 등록한다.
     */
    @Transactional
    fun enqueue(sourceEventId: UUID, documentId: Long, documentVersionId: Long?, type: JobType, traceId: String?) {
        check(jdbc.queryForObject("SELECT id FROM document WHERE id = :id FOR UPDATE",
            mapOf("id" to documentId), Long::class.java) != null)
        jdbc.update(
            """
            INSERT INTO indexing_job (source_event_id, document_id, document_version_id, job_type, status,
                attempt_count, execution_epoch, updated_at, trace_id)
            VALUES (:eventId, :documentId, :documentVersionId, :type, 'PENDING', 0, 0, $UTC_NOW,
                :traceId) ON CONFLICT DO NOTHING
            """.trimIndent(), mapOf("eventId" to sourceEventId, "documentId" to documentId, "documentVersionId" to documentVersionId,
                "type" to type.name, "traceId" to traceId),
        )
    }

    companion object {
        // Job 행만 잠그는 과거 Writer와 겹칠 때 후보 재시도를 끝내는 상한이다. 정상 Writer는 문서를 먼저 잠근다.
        // 넘으면 이번 claim은 빈손으로 끝나고 다음 poll에서 다시 찾는다. 짧은 트랜잭션 반복이 끝없이 이어지지 않게 하는 값이다.
        private const val CLAIM_CANDIDATE_LIMIT = 32
        const val UTC_NOW = "clock_timestamp()"
        fun ExecutionToken.params(): Map<String, Any> = mapOf("id" to jobId, "workerId" to workerId, "epoch" to epoch)
    }
}
