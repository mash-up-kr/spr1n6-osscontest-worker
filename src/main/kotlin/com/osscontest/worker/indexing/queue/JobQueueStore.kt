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

/** All lease decisions use the DB clock; no waiting occurs inside these transactions. */
@Repository
class JobQueueStore(
    private val jdbc: NamedParameterJdbcTemplate,
    private val properties: QueueProperties,
    private val claims: JobClaimTransactions,
    private val locks: JobExecutionLocks,
    private val ownership: JobOwnershipGuard,
) {
    /** Single operational entry point; the caller supplies an owner unique to this claim attempt. */
    fun claim(maxAttempts: Int, ownerId: String, prepared: (ExecutionToken) -> Unit = {}): ClaimedJob? {
        require(ownerId.isNotBlank() && maxAttempts > 0)
        val excluded = mutableSetOf<Long>()
        repeat(CLAIM_CANDIDATE_LIMIT) {
            when (val attempt = claims.claim(ownerId, properties.leaseDuration.toMillis(), maxAttempts, excluded, prepared)) {
                ClaimAttempt.Empty -> return null
                is ClaimAttempt.Acquired -> return attempt.job
                is ClaimAttempt.Skipped -> {
                    // An enclosing caller owns its transaction; it must release that lock before retrying.
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

    fun dbNow(): Instant = checkNotNull(jdbc.queryForObject("SELECT clock_timestamp()", emptyMap<String, Any>(),
        RowMapper { rs, _ -> rs.getTimestamp(1).toInstant() }))

    /** 문서 행을 잠그고 source_event_id가 중복되지 않는 Job을 등록한다. */
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
        // Bound retries against legacy Job-only locks; normal writers lock the document first.
        private const val CLAIM_CANDIDATE_LIMIT = 32
        const val UTC_NOW = "clock_timestamp()"
        fun ExecutionToken.params(): Map<String, Any> = mapOf("id" to jobId, "workerId" to workerId, "epoch" to epoch)
    }
}
