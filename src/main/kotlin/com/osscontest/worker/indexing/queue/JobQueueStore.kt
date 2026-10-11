package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.Instant
import java.util.UUID

/** All lease decisions use the DB clock; no waiting occurs inside these transactions. */
@Repository
class JobQueueStore(private val jdbc: NamedParameterJdbcTemplate, private val properties: QueueProperties) {
    private val jobMapper = RowMapper { rs, _ ->
        ClaimedJob(
            ExecutionToken(rs.getLong("id"), rs.getString("worker_id"), rs.getLong("execution_epoch")),
            JobType.valueOf(rs.getString("job_type")),
            rs.getObject("source_event_id", UUID::class.java),
            rs.getLong("document_id"),
            (rs.getObject("document_version_id") as Number?)?.toLong(),
            rs.getString("trace_id"),
            rs.getInt("attempt_count"),
        )
    }

    /** [onlyType]을 주면 그 종류의 Job만 후보로 본다. 같은 문서의 선행 Job 순서 조건은 그대로 지킨다. */
    @Transactional
    fun claim(maxAttempts: Int, onlyType: JobType? = null, prepared: (ExecutionToken) -> Unit): ClaimedJob? =
        claimForWorker(properties.workerId, properties.leaseDuration.toMillis(), maxAttempts, onlyType, prepared)

    @Transactional
    fun claimForWorker(workerId: String, leaseMillis: Long, maxAttempts: Int, onlyType: JobType? = null,
                       prepared: (ExecutionToken) -> Unit = {}): ClaimedJob? {
        require(leaseMillis > 0 && maxAttempts > 0)
        // 종류 값은 enum 이름이라 문자열 보간이 안전하다.
        val typeFilter = onlyType?.let { "AND j.job_type = '${it.name}'" }.orEmpty()
        val candidates = jdbc.query(
            """
            SELECT j.id, d.execution_epoch FROM indexing_job j
            JOIN document d ON d.id = j.document_id
            WHERE j.worker_id IS NULL AND j.lease_until IS NULL
              AND j.attempt_count < :maxAttempts
              AND (j.status = 'PENDING' OR (j.status = 'RETRY_WAIT' AND j.next_retry_at <= $UTC_NOW))
              $typeFilter
              AND NOT EXISTS (
                  SELECT 1 FROM indexing_job predecessor
                  WHERE predecessor.document_id = j.document_id AND predecessor.enqueue_seq < j.enqueue_seq
                    AND predecessor.status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT')
              )
            ORDER BY j.enqueue_seq
            LIMIT 1 FOR UPDATE OF j, d SKIP LOCKED
            """.trimIndent(), mapOf("maxAttempts" to maxAttempts),
        ) { rs, _ -> rs.getLong("id") to rs.getLong("execution_epoch") }
        val candidate = candidates.firstOrNull() ?: return null
        val token = ExecutionToken(candidate.first, workerId, candidate.second + 1)
        // Capture before UPDATE/COMMIT so a lost response resolves this exact generation.
        prepared(token)
        check(jdbc.update(
            """
            WITH bumped AS (
                UPDATE document d SET execution_epoch = :epoch
                FROM indexing_job j WHERE j.id = :id AND d.id = j.document_id
                  AND d.execution_epoch = :previousEpoch
                RETURNING d.id
            )
            UPDATE indexing_job j SET status = 'PROCESSING', worker_id = :workerId,
                execution_epoch = :epoch, lease_until = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond',
                attempt_count = attempt_count + 1, next_retry_at = NULL,
                started_at = COALESCE(started_at, $UTC_NOW), updated_at = $UTC_NOW
            FROM bumped b WHERE j.id = :id AND j.document_id = b.id AND j.worker_id IS NULL
            """.trimIndent(), token.params() + mapOf("previousEpoch" to candidate.second, "leaseMillis" to leaseMillis),
        ) == 1)
        return resolveClaim(token)
    }

    fun resolveClaim(token: ExecutionToken): ClaimedJob? = jdbc.query(
        "SELECT * FROM indexing_job WHERE id = :id AND worker_id = :workerId AND execution_epoch = :epoch " +
            "AND status = 'PROCESSING'",
        token.params(), jobMapper,
    ).firstOrNull()

    fun snapshot(token: ExecutionToken): JobSnapshot? = jdbc.query(
        """
        SELECT worker_id, execution_epoch, status, lease_until,
               next_retry_at AS retry_at, clock_timestamp() AS db_now
        FROM indexing_job WHERE id = :id
        """.trimIndent(), token.params(),
    ) { rs, _ ->
        JobSnapshot(rs.getString("worker_id"), rs.getLong("execution_epoch"),
            IndexingJobStatus.valueOf(rs.getString("status")), rs.getTimestamp("lease_until")?.toInstant(),
            rs.getTimestamp("retry_at")?.toInstant(), rs.getTimestamp("db_now").toInstant())
    }.firstOrNull()

    fun renew(token: ExecutionToken): Int = jdbc.update(
        """
        UPDATE indexing_job SET lease_until = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond',
            updated_at = $UTC_NOW
        WHERE id = :id AND worker_id = :workerId AND execution_epoch = :epoch
          AND status = 'PROCESSING' AND lease_until IS NOT NULL
        """.trimIndent(), token.params() + ("leaseMillis" to properties.leaseDuration.toMillis()),
    )

    fun dbNow(): Instant = checkNotNull(jdbc.queryForObject("SELECT clock_timestamp()", emptyMap<String, Any>(),
        RowMapper { rs, _ -> rs.getTimestamp(1).toInstant() }))

    @Transactional
    fun recover(maxAttempts: Int, baseDelay: Duration): Long {
        require(maxAttempts > 0 && baseDelay.toMillis() > 0)
        val recovered = checkNotNull(jdbc.queryForObject("SELECT recover_expired_indexing_jobs(:maxAttempts, :baseMillis)",
            mapOf("maxAttempts" to maxAttempts, "baseMillis" to baseDelay.toMillis()), Long::class.java))
        jdbc.update(
            """
            UPDATE indexing_job j SET status = 'FAILED', completed_at = $UTC_NOW, updated_at = $UTC_NOW,
                last_error_code = 'MAX_ATTEMPTS_EXCEEDED', next_retry_at = NULL,
                worker_id = NULL, lease_until = NULL
            WHERE worker_id IS NULL AND status IN ('PENDING', 'RETRY_WAIT') AND attempt_count >= :maxAttempts
            """.trimIndent(), mapOf("maxAttempts" to maxAttempts),
        )
        // Reject deleted documents and late historical retries before they can block successors.
        jdbc.update(
            """
            UPDATE indexing_job j SET status = 'FAILED', completed_at = $UTC_NOW, updated_at = $UTC_NOW,
                last_error_code = 'DOCUMENT_INELIGIBLE', next_retry_at = NULL,
                worker_id = NULL, lease_until = NULL
            WHERE j.job_type = 'INDEX' AND j.status IN ('PENDING','PROCESSING','RETRY_WAIT')
              AND EXISTS (
                SELECT 1 FROM document d JOIN document_version candidate ON candidate.id = j.document_version_id
                WHERE d.id = j.document_id AND (
                    d.deleted_at IS NOT NULL OR candidate.document_id <> d.id OR
                    EXISTS (SELECT 1 FROM document_version searchable
                            WHERE searchable.id = d.searchable_version_id AND searchable.version_no > candidate.version_no) OR
                    EXISTS (SELECT 1 FROM indexing_job prior JOIN document_version v ON v.id = prior.document_version_id
                            WHERE prior.document_id = j.document_id AND prior.enqueue_seq < j.enqueue_seq AND v.version_no > candidate.version_no)
                )
              )
            """.trimIndent(), emptyMap<String, Any>(),
        )
        return recovered
    }

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
        const val UTC_NOW = "clock_timestamp()"
        fun ExecutionToken.params(): Map<String, Any> = mapOf("id" to jobId, "workerId" to workerId, "epoch" to epoch)
    }
}
