package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
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
        )
    }

    @Transactional
    fun claim(retryOnly: Boolean, maxAttempts: Int, prepared: (ExecutionToken) -> Unit): ClaimedJob? {
        val candidates = jdbc.query(
            """
            SELECT j.* FROM indexing_job j
            WHERE j.worker_id IS NULL AND j.lease_expires_at IS NULL
              AND j.attempt_count < :maxAttempts
              AND (j.status = 'PENDING' OR (j.status = 'RETRY_WAIT' AND j.next_retry_at <= $UTC_NOW))
              AND NOT EXISTS (
                  SELECT 1 FROM indexing_job predecessor
                  WHERE predecessor.document_id = j.document_id AND predecessor.id < j.id
                    AND predecessor.status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT')
              )
              AND (j.next_retry_at IS NOT NULL OR (
                  NOT :retryOnly AND NOT EXISTS (
                      SELECT 1 FROM indexing_job retry
                      WHERE retry.status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT')
                        AND retry.next_retry_at IS NOT NULL
                  )
              ))
            ORDER BY (j.next_retry_at IS NOT NULL) DESC, j.id
            LIMIT 1 FOR UPDATE OF j SKIP LOCKED
            """.trimIndent(),
            mapOf("maxAttempts" to maxAttempts, "retryOnly" to retryOnly),
        ) { rs, _ -> rs.getLong("id") to rs.getLong("execution_epoch") }
        val candidate = candidates.firstOrNull() ?: return null
        val token = ExecutionToken(candidate.first, properties.workerId, candidate.second + 1)
        // Capture before UPDATE/COMMIT. A lost commit response can resolve this exact generation.
        prepared(token)
        jdbc.update(
            """
            UPDATE indexing_job SET status = 'PROCESSING', worker_id = :workerId,
                execution_epoch = :epoch, lease_expires_at = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond',
                attempt_count = attempt_count + 1, started_at = COALESCE(started_at, $UTC_NOW), updated_at = $UTC_NOW
            WHERE id = :id AND worker_id IS NULL AND execution_epoch = :previousEpoch
            """.trimIndent(),
            token.params() + mapOf("previousEpoch" to candidate.second, "leaseMillis" to properties.leaseDuration.toMillis()),
        ).also { check(it == 1) }
        return resolveClaim(token)
    }

    fun resolveClaim(token: ExecutionToken): ClaimedJob? = jdbc.query(
        "SELECT * FROM indexing_job WHERE id = :id AND worker_id = :workerId AND execution_epoch = :epoch " +
            "AND status IN ('PROCESSING', 'RETRY_WAIT')",
        token.params(), jobMapper,
    ).firstOrNull()

    fun snapshot(token: ExecutionToken): JobSnapshot? = jdbc.query(
        """
        SELECT worker_id, execution_epoch, status, lease_expires_at,
               next_retry_at AS retry_at, clock_timestamp() AS db_now
        FROM indexing_job WHERE id = :id
        """.trimIndent(), token.params(),
    ) { rs, _ ->
        JobSnapshot(rs.getString("worker_id"), rs.getLong("execution_epoch"),
            IndexingJobStatus.valueOf(rs.getString("status")), rs.getTimestamp("lease_expires_at")?.toInstant(),
            rs.getTimestamp("retry_at")?.toInstant(), rs.getTimestamp("db_now").toInstant())
    }.firstOrNull()

    fun renew(token: ExecutionToken): Int = jdbc.update(
        """
        UPDATE indexing_job SET lease_expires_at = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond',
            updated_at = $UTC_NOW
        WHERE id = :id AND worker_id = :workerId AND execution_epoch = :epoch
          AND status IN ('PROCESSING', 'RETRY_WAIT') AND lease_expires_at IS NOT NULL
        """.trimIndent(), token.params() + ("leaseMillis" to properties.leaseDuration.toMillis()),
    )

    fun resumeRetry(token: ExecutionToken, maxAttempts: Int): Int = jdbc.update(
        """
        UPDATE indexing_job SET status = 'PROCESSING', attempt_count = attempt_count + 1, updated_at = $UTC_NOW
        WHERE id = :id AND worker_id = :workerId AND execution_epoch = :epoch
          AND status = 'RETRY_WAIT' AND next_retry_at <= $UTC_NOW
          AND lease_expires_at > clock_timestamp() AND attempt_count < :maxAttempts
        """.trimIndent(), token.params() + ("maxAttempts" to maxAttempts),
    )

    fun dbNow(): Instant = checkNotNull(jdbc.queryForObject("SELECT clock_timestamp()", emptyMap<String, Any>(),
        RowMapper { rs, _ -> rs.getTimestamp(1).toInstant() }))

    fun cooldown(): Instant? = jdbc.queryForObject(
        """
        SELECT MAX(next_retry_at) FROM indexing_job
        WHERE status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT') AND last_error_code = 'RATE_LIMITED'
          AND next_retry_at > $UTC_NOW
        """.trimIndent(), emptyMap<String, Any>(), RowMapper { rs, _ -> rs.getTimestamp(1)?.toInstant() },
    )

    fun hasRetries(): Boolean = jdbc.queryForObject(
        "SELECT EXISTS (SELECT 1 FROM indexing_job WHERE status IN ('PENDING','PROCESSING','RETRY_WAIT') " +
            "AND next_retry_at IS NOT NULL)", emptyMap<String, Any>(), Boolean::class.java,
    ) == true

    @Transactional
    fun recover(maxAttempts: Int): Long {
        val recovered = checkNotNull(jdbc.queryForObject("SELECT recover_expired_indexing_jobs()",
            emptyMap<String, Any>(), Long::class.java))
        jdbc.update(
            """
            UPDATE indexing_job j SET status = 'FAILED', completed_at = $UTC_NOW, updated_at = $UTC_NOW,
                last_error_code = 'MAX_ATTEMPTS_EXCEEDED', next_retry_at = NULL,
                execution_epoch = execution_epoch + 1
            WHERE worker_id IS NULL AND status IN ('PENDING', 'RETRY_WAIT') AND attempt_count >= :maxAttempts
            """.trimIndent(), mapOf("maxAttempts" to maxAttempts),
        )
        // Reject deleted documents and late historical retries before they can block successors.
        jdbc.update(
            """
            UPDATE indexing_job j SET status = 'FAILED', completed_at = $UTC_NOW, updated_at = $UTC_NOW,
                last_error_code = 'DOCUMENT_INELIGIBLE', next_retry_at = NULL,
                worker_id = NULL, lease_expires_at = NULL, execution_epoch = execution_epoch + 1
            WHERE j.job_type = 'INDEX' AND j.status IN ('PENDING','PROCESSING','RETRY_WAIT')
              AND EXISTS (
                SELECT 1 FROM document d JOIN document_version candidate ON candidate.id = j.document_version_id
                WHERE d.id = j.document_id AND (
                    d.deleted_at IS NOT NULL OR candidate.document_id <> d.id OR
                    EXISTS (SELECT 1 FROM document_version searchable
                            WHERE searchable.id = d.searchable_version_id AND searchable.version_no > candidate.version_no) OR
                    EXISTS (SELECT 1 FROM indexing_job prior JOIN document_version v ON v.id = prior.document_version_id
                            WHERE prior.document_id = j.document_id AND prior.id < j.id AND v.version_no > candidate.version_no)
                )
              )
            """.trimIndent(), emptyMap<String, Any>(),
        )
        return recovered
    }

    /** Optional Kafka ingress uses durable Jobs; it never executes or directly deletes chunks. */
    @Transactional
    fun enqueue(sourceEventId: UUID, documentId: Long, versionId: Long?, type: JobType, traceId: String?,
                topic: String = "db-job-queue", partition: Int = 0, offset: Long = 0) {
        check(jdbc.queryForObject("SELECT id FROM document WHERE id = :id FOR UPDATE",
            mapOf("id" to documentId), Long::class.java) != null)
        jdbc.update(
            """
            INSERT INTO indexing_job (source_event_id, document_id, document_version_id, job_type, status,
                attempt_count, execution_epoch, updated_at, trace_id, kafka_topic, kafka_partition, kafka_offset)
            VALUES (:eventId, :documentId, :versionId, :type, 'PENDING', 0, 0, $UTC_NOW,
                :traceId, :topic, :partition, :offset) ON CONFLICT DO NOTHING
            """.trimIndent(), mapOf("eventId" to sourceEventId, "documentId" to documentId, "versionId" to versionId,
                "type" to type.name, "traceId" to traceId, "topic" to topic, "partition" to partition, "offset" to offset),
        )
    }

    companion object {
        const val UTC_NOW = "clock_timestamp()"
        fun ExecutionToken.params(): Map<String, Any> = mapOf("id" to jobId, "workerId" to workerId, "epoch" to epoch)
    }
}
