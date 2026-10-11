package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.queue.JobQueueStore.Companion.params
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional

sealed interface ClaimAttempt {
    data object Empty : ClaimAttempt
    data class Skipped(val documentId: Long) : ClaimAttempt
    data class Acquired(val job: ClaimedJob) : ClaimAttempt
}

/** One candidate per transaction: skipped candidates release their document lock before another attempt. */
@Component
class JobClaimTransactions(private val jdbc: NamedParameterJdbcTemplate, private val locks: JobExecutionLocks) {
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun claim(workerId: String, leaseMillis: Long, maxAttempts: Int, excludedDocuments: Set<Long>,
              allowLlm: Boolean, prepared: (ExecutionToken) -> Unit): ClaimAttempt {
        locks.requireTransaction()
        val exclusions = if (excludedDocuments.isEmpty()) "" else "AND d.id NOT IN (:excludedDocuments)"
        val params = mapOf("maxAttempts" to maxAttempts, "excludedDocuments" to excludedDocuments, "allowLlm" to allowLlm)
        val candidate = jdbc.query("""
            SELECT j.id, j.document_id FROM indexing_job j JOIN document d ON d.id = j.document_id
            WHERE $ELIGIBLE $exclusions
            ORDER BY j.enqueue_seq LIMIT 1 FOR UPDATE OF d SKIP LOCKED
        """.trimIndent(), params) { rs, _ -> rs.getLong("id") to rs.getLong("document_id") }.firstOrNull()
            ?: return ClaimAttempt.Empty
        val (jobId, documentId) = candidate
        val job = locks.lockJob(jobId, skipLocked = true) ?: return ClaimAttempt.Skipped(documentId)
        if (job.documentId != documentId) return ClaimAttempt.Skipped(documentId)
        // A separate statement under both locks observes fresh state, retry time and predecessor state.
        val eligible = jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM indexing_job j WHERE j.id = :id AND $ELIGIBLE)",
            params + ("id" to jobId), Boolean::class.java) == true
        if (!eligible) return ClaimAttempt.Skipped(documentId)
        val previousEpoch = checkNotNull(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id = :documentId",
            mapOf("documentId" to documentId), Long::class.java))
        val token = ExecutionToken(jobId, workerId, Math.addExact(previousEpoch, 1))
        prepared(token)
        check(jdbc.update("""
            WITH bumped AS (
                UPDATE document SET execution_epoch = :epoch
                WHERE id = :documentId AND execution_epoch = :previousEpoch RETURNING id
            )
            UPDATE indexing_job j SET status = 'PROCESSING', worker_id = :workerId, execution_epoch = :epoch,
                lease_until = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond',
                attempt_count = attempt_count + 1, next_retry_at = NULL, outcome = NULL,
                started_at = COALESCE(started_at, clock_timestamp()), updated_at = clock_timestamp()
            FROM bumped b WHERE j.id = :id AND j.document_id = b.id AND $ELIGIBLE
        """.trimIndent(), params + token.params() + mapOf("documentId" to documentId,
            "previousEpoch" to previousEpoch, "leaseMillis" to leaseMillis)) == 1)
        val claimed = checkNotNull(locks.lockJob(jobId)).claimed()
        return ClaimAttempt.Acquired(claimed)
    }

    companion object {
        private const val ELIGIBLE = """
            j.worker_id IS NULL AND j.lease_until IS NULL AND j.attempt_count < :maxAttempts
            AND (j.status = 'PENDING' OR (j.status = 'RETRY_WAIT' AND j.next_retry_at <= clock_timestamp()))
            AND (:allowLlm OR j.job_type = 'DELETE' OR EXISTS (
                SELECT 1 FROM document admission_document
                WHERE admission_document.id = j.document_id AND admission_document.deleted_at IS NOT NULL
            ))
            AND NOT EXISTS (
                SELECT 1 FROM indexing_job predecessor WHERE predecessor.document_id = j.document_id
                AND predecessor.enqueue_seq < j.enqueue_seq AND predecessor.status IN ('PENDING','PROCESSING','RETRY_WAIT')
            )
        """
    }
}
