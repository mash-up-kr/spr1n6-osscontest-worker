package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.JobExecutionLocks
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional
import java.sql.Types
import java.time.Instant
import java.time.ZoneOffset

@Repository
class IndexingJobWritesImpl(
    private val jdbc: NamedParameterJdbcTemplate,
    private val locks: JobExecutionLocks,
    private val ownership: JobOwnershipGuard,
) : IndexingJobWrites {
    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun complete(jobId: Long, workerId: String, epoch: Long): Int = write(ExecutionToken(jobId, workerId, epoch),
        "status = 'COMPLETED', completed_at = clock_timestamp(), next_retry_at = NULL, lease_until = NULL")

    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun updatePhase(jobId: Long, phase: String, workerId: String, epoch: Long): Int =
        write(ExecutionToken(jobId, workerId, epoch), "phase = :phase", MapSqlParameterSource("phase", phase))

    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun fail(jobId: Long, workerId: String, epoch: Long, status: String, retryAt: Instant?,
                      errorCode: String, errorMessage: String): Int {
        require(status in setOf("RETRY_WAIT", "FAILED"))
        require((status == "RETRY_WAIT") == (retryAt != null))
        return write(ExecutionToken(jobId, workerId, epoch), """
            status = :status, last_error_code = :errorCode, last_error_message = :errorMessage,
            next_retry_at = :retryAt,
            completed_at = CASE WHEN :status = 'FAILED' THEN clock_timestamp() ELSE NULL END,
            worker_id = NULL, lease_until = NULL
        """.trimIndent(), MapSqlParameterSource().addValue("status", status)
            .addValue("retryAt", retryAt?.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
            .addValue("errorCode", errorCode).addValue("errorMessage", errorMessage))
    }

    private fun write(token: ExecutionToken, assignments: String, params: MapSqlParameterSource = MapSqlParameterSource()): Int {
        val execution = locks.lock(token.jobId) ?: return 0
        if (!ownership.canWrite(execution, token)) return 0
        params.addValue("id", token.jobId).addValue("workerId", token.workerId).addValue("epoch", token.epoch)
        return jdbc.update("""
            UPDATE indexing_job j SET $assignments, updated_at = clock_timestamp()
            WHERE j.id = :id AND j.worker_id = :workerId AND j.execution_epoch = :epoch
              AND j.status = 'PROCESSING' AND j.lease_until > clock_timestamp()
              AND EXISTS (SELECT 1 FROM document d WHERE d.id = j.document_id AND d.execution_epoch = :epoch)
        """.trimIndent(), params)
    }
}
