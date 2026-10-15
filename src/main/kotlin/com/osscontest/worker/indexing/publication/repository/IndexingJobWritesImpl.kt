/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.JobExecutionLocks
import com.osscontest.worker.indexing.queue.JobOutcome
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.JobOwnershipSql
import com.osscontest.worker.indexing.queue.JobType
import com.osscontest.worker.indexing.queue.LockedJobExecution
import java.sql.Types
import java.time.Instant
import java.time.ZoneOffset
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional

@Repository
class IndexingJobWritesImpl(
    private val jdbc: NamedParameterJdbcTemplate,
    private val locks: JobExecutionLocks,
    private val ownership: JobOwnershipGuard,
) : IndexingJobWrites {
    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun complete(jobId: Long, workerId: String, epoch: Long): Int =
        complete(ExecutionToken(jobId, workerId, epoch), null)

    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun complete(jobId: Long, workerId: String, epoch: Long, outcome: JobOutcome): Int =
        complete(ExecutionToken(jobId, workerId, epoch), outcome)

    private fun complete(token: ExecutionToken, outcome: JobOutcome?): Int = write(token) { execution ->
        val resolved = outcome ?: if (execution.job.jobType == JobType.DELETE) JobOutcome.DELETED else JobOutcome.INDEXED
        require(resolved.completes(execution.job.jobType))
        JobUpdate(
            "status = 'COMPLETED', outcome = :outcome, completed_at = clock_timestamp(), " +
                "next_retry_at = NULL, worker_id = NULL, lease_until = NULL", MapSqlParameterSource("outcome", resolved.name))
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun updatePhase(jobId: Long, phase: String, workerId: String, epoch: Long): Int =
        write(ExecutionToken(jobId, workerId, epoch)) { JobUpdate("phase = :phase", MapSqlParameterSource("phase", phase)) }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    override fun fail(jobId: Long, workerId: String, epoch: Long, status: String, retryAt: Instant?,
                      errorCode: String, errorMessage: String): Int {
        require(status in setOf("RETRY_WAIT", "FAILED"))
        require((status == "RETRY_WAIT") == (retryAt != null))
        return write(ExecutionToken(jobId, workerId, epoch)) {
            JobUpdate("""
                status = :status, last_error_code = :errorCode, last_error_message = :errorMessage,
                next_retry_at = :retryAt,
                completed_at = CASE WHEN :status = 'FAILED' THEN clock_timestamp() ELSE NULL END,
                worker_id = NULL, lease_until = NULL,
                outcome = CASE WHEN :status = 'RETRY_WAIT' THEN 'RETRY_SCHEDULED' ELSE 'FAILED_PROCESSING' END
            """.trimIndent(), MapSqlParameterSource().addValue("status", status)
                .addValue("retryAt", retryAt?.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("errorCode", errorCode).addValue("errorMessage", errorMessage))
        }
    }

    private data class JobUpdate(val assignments: String, val params: MapSqlParameterSource)

    private fun write(token: ExecutionToken, update: (LockedJobExecution) -> JobUpdate): Int {
        val execution = locks.lock(token.jobId) ?: return 0
        val (assignments, params) = update(execution)
        if (!ownership.canWrite(execution, token)) return 0
        params.addValue("id", token.jobId).addValue("workerId", token.workerId).addValue("epoch", token.epoch)
        return jdbc.update("""
            UPDATE indexing_job j SET $assignments, updated_at = clock_timestamp()
            WHERE ${JobOwnershipSql.WRITABLE_PROCESSING}
        """.trimIndent(), params)
    }
}
