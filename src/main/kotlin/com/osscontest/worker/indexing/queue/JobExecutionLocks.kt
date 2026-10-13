package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.sql.Connection
import java.time.Instant
import java.util.UUID

data class LockedJobExecution(val job: IndexingJobEntity, val documentEpoch: Long, val now: Instant)

/** 잠금 대기 뒤 영속성 컨텍스트의 낡은 엔티티를 재사용하지 않도록 JDBC로 직접 조회한다. */
@Component
class JobExecutionLocks(private val jdbc: NamedParameterJdbcTemplate) {
    /** 문서 → Job 순서로 잠근다. Job이나 문서가 없거나, 잠그는 사이 Job의 document_id가 바뀌었으면 null을 돌려준다. */
    fun lock(jobId: Long): LockedJobExecution? {
        requireTransaction()
        val documentId = jdbc.query("SELECT document_id FROM indexing_job WHERE id = :id", mapOf("id" to jobId)) {
            rs, _ -> rs.getLong("document_id")
        }.firstOrNull() ?: return null
        val epoch = jdbc.query("SELECT execution_epoch FROM document WHERE id = :id FOR UPDATE",
            mapOf("id" to documentId)) { rs, _ -> rs.getLong("execution_epoch") }.firstOrNull() ?: return null
        val job = lockJob(jobId) ?: return null
        if (job.documentId != documentId) return null
        return LockedJobExecution(job, epoch, dbNow())
    }

    fun lockJob(jobId: Long, skipLocked: Boolean = false): IndexingJobEntity? {
        requireTransaction()
        return jdbc.query("SELECT * FROM indexing_job WHERE id = :id FOR UPDATE" +
            if (skipLocked) " SKIP LOCKED" else "", mapOf("id" to jobId), JOB_MAPPER).firstOrNull()
    }

    fun dbNow(): Instant = checkNotNull(jdbc.queryForObject("SELECT clock_timestamp()", emptyMap<String, Any>(),
        RowMapper { rs, _ -> rs.getTimestamp(1).toInstant() }))

    fun requireTransaction() {
        check(TransactionSynchronizationManager.isActualTransactionActive()) { "Execution locks require a write transaction" }
        check(jdbc.jdbcTemplate.execute(ConnectionCallback { it.transactionIsolation }) == Connection.TRANSACTION_READ_COMMITTED) {
            "Execution locks require READ COMMITTED isolation"
        }
    }

    companion object {
        val JOB_MAPPER = RowMapper { rs, _ ->
            IndexingJobEntity(
                id = rs.getLong("id"), sourceEventId = rs.getObject("source_event_id", UUID::class.java),
                documentId = rs.getLong("document_id"),
                documentVersionId = (rs.getObject("document_version_id") as Number?)?.toLong(),
                status = IndexingJobStatus.valueOf(rs.getString("status")), attemptCount = rs.getInt("attempt_count"),
                nextRetryAt = rs.getTimestamp("next_retry_at")?.toInstant(), workerId = rs.getString("worker_id"),
                lastErrorCode = rs.getString("last_error_code"), lastErrorMessage = rs.getString("last_error_message"),
                traceId = rs.getString("trace_id"), phase = rs.getString("phase"),
                startedAt = rs.getTimestamp("started_at")?.toInstant(), completedAt = rs.getTimestamp("completed_at")?.toInstant(),
                updatedAt = rs.getTimestamp("updated_at").toInstant(), jobType = JobType.valueOf(rs.getString("job_type")),
                enqueueSeq = rs.getLong("enqueue_seq"), executionEpoch = rs.getLong("execution_epoch"),
                outcome = rs.getString("outcome"), leaseUntil = rs.getTimestamp("lease_until")?.toInstant(),
            )
        }
    }
}

internal fun IndexingJobEntity.claimed(): ClaimedJob = ClaimedJob(
    ExecutionToken(checkNotNull(id), checkNotNull(workerId), executionEpoch), jobType, sourceEventId,
    documentId, documentVersionId, traceId, attemptCount,
)
