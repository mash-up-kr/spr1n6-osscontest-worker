/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

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

/** 한 트랜잭션에서 후보 하나만 시도한다. 건너뛴 후보는 문서 잠금을 푼 뒤 다음 시도로 넘어간다. */
@Component
class JobClaimTransactions(private val jdbc: NamedParameterJdbcTemplate, private val locks: JobExecutionLocks,
                           private val breaker: CircuitBreaker) {
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun claim(workerId: String, leaseMillis: Long, maxAttempts: Int, excludedDocuments: Set<Long>,
              allowLlm: Boolean, prepared: (ExecutionToken) -> Unit): ClaimAttempt {
        locks.requireTransaction()
        // 잠금 순서는 breaker → 문서 → Job이다. 시험 호출 예약과 claim은 같은 트랜잭션에 묶는다.
        val gate = breaker.lockForClaim()
        val admitted = allowLlm && gate?.available() == true
        val exclusions = if (excludedDocuments.isEmpty()) "" else "AND d.id NOT IN (:excludedDocuments)"
        val params = mapOf("maxAttempts" to maxAttempts, "excludedDocuments" to excludedDocuments, "allowLlm" to admitted)
        val candidate = jdbc.query("""
            SELECT j.id, j.document_id FROM indexing_job j JOIN document d ON d.id = j.document_id
            WHERE $ELIGIBLE $exclusions
            ORDER BY j.enqueue_seq LIMIT 1 FOR UPDATE OF d SKIP LOCKED
        """.trimIndent(), params) { rs, _ -> rs.getLong("id") to rs.getLong("document_id") }.firstOrNull()
            ?: return ClaimAttempt.Empty
        val (jobId, documentId) = candidate
        val job = locks.lockJob(jobId, skipLocked = true) ?: return ClaimAttempt.Skipped(documentId)
        if (job.documentId != documentId) return ClaimAttempt.Skipped(documentId)
        // 두 잠금을 잡은 뒤 별도 문장으로 다시 조회해야 최신 상태·재시도 시각·선행 Job 상태를 본다.
        val eligible = jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM indexing_job j WHERE j.id = :id AND $ELIGIBLE)",
            params + ("id" to jobId), Boolean::class.java) == true
        if (!eligible) return ClaimAttempt.Skipped(documentId)
        val previousEpoch = checkNotNull(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id = :documentId",
            mapOf("documentId" to documentId), Long::class.java))
        val token = ExecutionToken(jobId, workerId, Math.addExact(previousEpoch, 1))
        prepared(token)
        val activeIndex = job.jobType == JobType.INDEX && jdbc.queryForObject(
            "SELECT deleted_at IS NULL FROM document WHERE id=:id", mapOf("id" to documentId), Boolean::class.java) == true
        if (activeIndex && gate!!.state != BreakerState.CLOSED) breaker.reserveForClaim(gate, token, leaseMillis)
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
        // 임베딩을 쓰지 않는 Job(DELETE, 삭제된 문서의 INDEX)은 breaker가 막아도 claim한다.
        // 같은 문서에 먼저 등록된 미종결 Job이 있으면 뒤 Job은 기다린다.
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
