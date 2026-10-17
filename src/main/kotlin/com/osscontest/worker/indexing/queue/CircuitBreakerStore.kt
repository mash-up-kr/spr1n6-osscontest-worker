/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.Instant

/**
 * external_circuit_breaker 행을 읽고 쓴다. 행은 마이그레이션이 미리 넣어 두어야 한다.
 * breaker 쓰기는 문서·Job 행을 잠그지 않는다.
 */
@Repository
class CircuitBreakerStore(private val jdbc: NamedParameterJdbcTemplate) {
    fun snapshot(name: String): BreakerSnapshot = checkNotNull(read(name)) { "Required circuit row missing: $name" }
    fun lockForClaim(name: String): BreakerSnapshot? = read(name, " FOR UPDATE OF c SKIP LOCKED")

    private fun read(name: String, locking: String = ""): BreakerSnapshot? {
        val row = jdbc.query("""
        WITH db_time AS MATERIALIZED (SELECT clock_timestamp() AS db_now)
        SELECT c.*, tick.db_now,
            CASE WHEN c.probe_call_id IS NOT NULL THEN c.probe_call_until
                 WHEN j.status = 'PROCESSING' AND j.worker_id = c.probe_owner
                   AND j.execution_epoch = c.probe_epoch AND d.execution_epoch = c.probe_epoch
                   AND j.lease_until IS NOT NULL THEN GREATEST(c.probe_reserved_until, j.lease_until)
                 ELSE tick.db_now END AS probe_until
        FROM external_circuit_breaker c
        CROSS JOIN db_time tick
        LEFT JOIN indexing_job j ON j.id = c.probe_job_id
        LEFT JOIN document d ON d.id = j.document_id
        WHERE c.name = :name$locking
    """.trimIndent(), mapOf("name" to name)) { rs, _ ->
        BreakerSnapshot(BreakerState.valueOf(rs.getString("state")), rs.getTimestamp("opened_until")?.toInstant(),
            rs.getTimestamp("db_now").toInstant(), rs.getLong("generation"), rs.getString("probe_owner"),
            (rs.getObject("probe_job_id") as Number?)?.toLong(), (rs.getObject("probe_epoch") as Number?)?.toLong(),
            rs.getTimestamp("probe_until")?.toInstant(), rs.getString("probe_call_id"))
        }.firstOrNull()
        // 조회한 DB 시각이 FOR UPDATE 잠금 대기보다 앞설 수 있다. 잠금을 얻은 뒤 DB 시각을 다시 읽는다.
        return if (row != null && locking.isNotEmpty()) snapshot(name) else row
    }

    fun reserve(name: String, row: BreakerSnapshot, token: ExecutionToken, leaseMillis: Long) {
        check(jdbc.update("""
            UPDATE external_circuit_breaker SET state = 'HALF_OPEN', generation = generation + 1,
                probe_owner = :owner, probe_job_id = :job, probe_epoch = :epoch,
                probe_reserved_until = clock_timestamp() + :lease * INTERVAL '1 millisecond',
                probe_call_id = NULL, probe_call_until = NULL, updated_at = clock_timestamp()
            WHERE name = :name AND generation = :generation
        """.trimIndent(), mapOf("name" to name, "generation" to row.generation, "owner" to token.workerId,
            "job" to token.jobId, "epoch" to token.epoch, "lease" to leaseMillis)) == 1)
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun acquire(name: String, token: ExecutionToken, callId: String, callMillis: Long): CallAdmission {
        var row = checkNotNull(read(name, " FOR UPDATE OF c")) { "Required circuit row missing: $name" }
        if (row.state == BreakerState.CLOSED) return CallAdmission.Allowed(CircuitPermit(name, row.generation, callId))
        if (row.owns(token) && row.callId == callId && row.deadline()!!.isAfter(row.now)) {
            return CallAdmission.Allowed(CircuitPermit(name, row.generation, callId, token.workerId))
        }
        if (row.owns(token) && row.callId == null && row.deadline()!!.isAfter(row.now)) {
            // claim에서 시험 호출을 예약해 둔 실행이므로 실제 호출 TTL은 지금부터 센다.
        } else if (row.available()) {
            reserve(name, row, token, callMillis)
            row = snapshot(name)
        } else return CallAdmission.Blocked(Duration.between(row.now, row.deadline()).coerceAtLeast(Duration.ofMillis(1)))
        check(jdbc.update("""
            UPDATE external_circuit_breaker SET probe_call_id = :call,
                probe_call_until = clock_timestamp() + :ttl * INTERVAL '1 millisecond', updated_at = clock_timestamp()
            WHERE name = :name AND generation = :generation AND probe_owner = :owner AND probe_call_id IS NULL
        """.trimIndent(), mapOf("name" to name, "generation" to row.generation, "owner" to token.workerId,
            "call" to callId, "ttl" to callMillis)) == 1)
        return CallAdmission.Allowed(CircuitPermit(name, row.generation, callId, token.workerId))
    }

    fun valid(permit: CircuitPermit): Boolean {
        val row = snapshot(permit.name)
        return row.generation == permit.generation && if (permit.probeOwner == null) row.state == BreakerState.CLOSED
        else row.state == BreakerState.HALF_OPEN && row.probeOwner == permit.probeOwner &&
            row.callId == permit.callId && row.deadline()!!.isAfter(row.now)
    }

    /** 결과 기록과 상태 변경을 한 트랜잭션에 저장한다. 늦게 도착해 반영하지 않은 결과도 기록은 남긴다. */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun record(permit: CircuitPermit, result: CircuitResult, threshold: Int, openMillis: Long,
               rateDeadline: (Instant) -> Instant): BreakerState {
        val row = checkNotNull(read(permit.name, " FOR UPDATE OF c")) { "Required circuit row missing: ${permit.name}" }
        val params = mapOf("name" to permit.name, "call" to permit.callId, "generation" to permit.generation,
            "owner" to permit.probeOwner, "outcome" to result.outcome.name)
        // 이미 기록된 호출 결과 확인
        val receipt = jdbc.query("""SELECT outcome, resulting_state FROM external_circuit_result
            WHERE breaker_name = :name AND call_id = :call""", params) { rs, _ -> rs.getString(1) to rs.getString(2) }.firstOrNull()
        if (receipt != null) {
            check(receipt.first == result.outcome.name) { "Conflicting circuit result for ${permit.callId}" }
            // 기록이 있으면 이미 처리한 호출이다. 기록된 resulting_state는 과거 값이므로 현재 상태를 돌려준다.
            return row.state
        }
        // 허가 유효성 검증
        var accepted = row.generation == permit.generation && if (permit.probeOwner == null) row.state == BreakerState.CLOSED
            else row.state == BreakerState.HALF_OPEN && row.probeOwner == permit.probeOwner && row.callId == permit.callId &&
                row.deadline()!!.isAfter(row.now)
        // 다음 상태와 열림 기한 계산 및 반영
        if (accepted) {
            if (result.outcome == CircuitOutcome.RATE_LIMIT && result.retryAt == null) result.retryAt = rateDeadline(row.now)
            val probe = permit.probeOwner != null
            val next = when (result.outcome) {
                CircuitOutcome.SUCCESS -> BreakerState.CLOSED
                CircuitOutcome.FAILURE, CircuitOutcome.CONFIGURATION -> {
                    val count = jdbc.queryForObject("SELECT consecutive_failures FROM external_circuit_breaker WHERE name=:name", params, Int::class.java)!!
                    if (probe || result.outcome == CircuitOutcome.CONFIGURATION || count + 1 >= threshold) BreakerState.OPEN else BreakerState.CLOSED
                }
                CircuitOutcome.RATE_LIMIT, CircuitOutcome.PERMANENT -> if (probe) BreakerState.OPEN else row.state
            }
            val deadline = when (result.outcome) {
                CircuitOutcome.RATE_LIMIT -> result.retryAt
                CircuitOutcome.PERMANENT -> row.now // 입력 문제라 API 상태를 알 수 없다. 다른 Job이 바로 시험 호출하게 한다
                else -> row.now.plusMillis(openMillis)
            }
            accepted = jdbc.update("""
                UPDATE external_circuit_breaker SET state = :state,
                    generation = generation + CASE WHEN state <> :state OR :probe THEN 1 ELSE 0 END,
                    consecutive_failures = CASE WHEN :outcome = 'SUCCESS' THEN 0
                        WHEN :outcome IN ('FAILURE','CONFIGURATION') THEN consecutive_failures + 1 ELSE consecutive_failures END,
                    opened_until = CASE WHEN :state = 'OPEN' THEN :deadline ELSE NULL END,
                    probe_owner = NULL, probe_job_id = NULL, probe_epoch = NULL, probe_reserved_until = NULL,
                    probe_call_id = NULL, probe_call_until = NULL, updated_at = clock_timestamp()
                WHERE name = :name AND generation = :generation
                  AND ((NOT :probe AND state = 'CLOSED') OR
                       (:probe AND state = 'HALF_OPEN' AND probe_owner = :owner
                        AND probe_call_id = :call AND probe_call_until > clock_timestamp()))
            """.trimIndent(), params + mapOf("state" to next.name, "probe" to probe,
                "deadline" to deadline?.atOffset(java.time.ZoneOffset.UTC))) == 1
        }
        // 결과 기록 저장
        val resulting = snapshot(permit.name).state
        jdbc.update("""INSERT INTO external_circuit_result
            (breaker_name, call_id, generation, probe_owner, outcome, applied, resulting_state)
            VALUES (:name,:call,:generation,:owner,:outcome,:applied,:state)""",
            params + mapOf("applied" to accepted, "state" to resulting.name))
        return resulting
    }

    /**
     * 결과를 기록하지 못하고 끝난 시험 호출을 반납한다.
     * 다른 Worker가 바로 시험 호출을 하도록 열림 기한을 현재 시각으로 둔다.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun release(name: String, token: ExecutionToken, callId: String) {
        jdbc.update("""UPDATE external_circuit_breaker SET state='OPEN', generation=generation+1,
            opened_until=clock_timestamp(), probe_owner=NULL, probe_job_id=NULL, probe_epoch=NULL,
            probe_reserved_until=NULL, probe_call_id=NULL, probe_call_until=NULL, updated_at=clock_timestamp()
            WHERE name=:name AND probe_owner=:owner AND probe_job_id=:job AND probe_epoch=:epoch
              AND (probe_call_id IS NULL OR probe_call_id=:call)""", mapOf("name" to name,
            "owner" to token.workerId, "job" to token.jobId, "epoch" to token.epoch, "call" to callId))
    }
}
