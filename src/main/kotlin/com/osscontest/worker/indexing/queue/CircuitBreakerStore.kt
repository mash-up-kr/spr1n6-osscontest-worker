package com.osscontest.worker.indexing.queue

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.time.Instant

enum class BreakerState { CLOSED, OPEN, HALF_OPEN }

data class BreakerSnapshot(val state: BreakerState, val openedUntil: Instant?, val now: Instant)

/**
 * breaker 상태의 모든 전이는 조건부 UPDATE 한 문장이다. 경합하는 Worker 중 한 곳만 이긴다.
 * 시각 판단은 DB 시계만 쓰고, 읽기와 쓰기 모두 큐와 같은 Primary 연결을 쓴다.
 * 행이 없으면 조회는 null, 기록은 0건으로 끝나 호출자가 breaker 없이 동작한다.
 */
@Repository
class CircuitBreakerStore(private val jdbc: NamedParameterJdbcTemplate) {
    fun snapshot(name: String): BreakerSnapshot? = jdbc.query(
        "SELECT state, opened_until, clock_timestamp() AS db_now FROM external_circuit_breaker WHERE name = :name",
        mapOf("name" to name),
    ) { rs, _ ->
        BreakerSnapshot(BreakerState.valueOf(rs.getString("state")),
            rs.getTimestamp("opened_until")?.toInstant(), rs.getTimestamp("db_now").toInstant())
    }.firstOrNull()

    /**
     * 유지 시간이 지난 OPEN(또는 시험 호출자가 사라진 HALF_OPEN)에서 시험 호출권을 얻는다. 얻으면 true.
     * 시험 호출의 유효 시간도 [openMillis]를 쓴다. 호출이 이보다 오래 걸리면 다른 Worker가 두 번째 시험 호출을 얻을 수 있다.
     */
    fun tryStartProbe(name: String, openMillis: Long): Boolean = jdbc.update(
        """
        UPDATE external_circuit_breaker SET state = 'HALF_OPEN',
            opened_until = clock_timestamp() + :openMillis * INTERVAL '1 millisecond', updated_at = clock_timestamp()
        WHERE name = :name AND state IN ('OPEN', 'HALF_OPEN') AND opened_until <= clock_timestamp()
        """.trimIndent(), mapOf("name" to name, "openMillis" to openMillis),
    ) == 1

    /** 바꿀 것이 있을 때만 쓴다(CLOSED이고 카운트가 0이면 0건). */
    fun recordSuccess(name: String): Int = jdbc.update(
        """
        UPDATE external_circuit_breaker SET state = 'CLOSED', consecutive_failures = 0, opened_until = NULL,
            updated_at = clock_timestamp()
        WHERE name = :name AND (state <> 'CLOSED' OR consecutive_failures <> 0)
        """.trimIndent(), mapOf("name" to name),
    )

    /**
     * 일시 실패를 기록하고 갱신 뒤 상태를 돌려준다. CLOSED는 카운트를 올리고 임계치에서 열며,
     * HALF_OPEN은 곧바로 다시 열고, 이미 OPEN이면 바꾸지 않는다. SET의 식은 모두 갱신 전 값을 본다.
     */
    fun recordFailure(name: String, threshold: Int, openMillis: Long): BreakerState? = jdbc.query(
        """
        UPDATE external_circuit_breaker SET
            consecutive_failures = CASE WHEN state = 'CLOSED' THEN consecutive_failures + 1 ELSE consecutive_failures END,
            opened_until = CASE WHEN state = 'HALF_OPEN' OR (state = 'CLOSED' AND consecutive_failures + 1 >= :threshold)
                THEN clock_timestamp() + :openMillis * INTERVAL '1 millisecond' ELSE opened_until END,
            state = CASE WHEN state = 'HALF_OPEN' OR (state = 'CLOSED' AND consecutive_failures + 1 >= :threshold)
                THEN 'OPEN' ELSE state END,
            updated_at = clock_timestamp()
        WHERE name = :name
        RETURNING state
        """.trimIndent(), mapOf("name" to name, "threshold" to threshold, "openMillis" to openMillis),
    ) { rs, _ -> BreakerState.valueOf(rs.getString(1)) }.firstOrNull()
}
