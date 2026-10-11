package com.osscontest.worker.indexing.queue

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/** BLOCKED는 활성 INDEX를 제외한다. LIMITED의 시험 호출권은 실제 LLM 호출 직전에 DB에서 경쟁한다. */
enum class ClaimAdmission { NORMAL, LIMITED, BLOCKED }

/**
 * 임베딩 API의 연속 일시 실패를 모든 Worker가 DB로 공유하는 circuit breaker다.
 * 호출하는 쪽은 DB 장애 대기 규칙을 위해 `DatabaseOperations.execute` 안에서 부른다.
 * OPEN 동안에는 호출 자체를 하지 않으므로 Job의 시도 횟수가 소비되지 않는다.
 */
@Component
class CircuitBreaker(
    private val store: CircuitBreakerStore,
    private val properties: CircuitBreakerProperties,
    meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val open = AtomicInteger(0)

    init {
        // 스크레이프마다 DB를 읽지 않도록 마지막으로 관측한 상태를 노출한다.
        meters.gauge("external_circuit_breaker_open", open)
    }

    /**
     * 호출 전에 기다려야 할 시간을 돌려준다. ZERO이면 호출해도 된다.
     * 돌려받은 시간은 상한이다. 호출자는 확인 간격으로 끊어 기다리고 다시 부른다.
     * breaker 행이 없으면 breaker 없이 동작한다. 행은 스키마 담당자가 적용하는 계약 SQL이 만든다.
     */
    fun acquire(): Duration {
        val snapshot = store.snapshot(properties.name) ?: return Duration.ZERO
        observe(snapshot.state)
        if (snapshot.state == BreakerState.CLOSED) return Duration.ZERO
        val openedUntil = snapshot.openedUntil ?: return Duration.ZERO
        val remaining = Duration.between(snapshot.now, openedUntil)
        if (remaining.isPositive) return remaining
        // 시험 호출권을 못 얻으면 다른 Worker가 먼저 얻은 것이다. 남은 시간을 모르므로 상한(openDuration)을 돌려준다.
        if (!store.tryStartProbe(properties.name, properties.openDuration.toMillis())) return properties.openDuration
        log.info("CIRCUIT_BREAKER_HALF_OPEN name={}", properties.name)
        return Duration.ZERO
    }

    /** claim 전에 부른다. OPEN 유지 시간이 지났으면 LIMITED, 아직이면 BLOCKED를 돌려준다. */
    fun admission(): ClaimAdmission {
        val snapshot = store.snapshot(properties.name) ?: return ClaimAdmission.NORMAL
        observe(snapshot.state)
        if (snapshot.state == BreakerState.CLOSED) return ClaimAdmission.NORMAL
        val openedUntil = snapshot.openedUntil ?: return ClaimAdmission.NORMAL
        return if (openedUntil <= snapshot.now) ClaimAdmission.LIMITED else ClaimAdmission.BLOCKED
    }

    fun recordSuccess() {
        if (store.recordSuccess(properties.name) == 1) {
            open.set(0)
            log.info("CIRCUIT_BREAKER_CLOSED name={}", properties.name)
        }
    }

    /** 일시 실패를 기록하고, 기록 뒤 breaker가 OPEN이면 true를 돌려준다. */
    fun recordTransientFailure(): Boolean {
        val state = store.recordFailure(properties.name, properties.failureThreshold,
            properties.openDuration.toMillis()) ?: return false
        observe(state)
        if (state != BreakerState.OPEN) return false
        log.warn("CIRCUIT_BREAKER_OPEN name={} openDuration={}", properties.name, properties.openDuration)
        return true
    }

    private fun observe(state: BreakerState) = open.set(if (state == BreakerState.CLOSED) 0 else 1)
}
