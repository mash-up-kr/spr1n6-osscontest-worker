/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.service.RandomRetryJitter
import com.osscontest.worker.indexing.pipeline.service.RetryPolicy
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * 임베딩 API breaker 상태를 external_circuit_breaker 행 하나에 두어 모든 Worker 인스턴스가 공유한다.
 * [acquire]로 받은 허가는 [record]나 [release]로 반드시 끝내야 한다.
 */
@Component
class CircuitBreaker(
    private val store: CircuitBreakerStore,
    private val properties: CircuitBreakerProperties,
    meters: MeterRegistry,
    private val retryPolicy: RetryPolicy = RetryPolicy(RandomRetryJitter()),
    @Value("\${indexing.retry.base-delay:PT30S}") private val baseDelay: Duration = Duration.ofSeconds(30),
    @Value("\${spring.ai.openai.embedding.timeout:PT60S}") timeout: Duration = Duration.ofSeconds(60),
) {
    private val open = AtomicInteger(0)
    init {
        // 시험 호출 TTL이 임베딩 타임아웃보다 짧으면 정상 응답이 오기 전에 시험 호출이 만료된다.
        require(properties.callDuration > timeout) { "Circuit call TTL must exceed the embedding timeout" }
        meters.gauge("external_circuit_breaker_open", open)
    }
    fun admission(): ClaimAdmission {
        val row = store.snapshot(properties.name)
        observe(row.state)
        return when { row.state == BreakerState.CLOSED -> ClaimAdmission.NORMAL
            row.available() -> ClaimAdmission.LIMITED
            else -> ClaimAdmission.BLOCKED }
    }
    fun lockForClaim() = store.lockForClaim(properties.name)
    fun reserveForClaim(row: BreakerSnapshot, token: ExecutionToken, leaseMillis: Long) = store.reserve(properties.name, row, token, leaseMillis)
    fun acquire(token: ExecutionToken, callId: String): CallAdmission {
        val admission = store.acquire(properties.name, token, callId, properties.callDuration.toMillis())
        open.set(if (admission is CallAdmission.Allowed && admission.permit.probeOwner == null) 0 else 1)
        return admission
    }
    fun valid(permit: CircuitPermit) = store.valid(permit)
    fun release(token: ExecutionToken, callId: String) = store.release(properties.name, token, callId)
    fun record(permit: CircuitPermit, result: CircuitResult) {
        observe(store.record(permit, result, properties.failureThreshold, properties.openDuration.toMillis()) { now ->
            // rate limit 대기 시각만 계산한다. 시도 한도를 두지 않아 항상 RETRY_WAIT 기한이 나온다.
            retryPolicy.decideFailure(false, result.attempt, Int.MAX_VALUE, baseDelay, now, result.retryAfter).nextRetryAt!!
        })
    }
    private fun observe(state: BreakerState) = open.set(if (state == BreakerState.CLOSED) 0 else 1)
}
