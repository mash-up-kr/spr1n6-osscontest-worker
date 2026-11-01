/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("indexing.circuit-breaker")
data class CircuitBreakerProperties(
    val name: String = "embedding-api",
    // 임베딩 API의 일시 실패가 이 횟수만큼 연속되면 연다.
    val failureThreshold: Int = 5,
    // 연 뒤 시험 호출을 허용하기까지의 대기 시간이다.
    val openDuration: Duration = Duration.ofSeconds(60),
    // 시험 호출의 TTL이다. 임베딩 요청 타임아웃(60초)보다 길어야 호출이 끝나기 전에 만료되지 않는다.
    val callDuration: Duration = Duration.ofSeconds(90),
) {
    init {
        require(name.isNotBlank())
        require(failureThreshold > 0)
        require(openDuration.toMillis() > 0)
        require(callDuration.toMillis() > 0)
    }
}
