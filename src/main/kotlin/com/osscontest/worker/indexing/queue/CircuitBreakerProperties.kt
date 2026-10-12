package com.osscontest.worker.indexing.queue

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("indexing.circuit-breaker")
data class CircuitBreakerProperties(
    val name: String = "embedding-api",
    val failureThreshold: Int = 5,
    val openDuration: Duration = Duration.ofSeconds(60),
    val callDuration: Duration = Duration.ofSeconds(90),
) {
    init {
        require(name.isNotBlank())
        require(failureThreshold > 0)
        require(openDuration.toMillis() > 0)
        require(callDuration.toMillis() > 0)
    }
}
