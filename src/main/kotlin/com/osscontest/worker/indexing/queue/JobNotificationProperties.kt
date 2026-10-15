package com.osscontest.worker.indexing.queue

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("indexing.queue.notifications")
data class JobNotificationProperties(
    val enabled: Boolean = true,
    // API Server가 보내는 채널명과 같아야 한다. payload는 claim 조건으로 사용하지 않는다.
    val channel: String = "indexing_job",
    val waitTimeout: Duration = Duration.ofSeconds(5),
    val healthInterval: Duration = Duration.ofSeconds(30),
    val reconnectDelay: Duration = Duration.ofSeconds(1),
    val maxReconnectDelay: Duration = Duration.ofSeconds(30),
) {
    init {
        require(channel.matches(Regex("[a-z_][a-z0-9_]{0,62}"))) { "Invalid notification channel" }
        require(waitTimeout.toMillis() in 1..Int.MAX_VALUE.toLong())
        require(healthInterval.toMillis() > 0 && reconnectDelay.toMillis() > 0)
        require(maxReconnectDelay >= reconnectDelay)
    }
}
