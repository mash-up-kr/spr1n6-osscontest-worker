package com.osscontest.worker.indexing.queue

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.util.UUID

@ConfigurationProperties("indexing.queue")
data class QueueProperties(
    val enabled: Boolean = true,
    val workerId: String = UUID.randomUUID().toString(),
    val concurrency: Int = 5,
    val pollInterval: Duration = Duration.ofSeconds(1),
    val leaseDuration: Duration = Duration.ofSeconds(60),
    val heartbeatInterval: Duration = Duration.ofSeconds(10),
) {
    init {
        require(workerId.isNotBlank())
        require(concurrency > 0)
        require(pollInterval.toMillis() > 0 && heartbeatInterval.toMillis() > 0)
        require(leaseDuration > heartbeatInterval.multipliedBy(2))
    }
}

@ConfigurationProperties("indexing.database")
data class DatabaseWaitProperties(
    val checkInterval: Duration = Duration.ofSeconds(5),
    val queryTimeout: Duration = Duration.ofSeconds(3),
) {
    init {
        require(checkInterval.toMillis() > 0 && queryTimeout.seconds > 0)
    }
}
