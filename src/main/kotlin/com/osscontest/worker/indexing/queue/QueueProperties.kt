package com.osscontest.worker.indexing.queue

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.util.UUID

@ConfigurationProperties("indexing.queue")
data class QueueProperties(
    val enabled: Boolean = true,
    val workerId: String = UUID.randomUUID().toString(),
    val concurrency: Int = 5, // Worker 인스턴스당 최대 claim·처리 루프 수다.
    val pollInterval: Duration = Duration.ofSeconds(30), // 알림 유실과 예약 재시도를 보완하는 탐색 간격이다.
    val claimErrorDelay: Duration = Duration.ofSeconds(1),
    val workerIdleTimeout: Duration = Duration.ofSeconds(60), // idle 스레드를 이 시간 동안 재사용한 뒤 해제한다.
    val leaseDuration: Duration = Duration.ofSeconds(60),
    val heartbeatInterval: Duration = Duration.ofSeconds(10),
) {
    init {
        require(workerId.isNotBlank())
        require(concurrency > 0)
        require(pollInterval.toMillis() > 0 && heartbeatInterval.toMillis() > 0)
        require(claimErrorDelay.toMillis() > 0)
        require(workerIdleTimeout.toMillis() > 0)
        // heartbeat가 한 번 늦어도 lease가 끝나지 않도록 heartbeat 간격의 두 배보다 길게 둔다.
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
