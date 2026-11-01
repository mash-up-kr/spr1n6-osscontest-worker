/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("indexing.queue.notifications")
data class JobNotificationProperties(
    val enabled: Boolean = true,
    // API Server가 보내는 채널명과 같아야 한다. payload는 claim 조건으로 사용하지 않는다.
    val channel: String = "indexing_job",
    // 알림 한 번을 기다리는 최대 시간이다. 짧을수록 종료 신호와 헬스 점검 시각을 자주 확인한다.
    val waitTimeout: Duration = Duration.ofSeconds(5),
    // 같은 연결로 SELECT 1을 보내는 간격이다. 알림 대기만으로는 무응답 연결을 판별하지 못해 둔다.
    val healthInterval: Duration = Duration.ofSeconds(30),
    // 재접속 간격은 첫 값에서 시작해 두 배씩 늘어 최대값에서 멈춘다.
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
