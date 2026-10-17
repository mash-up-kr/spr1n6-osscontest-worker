/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.io.ClassPathResource
import java.time.Duration

class QueuePropertiesTest {
    @Test
    fun `기본 폴백은 30초이고 claim 오류 대기는 1초로 분리한다`() {
        assertThat(QueueProperties().pollInterval).isEqualTo(Duration.ofSeconds(30))
        assertThat(QueueProperties().claimErrorDelay).isEqualTo(Duration.ofSeconds(1))
        assertThatThrownBy { QueueProperties(claimErrorDelay = Duration.ZERO) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `채널 식별자와 수신 대기 및 재접속 설정은 시작 전에 검증한다`() {
        listOf("", "job; SELECT 1", "x".repeat(64)).forEach {
            assertThatThrownBy { JobNotificationProperties(channel = it) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
        assertThatThrownBy { JobNotificationProperties(waitTimeout = Duration.ZERO) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { JobNotificationProperties(maxReconnectDelay = Duration.ofMillis(1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `실제 application 설정은 환경변수의 동시 처리 상한과 탐색 및 idle 간격을 반영한다`() {
        val environment = StandardEnvironment()
        environment.propertySources.addFirst(MapPropertySource("worker-test-env", mapOf(
            "INDEXING_QUEUE_CONCURRENCY" to "3",
            "INDEXING_QUEUE_POLL_INTERVAL" to "PT2S",
            "INDEXING_WORKER_IDLE_TIMEOUT" to "PT15S",
            "INDEXING_QUEUE_CLAIM_ERROR_DELAY" to "PT3S",
        )))
        YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))
            .forEach(environment.propertySources::addLast)
        val properties = Binder.get(environment).bind("indexing.queue", Bindable.of(QueueProperties::class.java)).get()
        assertThat(properties.concurrency).isEqualTo(3)
        assertThat(properties.pollInterval).isEqualTo(Duration.ofSeconds(2))
        assertThat(properties.workerIdleTimeout).isEqualTo(Duration.ofSeconds(15))
        assertThat(properties.claimErrorDelay).isEqualTo(Duration.ofSeconds(3))
        val notifications = Binder.get(environment)
            .bind("indexing.queue.notifications", Bindable.of(JobNotificationProperties::class.java)).get()
        assertThat(notifications.channel).isEqualTo("indexing_job")
    }
}
