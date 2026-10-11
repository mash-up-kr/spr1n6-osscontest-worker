package com.osscontest.worker.indexing.queue

import org.assertj.core.api.Assertions.assertThat
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
    fun `실제 application 설정은 환경변수의 동시 처리 상한과 탐색 및 idle 간격을 반영한다`() {
        val environment = StandardEnvironment()
        environment.propertySources.addFirst(MapPropertySource("worker-test-env", mapOf(
            "INDEXING_QUEUE_CONCURRENCY" to "3",
            "INDEXING_QUEUE_POLL_INTERVAL" to "PT2S",
            "INDEXING_WORKER_IDLE_TIMEOUT" to "PT15S",
        )))
        YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))
            .forEach(environment.propertySources::addLast)
        val properties = Binder.get(environment).bind("indexing.queue", Bindable.of(QueueProperties::class.java)).get()
        assertThat(properties.concurrency).isEqualTo(3)
        assertThat(properties.pollInterval).isEqualTo(Duration.ofSeconds(2))
        assertThat(properties.workerIdleTimeout).isEqualTo(Duration.ofSeconds(15))
    }
}
