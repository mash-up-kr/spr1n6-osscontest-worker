package com.osscontest.worker.config

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.io.ClassPathResource

class ApplicationYmlConfigTest {
    @Test
    fun `max poll interval ms는 900초(900000)로 설정돼 있다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertThat(props.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms"))
            .isEqualTo("900000")
    }
}
