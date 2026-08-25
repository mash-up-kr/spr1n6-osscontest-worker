package com.osscontest.worker.config

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.io.ClassPathResource

class ApplicationYmlConfigTest {
    @Test
    fun `Kafka topic은 환경변수 미설정 시 doc events v1을 사용한다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertThat(props.getProperty("indexing.consumer.topic"))
            .isEqualTo("\${INDEXING_KAFKA_TOPIC:doc.events.v1}")
    }

    @Test
    fun `max poll interval ms는 900초(900000)로 설정돼 있다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertThat(props.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms"))
            .isEqualTo("900000")
    }

    @Test
    fun `임베딩 차원은 DB vector 타입과 같은 1536으로 고정한다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertThat(props.getProperty("spring.ai.openai.embedding.dimensions"))
            .isEqualTo("1536")
    }

    @Test
    fun `청크 토큰 상한과 overlap은 환경변수로 조정할 수 있다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertThat(props.getProperty("indexing.chunking.max-tokens-per-chunk"))
            .isEqualTo("\${INDEXING_MAX_TOKENS_PER_CHUNK:512}")
        assertThat(props.getProperty("indexing.chunking.overlap-tokens"))
            .isEqualTo("\${INDEXING_OVERLAP_TOKENS:64}")
    }

    @Test
    fun `fault injection은 기본 비활성이고 환경변수로 대상을 지정할 수 있다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        assertThat(props.getProperty("fault-injection.enabled"))
            .isEqualTo("\${FAULT_INJECTION_ENABLED:false}")
        assertThat(props.getProperty("fault-injection.phase"))
            .isEqualTo("\${FAULT_INJECTION_PHASE:EMBEDDING}")
    }
}
