package com.osscontest.worker.config

import com.osscontest.worker.indexing.embedding.config.EmbeddingClientConfiguration.Companion.EMBEDDING_DIMENSIONS
import com.osscontest.worker.indexing.publication.entity.DocumentChunkEntity
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.SQLExceptionOverride.Override
import jakarta.persistence.Column
import java.sql.SQLException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.io.ClassPathResource

class ApplicationYmlConfigTest {
    @Test
    fun `임베딩 요청과 응답 검증이 쓰는 차원은 청크 컬럼의 vector 차원과 같은 1536이다`() {
        val column = DocumentChunkEntity::class.java.getDeclaredField("embedding")
            .getAnnotation(Column::class.java)

        assertThat(EMBEDDING_DIMENSIONS).isEqualTo(1536)
        assertThat(column.columnDefinition).isEqualTo("vector($EMBEDDING_DIMENSIONS)")
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

    @Test
    fun `datasource는 OpenProxy 오류를 받은 연결을 풀에서 버린다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        val className = props.getProperty("spring.datasource.hikari.exception-override-class-name")
        assertThat(className).isNotNull()
        // 기동 때 Hikari가 하는 것처럼 클래스 이름으로 만든다. 클래스를 옮기거나 이름을 바꿔도 yml 문자열은 따라 바뀌지 않는다.
        val override = HikariConfig().apply { exceptionOverrideClassName = className }.exceptionOverride

        assertThat(override.adjudicate(SQLException("read-only", "25006"))).isEqualTo(Override.MUST_EVICT)
        assertThat(override.adjudicate(SQLException("AllServersDown", "58000"))).isEqualTo(Override.MUST_EVICT)
    }

    @Test
    fun `datasource 풀 크기는 OpenProxy aidocs 풀 크기를 넘지 않는다`() {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(ClassPathResource("application.yml"))
        val props = factory.getObject()!!

        val poolSize = props.getProperty("spring.datasource.hikari.maximum-pool-size").toInt()
        assertThat(poolSize).isLessThanOrEqualTo(10)
    }
}
