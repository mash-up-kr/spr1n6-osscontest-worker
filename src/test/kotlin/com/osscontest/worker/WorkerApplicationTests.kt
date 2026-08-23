package com.osscontest.worker

import com.osscontest.worker.indexing.pipeline.service.FakeIndexingProcessor
import com.osscontest.worker.indexing.pipeline.service.IndexingProcessor
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class WorkerApplicationTests {
    @Test
    fun contextLoads() {
    }

    // 외부 임베딩과 DB 쓰기를 실행하지 않고 애플리케이션 컨텍스트만 검증한다.
    @TestConfiguration
    class FakeIndexingProcessorConfig {
        @Bean
        @Primary
        fun indexingProcessor(): IndexingProcessor = FakeIndexingProcessor()
    }
}
