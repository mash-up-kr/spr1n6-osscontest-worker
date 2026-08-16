package com.osscontest.worker

import com.osscontest.worker.indexing.pipeline.service.FakeIndexingProcessor
import com.osscontest.worker.indexing.pipeline.service.IndexingProcessor
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class WorkerApplicationTests {
    @Test
    fun contextLoads() {
    }

    // Spring Boot가 @SpringBootTest 클래스 안의 중첩 @TestConfiguration을 자동으로 인식해서
    // 메인 컨텍스트에 얹어준다 — IndexingProcessor의 실제 구현이 없는(Task 2) 이 브랜치에서
    // 유일하게 이 fake만 빈으로 등록된다.
    @TestConfiguration
    class FakeIndexingProcessorConfig {
        @Bean
        fun indexingProcessor(): IndexingProcessor = FakeIndexingProcessor()
    }
}
