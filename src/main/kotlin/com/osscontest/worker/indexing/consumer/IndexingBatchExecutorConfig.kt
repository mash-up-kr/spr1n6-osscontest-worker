package com.osscontest.worker.indexing.consumer

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

// 배치 안에서 documentId 그룹을 동시에 처리하기 위한 고정 크기 스레드풀(§3.1, §3.8).
// LLM/임베딩 API 쿼터 제어 목적으로 동시성을 명시적으로 제한한다 — Kafka listener의
// concurrency(파티션 병렬성)와는 별개 축이다.
@Configuration
class IndexingBatchExecutorConfig {
    @Bean(destroyMethod = "shutdown")
    fun indexingBatchExecutor(
        @Value("\${indexing.consumer.concurrency:5}") concurrency: Int,
    ): ExecutorService = Executors.newFixedThreadPool(concurrency)
}
