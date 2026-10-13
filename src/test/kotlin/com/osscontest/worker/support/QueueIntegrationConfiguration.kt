package com.osscontest.worker.support

import com.osscontest.worker.indexing.queue.JobExecutionLocks
import com.osscontest.worker.indexing.queue.JobClaimTransactions
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.JobQueueStore
import com.osscontest.worker.indexing.queue.QueueProperties
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import

@TestConfiguration
@EnableConfigurationProperties(QueueProperties::class)
@Import(com.osscontest.worker.indexing.queue.ExecutionResults::class, JobExecutionLocks::class, JobClaimTransactions::class, JobOwnershipGuard::class, JobQueueStore::class, com.osscontest.worker.indexing.pipeline.service.RetryPolicy::class, com.osscontest.worker.indexing.pipeline.service.RandomRetryJitter::class)
class QueueIntegrationConfiguration {
    @Bean
    fun circuitBreaker() = closedCircuitBreaker()
    @Bean
    fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()
}
