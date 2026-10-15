package com.osscontest.worker.indexing.config

import com.osscontest.worker.indexing.chunking.config.ChunkingProperties
import com.osscontest.worker.indexing.fault.FaultInjectionProperties
import com.osscontest.worker.indexing.queue.CircuitBreakerProperties
import com.osscontest.worker.indexing.queue.QueueProperties
import com.osscontest.worker.indexing.queue.DatabaseWaitProperties
import com.osscontest.worker.indexing.queue.JobNotificationProperties
import com.osscontest.worker.indexing.retrieval.config.StorageProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
@EnableConfigurationProperties(FaultInjectionProperties::class, QueueProperties::class, DatabaseWaitProperties::class,
    CircuitBreakerProperties::class, ChunkingProperties::class, StorageProperties::class, JobNotificationProperties::class)
class IndexingConfiguration {
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
