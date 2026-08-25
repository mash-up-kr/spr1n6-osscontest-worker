package com.osscontest.worker.indexing.config

import com.osscontest.worker.indexing.fault.FaultInjectionProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
@EnableConfigurationProperties(FaultInjectionProperties::class)
class IndexingConfiguration {
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
