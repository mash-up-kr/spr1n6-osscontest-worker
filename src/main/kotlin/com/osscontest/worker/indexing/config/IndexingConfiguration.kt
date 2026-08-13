package com.osscontest.worker.indexing.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class IndexingConfiguration {
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
