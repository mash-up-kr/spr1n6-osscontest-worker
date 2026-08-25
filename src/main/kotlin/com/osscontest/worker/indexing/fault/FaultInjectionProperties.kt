package com.osscontest.worker.indexing.fault

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "fault-injection")
data class FaultInjectionProperties(
    val enabled: Boolean = false,
    val phase: String = "EMBEDDING",
)
