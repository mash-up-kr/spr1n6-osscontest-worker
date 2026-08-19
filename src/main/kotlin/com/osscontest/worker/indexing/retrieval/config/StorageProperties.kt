package com.osscontest.worker.indexing.retrieval.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "indexing.storage")
data class StorageProperties(
    val bucket: String,
    val endpoint: String?,
    val region: String,
    val downloadTimeout: Duration,
)
