package com.osscontest.worker.indexing.chunking.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "indexing.chunking")
data class ChunkingProperties(
    val maxChunksPerDocument: Int,
    val maxTotalTokens: Int,
)
