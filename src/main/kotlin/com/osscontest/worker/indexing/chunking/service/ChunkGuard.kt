package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.config.ChunkingProperties
import com.osscontest.worker.indexing.chunking.domain.Chunk
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.stereotype.Component

@Component
@EnableConfigurationProperties(ChunkingProperties::class)
class ChunkGuard(
    private val properties: ChunkingProperties,
) {
    fun assertValid(chunks: List<Chunk>) {
        if (chunks.isEmpty()) throw EmptyExtractionException()
        if (chunks.size > properties.maxChunksPerDocument) {
            throw ChunkLimitExceededException(chunks.size, properties.maxChunksPerDocument)
        }
        val totalTokens = chunks.sumOf { it.tokenCount }
        if (totalTokens > properties.maxTotalTokens) {
            throw TotalTokenLimitExceededException(totalTokens, properties.maxTotalTokens)
        }
    }
}
