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
        // §3.6: 총 토큰 상한이 없으면 대용량 문서에서 임베딩 호출이 통째로 실패한다.
        // tokenCount가 null인 청크(토큰 수를 세지 않는 전략)는 0으로 취급한다 — 알 수 없는 값을
        // 임의로 추정해 상한을 넘기는 것보다, 세어진 것만 합산하는 쪽이 덜 놀랍다.
        val totalTokens = chunks.sumOf { it.tokenCount ?: 0 }
        if (totalTokens > properties.maxTotalTokens) {
            throw TotalTokenLimitExceededException(totalTokens, properties.maxTotalTokens)
        }
    }
}
