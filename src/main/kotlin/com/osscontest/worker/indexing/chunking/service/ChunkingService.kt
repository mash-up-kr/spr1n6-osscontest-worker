package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.springframework.stereotype.Service

/**
 * 같은 블록과 전략에는 항상 같은 청크를 반환한다.
 *
 * 결과의 `chunkNo`는 0부터 연속되고 `tokenCount`는 헤딩 접두어를 포함한 최종 `content` 기준이다.
 */
@Service
class ChunkingService(
    private val fixedTokenChunker: FixedTokenChunker,
    private val paragraphChunker: ParagraphChunker,
    private val paragraphOverlapChunker: ParagraphOverlapChunker,
) {
    fun chunk(
        blocks: List<ParsedBlock>,
        strategy: ChunkingStrategy,
    ): List<Chunk> =
        when (strategy) {
            ChunkingStrategy.FIXED_TOKEN -> fixedTokenChunker.chunk(blocks)
            ChunkingStrategy.PARAGRAPH -> paragraphChunker.chunk(blocks)
            ChunkingStrategy.PARAGRAPH_OVERLAP -> paragraphOverlapChunker.chunk(blocks)
        }
}
