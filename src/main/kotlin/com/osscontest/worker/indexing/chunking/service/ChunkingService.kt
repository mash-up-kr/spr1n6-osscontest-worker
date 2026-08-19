package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.springframework.stereotype.Service

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
