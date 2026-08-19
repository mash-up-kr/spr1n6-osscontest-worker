package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.encoding
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.headingContextPrefix
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.sha256Hex
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.toIntArrayList
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.springframework.stereotype.Component

@Component
class ParagraphOverlapChunker(
    private val maxTokensPerChunk: Int = DEFAULT_MAX_TOKENS,
    private val overlapTokens: Int = DEFAULT_OVERLAP_TOKENS,
) {
    fun chunk(blocks: List<ParsedBlock>): List<Chunk> {
        var chunkNo = 0
        val result = mutableListOf<Chunk>()
        val stride = maxTokensPerChunk - overlapTokens
        require(stride > 0) { "overlapTokens must be smaller than maxTokensPerChunk" }

        for (block in blocks) {
            val prefix = headingContextPrefix(block.headingPath)
            val tokens = encoding.encode(block.text).boxed()

            if (tokens.size <= maxTokensPerChunk) {
                val content = prefix + block.text
                result.add(buildChunk(chunkNo++, content, tokens.size, block))
                continue
            }

            var start = 0
            while (start < tokens.size) {
                val end = minOf(start + maxTokensPerChunk, tokens.size)
                val window = tokens.subList(start, end)
                val content = prefix + encoding.decode(window.toIntArrayList())
                result.add(buildChunk(chunkNo++, content, window.size, block))
                if (end == tokens.size) break
                start += stride
            }
        }
        return result
    }

    private fun buildChunk(
        no: Int,
        content: String,
        tokenCount: Int,
        block: ParsedBlock,
    ) = Chunk(
        chunkNo = no,
        content = content,
        contentHash = sha256Hex(content),
        tokenCount = tokenCount,
        pageFrom = block.pageNo,
        pageTo = block.pageNo,
        sectionPath = block.headingPath.joinToString(" > ").ifEmpty { null },
        metadata = null,
    )

    private companion object {
        const val DEFAULT_MAX_TOKENS = 512
        const val DEFAULT_OVERLAP_TOKENS = 64
    }
}
