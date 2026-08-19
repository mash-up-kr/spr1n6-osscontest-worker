package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.encoding
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.headingContextPrefix
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.sha256Hex
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.toIntArrayList
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.springframework.stereotype.Component

@Component
class FixedTokenChunker(
    private val maxTokensPerChunk: Int = DEFAULT_MAX_TOKENS,
) {
    fun chunk(blocks: List<ParsedBlock>): List<Chunk> {
        val fullText = blocks.joinToString("\n\n") { it.text }
        val headingPath = blocks.firstOrNull()?.headingPath.orEmpty()
        val prefix = headingContextPrefix(headingPath)

        val tokens = encoding.encode(fullText).boxed()
        if (tokens.isEmpty()) return emptyList()

        return tokens.chunked(maxTokensPerChunk).mapIndexed { index, tokenWindow ->
            val body = encoding.decode(tokenWindow.toIntArrayList())
            val content = prefix + body
            Chunk(
                chunkNo = index,
                content = content,
                contentHash = sha256Hex(content),
                tokenCount = tokenWindow.size,
                pageFrom = blocks.firstOrNull()?.pageNo,
                pageTo = blocks.lastOrNull()?.pageNo,
                sectionPath = headingPath.joinToString(" > ").ifEmpty { null },
                metadata = null,
            )
        }
    }

    private companion object {
        const val DEFAULT_MAX_TOKENS = 512
    }
}
