package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.bodyTokenLimit
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.encoding
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.headingContextPrefix
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.sha256Hex
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.tokenCount
import com.osscontest.worker.indexing.chunking.service.ChunkerTokenizer.toIntArrayList
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class ParagraphChunker(
    @Value("\${indexing.chunking.max-tokens-per-chunk}")
    private val maxTokensPerChunk: Int,
) {
    fun chunk(blocks: List<ParsedBlock>): List<Chunk> {
        var chunkNo = 0
        val result = mutableListOf<Chunk>()

        for (block in blocks) {
            val prefix = headingContextPrefix(block.headingPath)
            val maxBodyTokens = bodyTokenLimit(prefix, maxTokensPerChunk)
            val tokens = encoding.encode(block.text).boxed()

            if (tokens.size <= maxBodyTokens) {
                val content = prefix + block.text
                result.add(buildChunk(chunkNo++, content, block))
            } else {
                for (window in tokens.chunked(maxBodyTokens)) {
                    val content = prefix + encoding.decode(window.toIntArrayList())
                    result.add(buildChunk(chunkNo++, content, block))
                }
            }
        }
        return result
    }

    private fun buildChunk(
        no: Int,
        content: String,
        block: ParsedBlock,
    ) = Chunk(
        chunkNo = no,
        content = content,
        contentHash = sha256Hex(content),
        tokenCount = tokenCount(content),
        pageFrom = block.pageNo,
        pageTo = block.pageNo,
        sectionPath = block.headingPath.joinToString(" > ").ifEmpty { null },
        metadata = null,
    )
}
