package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ParagraphChunkerTest {
    private val chunker = ParagraphChunker(maxTokensPerChunk = 512)

    @Test
    fun `문단 경계를 존중하며 문단이 짧으면 하나로 합치지 않는다`() {
        val blocks =
            listOf(
                ParsedBlock(0, BlockType.PARAGRAPH, "첫 문단", null, emptyList()),
                ParsedBlock(1, BlockType.PARAGRAPH, "둘째 문단", null, emptyList()),
            )

        val chunks = chunker.chunk(blocks)

        assertThat(chunks).hasSize(2)
        assertThat(chunks[0].content).endsWith("첫 문단")
        assertThat(chunks[1].content).endsWith("둘째 문단")
    }

    @Test
    fun `토큰 상한을 넘는 문단은 분할한다`() {
        val longParagraph = (1..200).joinToString(" ") { "단어$it" }
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, longParagraph, null, emptyList()))

        val chunks = ParagraphChunker(maxTokensPerChunk = 10).chunk(blocks)

        assertThat(chunks.size).isGreaterThan(1)
    }

    @Test
    fun `각 블록은 자신의 헤딩 경로만 접두어로 사용한다`() {
        val blocks =
            listOf(
                ParsedBlock(0, BlockType.PARAGRAPH, "첫 문단", null, listOf("A")),
                ParsedBlock(1, BlockType.PARAGRAPH, "둘째 문단", null, listOf("B")),
            )

        val chunks = chunker.chunk(blocks)

        assertThat(chunks[0].content).startsWith("[A]\n")
        assertThat(chunks[1].content).startsWith("[B]\n")
    }

    @Test
    fun `빈 입력이면 청크를 생성하지 않는다`() {
        assertThat(chunker.chunk(emptyList())).isEmpty()
    }

    @Test
    fun `본문이 빈 블록도 청크 하나를 생성한다`() {
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, "", null, emptyList()))

        val chunks = chunker.chunk(blocks)

        assertThat(chunks).hasSize(1)
    }
}
