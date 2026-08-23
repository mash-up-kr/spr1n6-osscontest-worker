package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class ParagraphOverlapChunkerTest {
    private val chunker = ParagraphOverlapChunker(maxTokensPerChunk = 10, overlapTokens = 3)

    @Test
    fun `분할된 연속 청크는 겹치는 구간을 공유한다`() {
        val longParagraph = List(50) { "token" }.joinToString(" ")
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, longParagraph, null, emptyList()))

        val chunks = chunker.chunk(blocks)

        assertThat(chunks.size).isGreaterThan(1)
        val firstTail = ChunkerTokenizer.encoding.encode(chunks[0].content).boxed().takeLast(3)
        val secondHead = ChunkerTokenizer.encoding.encode(chunks[1].content).boxed().take(3)
        assertThat(secondHead).isEqualTo(firstTail)
    }

    @Test
    fun `동일 입력 100회 반복해도 결과가 같다`() {
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, (1..50).joinToString(" ") { "w$it" }, null, emptyList()))

        val hashes = (1..100).map { chunker.chunk(blocks).map { c -> c.contentHash } }

        assertThat(hashes.distinct()).hasSize(1)
    }

    @Test
    fun `overlapTokens가 maxTokensPerChunk 이상이면 예외를 던진다`() {
        val invalidChunker = ParagraphOverlapChunker(maxTokensPerChunk = 10, overlapTokens = 10)
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, "아무 본문 텍스트", null, emptyList()))

        assertThatThrownBy { invalidChunker.chunk(blocks) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `overlapTokens가 0이면 ParagraphChunker와 동일하게 겹침 없이 분할한다`() {
        val longParagraph = (1..30).joinToString(" ") { "word$it" }
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, longParagraph, null, emptyList()))
        val noOverlapChunker = ParagraphOverlapChunker(maxTokensPerChunk = 10, overlapTokens = 0)
        val paragraphChunker = ParagraphChunker(maxTokensPerChunk = 10)

        val overlapResult = noOverlapChunker.chunk(blocks)
        val paragraphResult = paragraphChunker.chunk(blocks)

        assertThat(overlapResult.map { it.content }).isEqualTo(paragraphResult.map { it.content })
    }

    @Test
    fun `헤딩 경로를 본문 앞에 주입한다`() {
        val blocks =
            listOf(
                ParsedBlock(0, BlockType.PARAGRAPH, "본문", null, listOf("3. 아키텍처", "3.2 벡터 저장")),
            )

        val chunks = ParagraphOverlapChunker(maxTokensPerChunk = 64, overlapTokens = 3).chunk(blocks)

        assertThat(chunks.single().content).startsWith("[3. 아키텍처 > 3.2 벡터 저장]\n")
        assertThat(chunks.single().content).endsWith("본문")
        assertThat(chunks).allMatch { it.tokenCount <= 64 }
        assertThat(chunks).allMatch { it.tokenCount == ChunkerTokenizer.tokenCount(it.content) }
    }

    @Test
    fun `빈 입력이면 청크를 생성하지 않는다`() {
        assertThat(chunker.chunk(emptyList())).isEmpty()
    }

    @Test
    fun `토큰 수가 상한 이하인 블록은 청크 하나로 유지한다`() {
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, "hello world", null, emptyList()))

        val chunks = chunker.chunk(blocks)

        assertThat(chunks).hasSize(1)
    }
}
