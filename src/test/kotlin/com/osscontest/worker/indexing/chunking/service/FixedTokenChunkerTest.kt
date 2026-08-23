package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FixedTokenChunkerTest {
    private val chunker = FixedTokenChunker(maxTokensPerChunk = 10)

    @Test
    fun `토큰 상한을 넘으면 청크를 나눈다`() {
        val longText = (1..30).joinToString(" ") { "단어$it" }
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, longText, null, emptyList()))

        val chunks = chunker.chunk(blocks)

        assertThat(chunks.size).isGreaterThan(1)
        assertThat(chunks.map { it.chunkNo }).isEqualTo((0 until chunks.size).toList())
        assertThat(chunks.dropLast(1)).allMatch { it.tokenCount == 10 }
    }

    @Test
    fun `빈 입력이면 청크를 생성하지 않는다`() {
        assertThat(chunker.chunk(emptyList())).isEmpty()
    }

    @Test
    fun `헤딩 경로를 본문 앞에 주입한다`() {
        val blocks =
            listOf(
                ParsedBlock(0, BlockType.PARAGRAPH, "본문", null, listOf("3. 아키텍처", "3.2 벡터 저장")),
            )

        val chunks = FixedTokenChunker(maxTokensPerChunk = 64).chunk(blocks)

        assertThat(chunks.single().content).startsWith("[3. 아키텍처 > 3.2 벡터 저장]\n")
        assertThat(chunks.single().content).endsWith("본문")
        assertThat(chunks).allMatch { it.tokenCount <= 64 }
        assertThat(chunks).allMatch { it.tokenCount == ChunkerTokenizer.tokenCount(it.content) }
    }

    @Test
    fun `동일 입력 100회 반복 시 결과가 모두 동일하다 - 결정성`() {
        val blocks =
            listOf(
                ParsedBlock(0, BlockType.HEADING, "제목", null, listOf("제목")),
                ParsedBlock(1, BlockType.PARAGRAPH, "본문 내용입니다.", null, listOf("제목")),
            )

        val hashes = (1..100).map { chunker.chunk(blocks).map { it.contentHash } }

        assertThat(hashes.distinct()).hasSize(1)
    }
}
