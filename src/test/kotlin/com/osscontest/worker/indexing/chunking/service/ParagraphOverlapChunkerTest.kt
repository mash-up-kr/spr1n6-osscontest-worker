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
        // CL100K_BASE는 이 흔한 영단어들을 앞의 공백과 합쳐서 하나의 토큰으로 인코딩한다
        // (예: " box" -> 토큰 1개), 그래서 연속된 단어들이 토큰과 1:1로 매핑된다 —
        // 모든 단어에 대해 encoding.encode(" word").boxed().size == 1로 검증했다.
        // "7"을 인덱스 6에 일부러 넣은 이유: CL100K_BASE는 숫자 토큰 앞의 공백은 병합하지
        // 않는다(" 7" -> 토큰 2개: 순수 공백 토큰 + 순수 "7" 토큰, encoding.encode(" 7").boxed()
        // == [220, <digit-token>]로 검증). 그래서 토큰 인덱스 7 지점의 청크 경계
        // (stride = maxTokensPerChunk - overlapTokens = 7)가 공백과 병합된 단어 토큰이 아니라
        // 순수 숫자 토큰 위에 정확히 걸린다. 이 덕분에 chunk[1]의 디코딩된 내용 맨 앞에 공백이
        // 남지 않는데, 만약 공백이 남으면 split(" ")에서 앞에 빈 문자열이 끼어들어 첫 번째
        // 이후의 청크 경계에서 이 단어 정렬 검증이 깨진다.
        val plainWords = listOf(
            "the", "cat", "dog", "run", "sun", "map", "box", "red", "big", "top",
            "cup", "pen", "car", "bus", "bed", "hat", "leg", "arm", "eye", "ear",
        )
        val items = mutableListOf<String>()
        items.addAll(plainWords.subList(0, 6))
        items.add("7")
        var i = 6
        while (items.size < 50) {
            items.add(plainWords[i % plainWords.size])
            i++
        }
        val longParagraph = items.joinToString(" ")
        val blocks = listOf(ParsedBlock(0, BlockType.PARAGRAPH, longParagraph, null, emptyList()))

        val chunks = chunker.chunk(blocks)

        assertThat(chunks.size).isGreaterThan(1)
        val firstTail = chunks[0].content.split(" ").takeLast(3)
        val secondHead = chunks[1].content.split(" ").take(3)
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

        val chunks = chunker.chunk(blocks)

        assertThat(chunks.single().content).startsWith("[3. 아키텍처 > 3.2 벡터 저장]\n")
        assertThat(chunks.single().content).endsWith("본문")
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
