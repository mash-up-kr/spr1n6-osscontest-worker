package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TextDocumentParserTest {
    private val parser = TextDocumentParser()

    @Test
    fun `MD 헤딩과 문단을 구분한다`() {
        val markdown =
            """
            # 1. 아키텍처

            첫 문단입니다.

            ## 1.2 세부

            두번째 문단입니다.
            """.trimIndent()

        val blocks = parser.parse(markdown.byteInputStream()).toList()

        assertThat(blocks.filter { it.type == BlockType.HEADING }).hasSize(2)
        assertThat(blocks.first { it.text == "1.2 세부" }.headingPath)
            .containsExactly("1. 아키텍처", "1.2 세부")
        assertThat(blocks.first { it.text == "두번째 문단입니다." }.headingPath)
            .containsExactly("1. 아키텍처", "1.2 세부")
    }

    @Test
    fun `일반 텍스트는 헤딩 없이 문단으로만 파싱된다`() {
        val blocks = parser.parse("그냥 평문입니다.".byteInputStream()).toList()

        assertThat(blocks).hasSize(1)
        assertThat(blocks.single().type).isEqualTo(BlockType.PARAGRAPH)
        assertThat(blocks.single().headingPath).isEmpty()
    }

    @Test
    fun `같은 레벨의 헤딩이 연속되면 헤딩 스택을 pop하고 새 헤딩으로 교체한다`() {
        val markdown =
            """
            # 1. 아키텍처

            ## 1.1 세부

            # 2. 배포

            두번째 섹션 문단입니다.
            """.trimIndent()

        val blocks = parser.parse(markdown.byteInputStream()).toList()

        assertThat(blocks.first { it.text == "2. 배포" }.headingPath)
            .containsExactly("2. 배포")
        assertThat(blocks.first { it.text == "두번째 섹션 문단입니다." }.headingPath)
            .containsExactly("2. 배포")
    }

    @Test
    fun `빈 입력은 빈 블록 목록을 반환한다`() {
        val blocks = parser.parse("".byteInputStream()).toList()

        assertThat(blocks).isEmpty()
    }

    @Test
    fun `헤딩만 있고 문단이 없으면 헤딩 블록만 반환한다`() {
        val blocks = parser.parse("# Only Heading".byteInputStream()).toList()

        assertThat(blocks).hasSize(1)
        assertThat(blocks.single().type).isEqualTo(BlockType.HEADING)
        assertThat(blocks.single().text).isEqualTo("Only Heading")
    }
}
