package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import org.apache.poi.xwpf.usermodel.XWPFDocument
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream

class DocxDocumentParserTest {
    private val parser = DocxDocumentParser()

    @Test
    fun `Heading 스타일 문단은 HEADING으로, 나머지는 PARAGRAPH로 분류한다`() {
        val bytes =
            docxWith { doc ->
                doc.createParagraph().apply {
                    style = "Heading1"
                    createRun().setText("1. 개요")
                }
                doc.createParagraph().createRun().setText("본문 내용입니다.")
            }

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks[0].type).isEqualTo(BlockType.HEADING)
        assertThat(blocks[0].text).isEqualTo("1. 개요")
        assertThat(blocks[1].type).isEqualTo(BlockType.PARAGRAPH)
        assertThat(blocks[1].headingPath).containsExactly("1. 개요")
    }

    @Test
    fun `행과 열이 채워진 표는 하나의 TABLE 블록으로 마크다운 파이프 형식으로 변환된다`() {
        val bytes =
            docxWith { doc ->
                val table = doc.createTable(2, 2)
                table.getRow(0).getCell(0).setText("이름")
                table.getRow(0).getCell(1).setText("나이")
                table.getRow(1).getCell(0).setText("홍길동")
                table.getRow(1).getCell(1).setText("30")
            }

        val blocks = parser.parse(bytes.inputStream()).toList()
        val tableBlocks = blocks.filter { it.type == BlockType.TABLE }

        assertThat(tableBlocks).hasSize(1)
        assertThat(tableBlocks[0].text).isEqualTo("| 이름 | 나이 |\n| 홍길동 | 30 |")
    }

    @Test
    fun `행이 없는 표는 TABLE 블록을 생성하지 않는다`() {
        val bytes =
            docxWith { doc ->
                val table = doc.createTable(1, 1)
                table.removeRow(0)
            }

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks.filter { it.type == BlockType.TABLE }).isEmpty()
    }

    private fun docxWith(build: (XWPFDocument) -> Unit): ByteArray {
        XWPFDocument().use { doc ->
            build(doc)
            val out = ByteArrayOutputStream()
            doc.write(out)
            return out.toByteArray()
        }
    }
}
