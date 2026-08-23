package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.pdmodel.PDPage
import org.apache.pdfbox.pdmodel.PDPageContentStream
import org.apache.pdfbox.pdmodel.font.PDType1Font
import org.apache.pdfbox.pdmodel.font.Standard14Fonts
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream

class PdfDocumentParserTest {
    private val parser = PdfDocumentParser()

    @Test
    fun `PDF 텍스트를 페이지 번호와 함께 파싱한다`() {
        // PDFBox 기본 Helvetica 글꼴은 한글을 지원하지 않으므로 테스트 PDF에는 ASCII를 쓴다.
        val bytes = onePagePdf("pgvector is a PostgreSQL extension for vector data.")

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).isNotEmpty
        assertThat(blocks.first().type).isEqualTo(BlockType.PARAGRAPH)
        assertThat(blocks.first().pageNo).isEqualTo(1)
        assertThat(blocks.first().text).contains("pgvector")
    }

    @Test
    fun `한 페이지 안에서 빈 줄로 구분된 두 문단을 별개 블록으로 분리한다`() {
        // 공백 글리프 한 줄은 PDFTextStripper 결과에서 빈 줄이 되어 문단 구분자로 쓰인다.
        val bytes =
            multiPagePdf(
                listOf(
                    "paragraph one line a.",
                    "paragraph one line b.",
                    " ",
                    "paragraph two line a.",
                    "paragraph two line b.",
                ),
            )

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).hasSize(2)
        assertThat(blocks).allMatch { it.type == BlockType.PARAGRAPH }
        assertThat(blocks).allMatch { it.pageNo == 1 }
        assertThat(blocks[0].text).isEqualTo("paragraph one line a. paragraph one line b.")
        assertThat(blocks[1].text).isEqualTo("paragraph two line a. paragraph two line b.")
    }

    @Test
    fun `공백 글리프 없이 순수한 수직 간격만으로도 두 문단을 분리한다`() {
        // 실제 문서는 공백 글리프 없이 수직 간격만으로 문단을 나누기도 한다. 14pt 줄 간격 뒤
        // 40pt 간격을 두어 PDFTextStripper의 문단 감지 결과가 빈 줄로 출력되는지 검증한다.
        val bytes =
            pageWithVerticalGap(
                topLines = listOf("paragraph one line a.", "paragraph one line b."),
                gapBeforeBottom = 40f,
                bottomLines = listOf("paragraph two line a.", "paragraph two line b."),
            )

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).hasSize(2)
        assertThat(blocks).allMatch { it.type == BlockType.PARAGRAPH }
        assertThat(blocks).allMatch { it.pageNo == 1 }
        assertThat(blocks[0].text).isEqualTo("paragraph one line a. paragraph one line b.")
        assertThat(blocks[1].text).isEqualTo("paragraph two line a. paragraph two line b.")
    }

    @Test
    fun `여러 페이지에 걸쳐 페이지 번호를 유지하고 순서가 계속 증가한다`() {
        val bytes =
            multiPagePdf(
                listOf("first page paragraph one.", " ", "first page paragraph two."),
                listOf("second page paragraph."),
            )

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).hasSize(3)
        assertThat(blocks.map { it.pageNo }).containsExactly(1, 1, 2)
        assertThat(blocks.map { it.order }).containsExactly(0, 1, 2)
        // 블록 순서는 페이지가 바뀌어도 초기화하지 않는다.
        assertThat(blocks.zipWithNext().all { (a, b) -> b.order == a.order + 1 }).isTrue
    }

    @Test
    fun `텍스트가 없는 빈 페이지는 블록을 생성하지 않고 앞뒤 페이지 번호와 순서를 보존한다`() {
        val bytes =
            multiPagePdf(
                listOf("page one paragraph."),
                null, // 콘텐츠 스트림이 없는 빈 페이지
                listOf("page three paragraph."),
            )

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).hasSize(2)
        assertThat(blocks.map { it.pageNo }).containsExactly(1, 3)
        assertThat(blocks).noneMatch { it.pageNo == 2 }
        assertThat(blocks.map { it.order }).containsExactly(0, 1)
    }

    private fun onePagePdf(text: String): ByteArray {
        PDDocument().use { doc ->
            val page = PDPage()
            doc.addPage(page)
            PDPageContentStream(doc, page).use { stream ->
                stream.beginText()
                stream.setFont(PDType1Font(Standard14Fonts.FontName.HELVETICA), 12f)
                stream.newLineAtOffset(50f, 700f)
                stream.showText(text)
                stream.endText()
            }
            val out = ByteArrayOutputStream()
            doc.save(out)
            return out.toByteArray()
        }
    }

    /** 각 원소를 한 페이지의 줄 목록으로 렌더링하며 `null`은 콘텐츠 스트림 없는 빈 페이지다. */
    private fun multiPagePdf(vararg pages: List<String>?): ByteArray {
        PDDocument().use { doc ->
            for (lines in pages) {
                val page = PDPage()
                doc.addPage(page)
                if (lines == null) continue

                PDPageContentStream(doc, page).use { stream ->
                    var y = 700f
                    for (line in lines) {
                        stream.beginText()
                        stream.setFont(PDType1Font(Standard14Fonts.FontName.HELVETICA), 12f)
                        stream.newLineAtOffset(50f, y)
                        stream.showText(line)
                        stream.endText()
                        y -= 14f
                    }
                }
            }
            val out = ByteArrayOutputStream()
            doc.save(out)
            return out.toByteArray()
        }
    }

    /** 두 줄 그룹 사이에 공백 글리프 없이 [gapBeforeBottom]만큼 수직 간격을 둔 PDF를 만든다. */
    private fun pageWithVerticalGap(
        topLines: List<String>,
        gapBeforeBottom: Float,
        bottomLines: List<String>,
    ): ByteArray {
        val normalPitch = 14f
        PDDocument().use { doc ->
            val page = PDPage()
            doc.addPage(page)
            PDPageContentStream(doc, page).use { stream ->
                var y = 700f
                for (line in topLines) {
                    stream.beginText()
                    stream.setFont(PDType1Font(Standard14Fonts.FontName.HELVETICA), 12f)
                    stream.newLineAtOffset(50f, y)
                    stream.showText(line)
                    stream.endText()
                    y -= normalPitch
                }
                y -= (gapBeforeBottom - normalPitch)
                for (line in bottomLines) {
                    stream.beginText()
                    stream.setFont(PDType1Font(Standard14Fonts.FontName.HELVETICA), 12f)
                    stream.newLineAtOffset(50f, y)
                    stream.showText(line)
                    stream.endText()
                    y -= normalPitch
                }
            }
            val out = ByteArrayOutputStream()
            doc.save(out)
            return out.toByteArray()
        }
    }
}
