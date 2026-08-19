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
        // NOTE: Standard 14 fonts (e.g. Helvetica) only support WinAnsiEncoding, which has no
        // Korean glyphs — PDType1Font.showText() throws IllegalArgumentException for U+AC00-D7A3.
        // Using ASCII content here to isolate the test from that PDFBox font-encoding constraint;
        // the parser itself is charset-agnostic and doesn't care what language the PDF contains.
        val bytes = onePagePdf("pgvector is a PostgreSQL extension for vector data.")

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).isNotEmpty
        assertThat(blocks.first().type).isEqualTo(BlockType.PARAGRAPH)
        assertThat(blocks.first().pageNo).isEqualTo(1)
        assertThat(blocks.first().text).contains("pgvector")
    }

    @Test
    fun `한 페이지 안에서 빈 줄로 구분된 두 문단을 별개 블록으로 분리한다`() {
        // A lone-space text line between two groups of real text lines is how PDFTextStripper's
        // default extraction actually renders a blank line as "\n \n" in the stripped text
        // (verified empirically — PDFTextStripper's built-in paragraphStart/paragraphEnd are
        // empty by default, so vertical spacing alone does NOT produce a blank line; only an
        // actual whitespace glyph run does). This is what pageText.split(Regex("\n\\s*\n"))
        // in the parser is designed to split on.
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
        // Realistic PDFs (from Word/LaTeX/Google Docs etc.) mark a paragraph break purely with
        // extra vertical spacing between lines — no literal whitespace glyph is present. PDFBox's
        // PDFTextStripper has a built-in vertical-gap paragraph detector (dropThreshold, default
        // 2.5x the previous line height) that fires internally, but by default it writes nothing
        // extra to the output because paragraphStart/paragraphEnd default to "" — so without
        // configuring paragraphStart, a page like this would extract as one run-on paragraph and
        // pageText.split(Regex("\n\\s*\n")) would never split. PdfDocumentParser sets
        // paragraphStart = "\n" specifically so this realistic case produces the blank-line gap
        // the split regex needs. Two normal-pitch lines (14pt), then a ~40pt jump (well above the
        // ~30-35pt dropThreshold for 12pt text), then two more normal-pitch lines.
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
        // strictly increasing across the whole document, not reset per page
        assertThat(blocks.zipWithNext().all { (a, b) -> b.order == a.order + 1 }).isTrue
    }

    @Test
    fun `텍스트가 없는 빈 페이지는 블록을 생성하지 않고 앞뒤 페이지 번호와 순서를 보존한다`() {
        val bytes =
            multiPagePdf(
                listOf("page one paragraph."),
                null, // blank page: PDPage() added with no content stream at all
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

    /**
     * Builds a multi-page PDF. Each element is the list of text lines to render top-to-bottom
     * on that page, or `null` for a page with no content stream at all (a truly blank page).
     * A line consisting of a single space (`" "`) renders as its own whitespace-only text run,
     * which PDFTextStripper surfaces as a blank line (`"\n \n"`) in the extracted text — the
     * gap the parser's paragraph-split regex looks for.
     */
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

    /**
     * Builds a single-page PDF with two groups of lines (normal 14pt line pitch within each
     * group) separated by [gapBeforeBottom] — a pure vertical jump, no whitespace glyph involved.
     * Used to validate paragraph splitting against realistic PDF paragraph-break geometry (as
     * opposed to the artificial space-glyph line used elsewhere in this file).
     */
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
