package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.apache.pdfbox.Loader
import org.apache.pdfbox.text.PDFTextStripper
import org.springframework.stereotype.Component
import java.io.InputStream

@Component
class PdfDocumentParser : DocumentParser {
    override val supportedMimeTypes = setOf("application/pdf")
    override fun parse(input: InputStream): Sequence<ParsedBlock> =
        sequence {
            Loader.loadPDF(input.readAllBytes()).use { document ->
                var order = 0
                for (pageIndex in 1..document.numberOfPages) {
                    val stripper =
                        PDFTextStripper().apply {
                            startPage = pageIndex
                            endPage = pageIndex
                            paragraphStart = "\n"
                        }
                    val pageText = stripper.getText(document).trim()
                    if (pageText.isEmpty()) continue

                    for (paragraph in pageText.split(Regex("\n\\s*\n"))) {
                        val trimmed = paragraph.trim().replace(Regex("\\s+"), " ")
                        if (trimmed.isEmpty()) continue
                        yield(
                            ParsedBlock(
                                order = order++,
                                type = BlockType.PARAGRAPH,
                                text = trimmed,
                                pageNo = pageIndex,
                                headingPath = emptyList(),
                            ),
                        )
                    }
                }
            }
        }
}
