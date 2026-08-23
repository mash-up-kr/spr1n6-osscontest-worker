package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.apache.poi.xwpf.usermodel.XWPFDocument
import org.springframework.stereotype.Component
import java.io.InputStream

@Component
class DocxDocumentParser : DocumentParser {
    override val supportedMimeTypes =
        setOf("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    override fun parse(input: InputStream): Sequence<ParsedBlock> =
        sequence {
            XWPFDocument(input).use { document ->
                var order = 0
                val headingStack = mutableListOf<String>()

                for (paragraph in document.paragraphs) {
                    val text = paragraph.text.trim()
                    if (text.isEmpty()) continue

                    val isHeading = paragraph.style?.startsWith("Heading") == true
                    if (isHeading) {
                        headingStack.clear()
                        headingStack.add(text)
                        yield(
                            ParsedBlock(
                                order = order++, type = BlockType.HEADING, text = text,
                                pageNo = null, headingPath = headingStack.toList(),
                            ),
                        )
                    } else {
                        yield(
                            ParsedBlock(
                                order = order++, type = BlockType.PARAGRAPH, text = text,
                                pageNo = null, headingPath = headingStack.toList(),
                            ),
                        )
                    }
                }

                for (table in document.tables) {
                    val markdown = table.rows.joinToString("\n") { row ->
                        "| " + row.tableCells.joinToString(" | ") { it.text } + " |"
                    }
                    if (markdown.isBlank()) continue
                    yield(
                        ParsedBlock(
                            order = order++, type = BlockType.TABLE, text = markdown,
                            pageNo = null, headingPath = headingStack.toList(),
                        ),
                    )
                }
            }
        }
}
