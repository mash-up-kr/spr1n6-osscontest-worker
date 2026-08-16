package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import kr.dogfoot.hwplib.`object`.HWPFile
import kr.dogfoot.hwplib.reader.HWPReader
import kr.dogfoot.hwplib.tool.textextractor.TextExtractMethod
import kr.dogfoot.hwplib.tool.textextractor.TextExtractor
import org.springframework.stereotype.Component
import java.io.File
import java.io.InputStream

@Component
class HwpDocumentParser : DocumentParser {
    override val supportedMimeTypes = setOf("application/x-hwp", "application/haansofthwp")
    override val parserVersion = "hwp-parser/1.0.0"

    override fun parse(input: InputStream): Sequence<ParsedBlock> {
        val tempFile = File.createTempFile("hwp-parse-", ".hwp")
        val fullText =
            try {
                tempFile.writeBytes(input.readAllBytes())
                val hwpFile: HWPFile = HWPReader.fromFile(tempFile.absolutePath)
                TextExtractor.extract(
                    hwpFile,
                    TextExtractMethod.InsertControlTextBetweenParagraphText,
                )
            } finally {
                tempFile.delete()
            }

        return sequence {
            var order = 0
            for (paragraph in fullText.split(Regex("\n\\s*\n"))) {
                val trimmed = paragraph.trim()
                if (trimmed.isEmpty()) continue
                yield(
                    ParsedBlock(
                        order = order++, type = BlockType.PARAGRAPH, text = trimmed,
                        pageNo = null, headingPath = emptyList(),
                    ),
                )
            }
        }
    }
}
