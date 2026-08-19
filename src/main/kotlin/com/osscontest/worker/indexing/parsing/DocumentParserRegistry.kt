package com.osscontest.worker.indexing.parsing

import org.springframework.stereotype.Component

@Component
class DocumentParserRegistry(
    parsers: List<DocumentParser>,
) {
    private val byMimeType: Map<String, DocumentParser> =
        buildMap {
            parsers.forEach { parser ->
                parser.supportedMimeTypes.forEach { mimeType ->
                    val previous = put(mimeType, parser)
                    check(previous == null) { "Duplicate parser for mimeType=$mimeType: $previous vs $parser" }
                }
            }
        }

    fun findParser(mimeType: String): DocumentParser =
        byMimeType[mimeType] ?: throw UnsupportedMimeTypeException(mimeType)
}
