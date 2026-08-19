package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import java.io.InputStream

interface DocumentParser {
    val supportedMimeTypes: Set<String>
    val parserVersion: String

    /**
     * Callers must fully consume the returned [Sequence] — implementations may keep the
     * underlying [input] stream/reader open until the sequence is drained, so partial
     * consumption can leak a resource.
     */
    fun parse(input: InputStream): Sequence<ParsedBlock>
}
