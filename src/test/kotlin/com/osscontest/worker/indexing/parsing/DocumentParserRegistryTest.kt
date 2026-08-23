package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.InputStream

class DocumentParserRegistryTest {
    private class FakeParser(
        override val supportedMimeTypes: Set<String>,
    ) : DocumentParser {
        override fun parse(input: InputStream): Sequence<ParsedBlock> =
            sequenceOf(ParsedBlock(0, BlockType.PARAGRAPH, "fake", null, emptyList()))
    }

    @Test
    fun `mimeType에 맞는 파서를 찾는다`() {
        val registry = DocumentParserRegistry(listOf(FakeParser(setOf("text/plain"))))

        val parser = registry.findParser("text/plain")

        assertThat(parser.supportedMimeTypes).contains("text/plain")
    }

    @Test
    fun `등록되지 않은 mimeType은 예외를 던진다`() {
        val registry = DocumentParserRegistry(emptyList())

        val thrown =
            try {
                registry.findParser("application/x-unknown")
                null
            } catch (e: UnsupportedMimeTypeException) {
                e
            }

        assertThat(thrown).isNotNull
        assertThat(thrown!!.code).isEqualTo("UNSUPPORTED_MIME_TYPE")
    }

    @Test
    fun `같은 mimeType을 두 파서가 등록하면 생성 시점에 예외를 던진다`() {
        val parsers =
            listOf(
                FakeParser(setOf("text/plain")),
                FakeParser(setOf("text/plain")),
            )

        assertThatThrownBy { DocumentParserRegistry(parsers) }
            .isInstanceOf(IllegalStateException::class.java)
    }
}
