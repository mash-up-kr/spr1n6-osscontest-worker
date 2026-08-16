package com.osscontest.worker.indexing.parsing

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

class HwpDocumentParserTest {
    private val parser = HwpDocumentParser()

    @Test
    fun `HWP 문서에서 텍스트를 추출한다`() {
        val bytes = File("src/test/resources/fixtures/sample.hwp").readBytes()

        val blocks = parser.parse(bytes.inputStream()).toList()

        assertThat(blocks).isNotEmpty
        assertThat(blocks.all { it.text.isNotBlank() }).isTrue()
    }
}
