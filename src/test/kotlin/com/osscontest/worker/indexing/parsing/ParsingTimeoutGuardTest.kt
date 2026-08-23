package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.IOException
import java.nio.file.Files
import java.time.Duration

class ParsingTimeoutGuardTest {
    @Test
    fun `제한시간 안에 끝나면 정상적으로 결과를 반환한다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(500), concurrency = 2, meterRegistry = SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        val result = guard.parse(parser, path, "text/plain")

        assertThat(result).containsExactly(block)
    }

    @Test
    fun `제한시간을 넘기면 ParseTimeoutException을 던진다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(100), concurrency = 2, meterRegistry = SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<ParseTimeoutException> { guard.parse(parser, path, "text/plain") }
    }

    @Test
    fun `파싱 중 IOException이 나면 CorruptedFileException으로 감싼다`() {
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofSeconds(5), concurrency = 2, meterRegistry = SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(sequence { throw IOException("bad pdf") })
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<CorruptedFileException> { guard.parse(parser, path, "application/pdf") }
    }

    @Test
    fun `타임아웃이 발생하면 parse_timeout_total 카운터가 증가한다`() {
        val meterRegistry = SimpleMeterRegistry()
        val guard = ParsingTimeoutGuard(parseTimeout = Duration.ofMillis(100), concurrency = 2, meterRegistry = meterRegistry)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<ParseTimeoutException> { guard.parse(parser, path, "text/plain") }

        assertThat(meterRegistry.get("parse_timeout_total").counter().count()).isEqualTo(1.0)
    }
}
