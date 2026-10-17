/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

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
    private val guards = mutableListOf<ParsingTimeoutGuard>()
    private val files = mutableListOf<com.osscontest.worker.indexing.retrieval.TemporaryDocumentFile>()
    private fun guard(parseTimeout: Duration, concurrency: Int, meterRegistry: SimpleMeterRegistry) =
        ParsingTimeoutGuard(parseTimeout, concurrency, meterRegistry).also(guards::add)
    private fun file(path: java.nio.file.Path) =
        com.osscontest.worker.indexing.retrieval.TemporaryDocumentFile(path).also(files::add)
    @org.junit.jupiter.api.AfterEach fun cleanup() {
        guards.forEach { it.shutdown() }
        files.forEach { it.close() }
    }

    @Test
    fun `parser Error는 감싸지 않고 전달한다`() {
        val guard = guard(Duration.ofSeconds(5), 1, SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        val error = AssertionError("parser error")
        whenever(parser.parse(any())).thenReturn(sequence { throw error })
        val path = Files.createTempFile("test-", ".tmp")
        assertThat(assertThrows<AssertionError> { guard.parse(parser, file(path), "text/plain") }).isSameAs(error)
    }

    @Test
    fun `제한시간 안에 끝나면 정상적으로 결과를 반환한다`() {
        val guard = guard(parseTimeout = Duration.ofMillis(500), concurrency = 2, meterRegistry = SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        val result = guard.parse(parser, file(path), "text/plain")

        assertThat(result).containsExactly(block)
    }

    @Test
    fun `제한시간을 넘기면 ParseTimeoutException을 던진다`() {
        val guard = guard(parseTimeout = Duration.ofMillis(100), concurrency = 2, meterRegistry = SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<ParseTimeoutException> { guard.parse(parser, file(path), "text/plain") }
    }

    @Test
    fun `파싱 중 IOException이 나면 CorruptedFileException으로 감싼다`() {
        val guard = guard(parseTimeout = Duration.ofSeconds(5), concurrency = 2, meterRegistry = SimpleMeterRegistry())
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(sequence { throw IOException("bad pdf") })
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<CorruptedFileException> { guard.parse(parser, file(path), "application/pdf") }
    }

    @Test
    fun `타임아웃이 발생하면 parse_timeout_total 카운터가 증가한다`() {
        val meterRegistry = SimpleMeterRegistry()
        val guard = guard(parseTimeout = Duration.ofMillis(100), concurrency = 2, meterRegistry = meterRegistry)
        val parser: DocumentParser = mock()
        whenever(parser.parse(any())).thenReturn(
            sequence {
                Thread.sleep(2000)
                yield(ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "x", pageNo = null, headingPath = emptyList()))
            },
        )
        val path = Files.createTempFile("test-", ".tmp").also { Files.write(it, "x".toByteArray()) }

        assertThrows<ParseTimeoutException> { guard.parse(parser, file(path), "text/plain") }

        assertThat(meterRegistry.get("parse_timeout_total").counter().count()).isEqualTo(1.0)
    }
}
