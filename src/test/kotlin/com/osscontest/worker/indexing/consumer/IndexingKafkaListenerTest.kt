package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.slf4j.MDC
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.kafka.support.Acknowledgment
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.time.Duration
import java.util.concurrent.Executors

// Jackson 3.x(tools.jackson.*) 사용 프로젝트다 — com.fasterxml.jackson.*가 아니다.
class IndexingKafkaListenerTest {
    private val runner: IndexingPipelineRunner = mock()
    private val deletionHandler: DocumentDeletionHandler = mock()
    private val objectMapper = jacksonObjectMapper()
    private val executor = Executors.newSingleThreadExecutor()
    private val listener = IndexingKafkaListener(runner, deletionHandler, objectMapper, executor)
    private val ack: Acknowledgment = mock()

    @AfterEach
    fun tearDown() {
        executor.shutdownNow()
    }

    @Test
    fun `배치 안의 모든 INDEXING_REQUESTED를 PipelineRunner로 보내고 마지막에 한 번만 ack한다`() {
        val records =
            listOf(
                record(key = "1", value = indexingRequestedJson(documentId = 1)),
                record(key = "2", value = indexingRequestedJson(documentId = 2)),
            )

        listener.onMessage(records, ack)

        verify(runner, times(2)).run(any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `같은 documentId(key)의 이벤트는 원래 순서대로 처리한다`() {
        val v1 = record(key = "1", value = indexingRequestedJson(documentId = 1, documentVersionId = 1))
        val v2 = record(key = "1", value = indexingRequestedJson(documentId = 1, documentVersionId = 2))

        listener.onMessage(listOf(v1, v2), ack)

        val order = inOrder(runner)
        order.verify(runner).run(argThatVersionIs(1))
        order.verify(runner).run(argThatVersionIs(2))
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `DOCUMENT_DELETED는 DeletionHandler로 보낸다`() {
        val record = record(key = "1", value = documentDeletedJson())

        listener.onMessage(listOf(record), ack)

        verify(deletionHandler, times(1)).handle(any())
        verify(runner, never()).run(any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `배치 안 한 레코드가 예외를 던져도 나머지는 계속 처리하고 ack한다`() {
        whenever(runner.run(any())).doThrow(RuntimeException("boom"))
        val records =
            listOf(
                record(key = "1", value = indexingRequestedJson(documentId = 1)),
                record(key = "2", value = documentDeletedJson()),
            )

        listener.onMessage(records, ack)

        verify(deletionHandler, times(1)).handle(any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `역직렬화 실패해도 나머지 레코드는 처리하고 ack한다`() {
        val records =
            listOf(
                record(key = "1", value = "not-json"),
                record(key = "2", value = indexingRequestedJson(documentId = 2)),
            )

        listener.onMessage(records, ack)

        verify(runner, times(1)).run(any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `DB 장애(DataAccessException)면 ack 대신 배치 전체를 nack한다`() {
        whenever(runner.run(any())).thenThrow(DataAccessResourceFailureException("connection refused"))
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1)))

        listener.onMessage(records, ack)

        verify(ack, never()).acknowledge()
        verify(ack, times(1)).nack(0, Duration.ofSeconds(5))
    }

    @Test
    fun `DB 장애가 일부 그룹에서만 나도 다른 그룹은 끝까지 처리된 뒤 nack한다`() {
        var otherGroupProcessed = false
        whenever(runner.run(argThatVersionIs(1))).thenThrow(DataAccessResourceFailureException("connection refused"))
        whenever(runner.run(argThatVersionIs(2))).thenAnswer { otherGroupProcessed = true; null }
        val records =
            listOf(
                record(key = "1", value = indexingRequestedJson(documentId = 1, documentVersionId = 1)),
                record(key = "2", value = indexingRequestedJson(documentId = 2, documentVersionId = 2)),
            )

        listener.onMessage(records, ack)

        assertThat(otherGroupProcessed).isTrue()
        verify(ack, never()).acknowledge()
        verify(ack, times(1)).nack(0, Duration.ofSeconds(5))
    }

    @Test
    fun `DB 장애가 아닌 일반 예외는 여전히 ack한다`() {
        whenever(runner.run(any())).thenThrow(RuntimeException("boom"))
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1)))

        listener.onMessage(records, ack)

        verify(ack, times(1)).acknowledge()
        verify(ack, never()).nack(any(), any())
    }

    @Test
    fun `이벤트 처리 중에는 traceId가 MDC에 설정되고 처리 후 worker thread에서 제거된다`() {
        var capturedDuringCall: String? = null
        whenever(runner.run(any())).thenAnswer {
            capturedDuringCall = MDC.get("traceId")
            null
        }
        val records =
            listOf(
                record(
                    key = "1",
                    value = indexingRequestedJson(documentId = 1, traceId = "abc-123"),
                ),
            )

        listener.onMessage(records, ack)
        val capturedAfterCall = executor.submit<String?> { MDC.get("traceId") }.get()

        assertThat(capturedDuringCall).isEqualTo("abc-123")
        assertThat(capturedAfterCall).isNull()
    }

    private fun argThatVersionIs(documentVersionId: Long) =
        org.mockito.kotlin.argThat<IndexingRequestedEvent> { this.documentVersionId == documentVersionId }

    private fun record(
        key: String,
        value: String,
    ) = ConsumerRecord<String, String>("indexing", 0, 0L, key, value)

    private fun indexingRequestedJson(
        documentId: Long,
        documentVersionId: Long = 1001,
        traceId: String? = null,
    ) = """
        {"eventId":"${java.util.UUID.randomUUID()}","eventType":"INDEXING_REQUESTED",
         "eventSchemaVersion":1,"tenantId":7,"documentId":$documentId,"documentVersionId":$documentVersionId,
         "occurredAt":"2026-08-16T09:14:22Z","traceId":${traceId?.let { "\"$it\"" } ?: "null"}}
        """.trimIndent()

    private fun documentDeletedJson() =
        """
        {"eventId":"${java.util.UUID.randomUUID()}","eventType":"DOCUMENT_DELETED",
         "eventSchemaVersion":1,"tenantId":7,"documentId":42,"documentVersionId":null,
         "occurredAt":"2026-08-16T09:20:00Z","traceId":null}
        """.trimIndent()
}
