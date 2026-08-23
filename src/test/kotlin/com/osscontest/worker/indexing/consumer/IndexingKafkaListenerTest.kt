package com.osscontest.worker.indexing.consumer

import ch.qos.logback.classic.Logger
import ch.qos.logback.core.read.ListAppender
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.kafka.support.Acknowledgment
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.time.Duration
import java.util.concurrent.ExecutionException
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

        verify(runner, times(2)).run(any(), any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `같은 documentId(key)의 이벤트는 원래 순서대로 처리한다`() {
        val v1 = record(key = "1", value = indexingRequestedJson(documentId = 1, documentVersionId = 1))
        val v2 = record(key = "1", value = indexingRequestedJson(documentId = 1, documentVersionId = 2))

        listener.onMessage(listOf(v1, v2), ack)

        val order = inOrder(runner)
        order.verify(runner).run(argThatVersionIs(1), any())
        order.verify(runner).run(argThatVersionIs(2), any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `DOCUMENT_DELETED는 DeletionHandler로 보낸다`() {
        val record = record(key = "1", value = documentDeletedJson())

        listener.onMessage(listOf(record), ack)

        verify(deletionHandler, times(1)).handle(any())
        verify(runner, never()).run(any(), any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `인덱싱 처리에서 예상하지 못한 예외가 나면 ack하지 않고 전파한다`() {
        whenever(runner.run(any(), any())).doThrow(RuntimeException("boom"))
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1)))

        val thrown = assertThrows<ExecutionException> { listener.onMessage(records, ack) }

        assertThat(thrown.cause).isInstanceOf(RuntimeException::class.java)
        verify(ack, never()).acknowledge()
        verify(ack, never()).nack(any(), any())
    }

    @Test
    fun `역직렬화 실패해도 나머지 레코드는 처리하고 ack한다`() {
        val records =
            listOf(
                record(key = "1", value = "not-json"),
                record(key = "2", value = indexingRequestedJson(documentId = 2)),
            )

        listener.onMessage(records, ack)

        verify(runner, times(1)).run(any(), any())
        verify(ack, times(1)).acknowledge()
    }

    @Test
    fun `DB 장애(DataAccessException)면 ack 대신 배치 전체를 nack한다`() {
        whenever(runner.run(any(), any())).thenThrow(DataAccessResourceFailureException("connection refused"))
        val records = listOf(record(key = "1", value = indexingRequestedJson(documentId = 1)))

        listener.onMessage(records, ack)

        verify(ack, never()).acknowledge()
        verify(ack, times(1)).nack(0, Duration.ofSeconds(5))
    }

    @Test
    fun `DB 장애가 일부 그룹에서만 나도 다른 그룹은 끝까지 처리된 뒤 nack한다`() {
        var otherGroupProcessed = false
        whenever(runner.run(argThatVersionIs(1), any())).thenThrow(DataAccessResourceFailureException("connection refused"))
        whenever(runner.run(argThatVersionIs(2), any())).thenAnswer { otherGroupProcessed = true; null }
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
    fun `삭제 처리에서 예상하지 못한 예외가 나면 ack하지 않고 전파한다`() {
        whenever(deletionHandler.handle(any())).thenThrow(RuntimeException("boom"))
        val records = listOf(record(key = "1", value = documentDeletedJson()))

        val thrown = assertThrows<ExecutionException> { listener.onMessage(records, ack) }

        assertThat(thrown.cause).isInstanceOf(RuntimeException::class.java)
        verify(ack, never()).acknowledge()
        verify(ack, never()).nack(any(), any())
    }

    @Test
    fun `Kafka 헤더 traceId는 이벤트와 MDC에 설정되고 처리 후 worker thread에서 제거된다`() {
        var capturedDuringCall: String? = null
        var capturedEventTraceId: String? = null
        whenever(runner.run(any(), any())).thenAnswer { invocation ->
            capturedDuringCall = MDC.get("traceId")
            capturedEventTraceId = invocation.getArgument<IndexingEvent>(0).traceId
            null
        }
        val records =
            listOf(
                record(
                    key = "1",
                    value = indexingRequestedJson(documentId = 1),
                    traceId = "abc-123",
                ),
            )

        listener.onMessage(records, ack)
        val capturedAfterCall = executor.submit<String?> { MDC.get("traceId") }.get()

        assertThat(capturedDuringCall).isEqualTo("abc-123")
        assertThat(capturedEventTraceId).isEqualTo("abc-123")
        assertThat(capturedAfterCall).isNull()
    }

    @Test
    fun `이벤트 수신 로그에는 traceId와 Kafka 위치가 포함된다`() {
        val logger = LoggerFactory.getLogger(IndexingKafkaListener::class.java) as Logger
        val appender = ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)

        try {
            val record =
                record(
                    key = "1",
                    value = indexingRequestedJson(documentId = 1),
                    partition = 3,
                    offset = 99L,
                    traceId = "trace-log-123",
                )

            listener.onMessage(listOf(record), ack)

            val receivedLog =
                appender.list.first { it.formattedMessage.startsWith("event received:") }
            assertThat(receivedLog.mdcPropertyMap["traceId"]).isEqualTo("trace-log-123")
            assertThat(receivedLog.formattedMessage)
                .contains("eventType=INDEXING_REQUESTED")
                .contains("documentId=1")
                .contains("topic=indexing partition=3 offset=99")
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun `INDEXING_REQUESTED의 Kafka record identity를 Runner에 전달한다`() {
        val record =
            record(
                key = "1",
                value = indexingRequestedJson(documentId = 1),
                partition = 3,
                offset = 99L,
            )

        listener.onMessage(listOf(record), ack)

        verify(runner).run(any(), eq(KafkaRecordIdentity(topic = "indexing", partition = 3, offset = 99L)))
    }

    private fun argThatVersionIs(documentVersionId: Long) =
        org.mockito.kotlin.argThat<IndexingEvent> { this.documentVersionId == documentVersionId }

    private fun record(
        key: String,
        value: String,
        partition: Int = 0,
        offset: Long = 0L,
        traceId: String? = null,
    ) = ConsumerRecord<String, String>("indexing", partition, offset, key, value)
        .also { record ->
            traceId?.let { record.headers().add("traceId", it.toByteArray(Charsets.UTF_8)) }
        }

    private fun indexingRequestedJson(
        documentId: Long,
        documentVersionId: Long = 1001,
    ) = """
        {"eventId":"${java.util.UUID.randomUUID()}","eventType":"INDEXING_REQUESTED",
         "schemaVersion":1,"tenantId":7,"documentId":$documentId,"documentVersionId":$documentVersionId,
         "occurredAt":"2026-08-16T09:14:22Z"}
        """.trimIndent()

    private fun documentDeletedJson() =
        """
        {"eventId":"${java.util.UUID.randomUUID()}","eventType":"DOCUMENT_DELETED",
         "schemaVersion":1,"tenantId":7,"documentId":42,"documentVersionId":null,
         "occurredAt":"2026-08-16T09:20:00Z"}
        """.trimIndent()
}
