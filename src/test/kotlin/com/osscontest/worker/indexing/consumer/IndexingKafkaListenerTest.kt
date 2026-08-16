package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.kafka.support.Acknowledgment
import tools.jackson.module.kotlin.jacksonObjectMapper

// Jackson 3.x(tools.jackson.*) 사용 프로젝트다 — com.fasterxml.jackson.*가 아니다.
// java.time(occurredAt: Instant) 지원이 jackson-databind에 내장돼 있어 별도 JavaTimeModule
// 등록이 필요 없다. 대신 Kotlin data class(생성자 기반 역직렬화, nullable/non-null 필드 처리)를
// 올바르게 다루려면 jackson-module-kotlin의 KotlinModule이 등록돼야 한다 — jacksonObjectMapper()가
// 그 등록까지 포함한 ObjectMapper를 만들어주는 이 모듈의 관용적인 진입점이다. 프로덕션
// ObjectMapper 빈(Spring Boot 자동 설정)도 클래스패스에서 이 모듈을 자동으로 찾아 등록한다.
class IndexingKafkaListenerTest {
    private val runner: IndexingPipelineRunner = mock()
    private val deletionHandler: DocumentDeletionHandler = mock()
    private val objectMapper = jacksonObjectMapper()
    private val listener = IndexingKafkaListener(runner, deletionHandler, objectMapper)
    private val ack: Acknowledgment = mock()

    @Test
    fun `INDEXING_REQUESTED는 PipelineRunner로 보내고 항상 ack한다`() {
        val record = record(indexingRequestedJson())

        listener.onMessage(record, ack)

        verify(runner, times(1)).run(any())
        verify(ack).acknowledge()
    }

    @Test
    fun `DOCUMENT_DELETED는 DeletionHandler로 보내고 항상 ack한다`() {
        val record = record(documentDeletedJson())

        listener.onMessage(record, ack)

        verify(deletionHandler, times(1)).handle(any())
        verify(runner, never()).run(any())
        verify(ack).acknowledge()
    }

    @Test
    fun `비즈니스 예외가 나도 ack한다`() {
        whenever(runner.run(any())).doThrow(RuntimeException("boom"))
        val record = record(indexingRequestedJson())

        listener.onMessage(record, ack)

        verify(ack).acknowledge()
    }

    @Test
    fun `역직렬화 실패해도 ack한다`() {
        val record = record("not-json")

        listener.onMessage(record, ack)

        verify(ack).acknowledge()
    }

    @Test
    fun `알 수 없는 eventType은 아무 핸들러도 타지 않고 ack한다`() {
        val record = record(unknownEventTypeJson())

        listener.onMessage(record, ack)

        verify(runner, never()).run(any())
        verify(deletionHandler, never()).handle(any())
        verify(ack).acknowledge()
    }

    private fun record(value: String) =
        ConsumerRecord<String, String>("indexing", 0, 0L, "42", value)

    private fun indexingRequestedJson() =
        """
        {"eventId":"9f2b1c4a-3e5d-4a71-b8c2-1f0e7d9a4c33","eventType":"INDEXING_REQUESTED",
         "eventSchemaVersion":1,"tenantId":7,"documentId":42,"documentVersionId":1001,
         "occurredAt":"2026-08-16T09:14:22Z","traceId":null}
        """.trimIndent()

    private fun documentDeletedJson() =
        """
        {"eventId":"5e6b2b1a-1111-4a71-b8c2-1f0e7d9a4c99","eventType":"DOCUMENT_DELETED",
         "eventSchemaVersion":1,"tenantId":7,"documentId":42,"documentVersionId":null,
         "occurredAt":"2026-08-16T09:20:00Z","traceId":null}
        """.trimIndent()

    private fun unknownEventTypeJson() =
        """
        {"eventId":"1a2b3c4d-2222-4a71-b8c2-1f0e7d9a4c11","eventType":"FOO",
         "eventSchemaVersion":1,"tenantId":7,"documentId":42,"documentVersionId":1001,
         "occurredAt":"2026-08-16T09:25:00Z","traceId":null}
        """.trimIndent()
}
