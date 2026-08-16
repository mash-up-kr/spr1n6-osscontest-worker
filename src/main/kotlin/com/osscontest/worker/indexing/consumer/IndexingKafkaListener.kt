package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// 토픽 이름은 이벤트 타입에 종속되지 않게 짓는다("indexing.requested"가 아니라 "indexing") —
// INDEXING_REQUESTED와 DOCUMENT_DELETED가 같은 토픽·같은 파티션(documentId 키)으로 온다.
// 별도 토픽으로 쪼개면 "업로드 뒤 삭제"류 순서 보장이 깨진다(스펙 §0.3).
@Component
class IndexingKafkaListener(
    private val pipelineRunner: IndexingPipelineRunner,
    private val deletionHandler: DocumentDeletionHandler,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["indexing"], id = "indexing")
    fun onMessage(
        record: ConsumerRecord<String, String>,
        ack: Acknowledgment,
    ) {
        // 역직렬화가 성공한 뒤부터는 이후 catch 블록에서도 event.eventId/documentId를 로그에
        // 남길 수 있도록 try 바깥에 선언한다. DeserializationException 쪽은 애초에 event가
        // 없으므로 null로 남는다.
        var event: IndexingRequestedEvent? = null
        try {
            event = deserialize(record.value())
            when (event.eventType) {
                "INDEXING_REQUESTED" -> pipelineRunner.run(event)
                "DOCUMENT_DELETED" -> deletionHandler.handle(event)
                else ->
                    throw InvalidEventException(
                        "UNKNOWN_EVENT_TYPE",
                        "eventType=${event.eventType} is not recognized",
                    )
            }
        } catch (e: DeserializationException) {
            log.error("event schema invalid: partition={} offset={}", record.partition(), record.offset(), e)
        } catch (e: InvalidEventException) {
            log.error(
                "event validation failed: code={} eventId={} documentId={} partition={} offset={}",
                e.code, event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        } catch (e: Exception) {
            // Throwable/Error(예: StackOverflowError)는 의도적으로 여기서 잡지 않고 그대로 전파한다.
            // finally는 어차피 실행되어 ack는 보장되지만, 잡히지 않은 Error가 리스너 스레드에
            // 영향을 줄 수 있다는 걸 감수한 트레이드오프이지 실수가 아니다.
            log.error(
                "indexing failed: eventId={} documentId={} partition={} offset={}",
                event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        } finally {
            ack.acknowledge()
        }
    }

    private fun deserialize(value: String): IndexingRequestedEvent =
        try {
            objectMapper.readValue(value, IndexingRequestedEvent::class.java)
        } catch (e: Exception) {
            throw DeserializationException(e)
        }
}
