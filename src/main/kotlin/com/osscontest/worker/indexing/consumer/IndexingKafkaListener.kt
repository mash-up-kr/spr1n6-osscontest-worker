package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.util.concurrent.ExecutorService

// 토픽 이름은 이벤트 타입에 종속되지 않게 짓는다("indexing.requested"가 아니라 "indexing") —
// INDEXING_REQUESTED와 DOCUMENT_DELETED가 같은 토픽·같은 파티션(documentId 키)으로 온다.
// 별도 토픽으로 쪼개면 "업로드 뒤 삭제"류 순서 보장이 깨진다(스펙 §0.3).
@Component
class IndexingKafkaListener(
    private val pipelineRunner: IndexingPipelineRunner,
    private val deletionHandler: DocumentDeletionHandler,
    private val objectMapper: ObjectMapper,
    @Qualifier("indexingBatchExecutor") private val executor: ExecutorService,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // spring.kafka.listener.type=batch + max-poll-records로 배치 크기가 정해진다(application.yml).
    // documentId(=메시지 key)로 그룹핑해 같은 문서의 이벤트는 순서대로, 서로 다른 문서는
    // 동시에 처리한다 — 배치 전체가 끝나야(모든 그룹의 future가 끝나야) 한 번만 ack한다.
    // §3.1의 "Kafka offset은 배치 전체를 봤다, DB status는 각 Job을 처리했다" 원칙.
    @KafkaListener(topics = ["indexing"], id = "indexing")
    fun onMessage(
        records: List<ConsumerRecord<String, String>>,
        ack: Acknowledgment,
    ) {
        records
            .groupBy { it.key() }
            .values
            .map { sameKeyRecords -> executor.submit { sameKeyRecords.forEach(::processRecord) } }
            .forEach { it.get() }
        ack.acknowledge()
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
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
            // Throwable/Error(예: StackOverflowError)는 의도적으로 여기서 잡지 않고 그대로 전파한다 —
            // 이 documentId 그룹의 future를 실패시켜 onMessage()의 forEach { it.get() }에서 다시
            // 던져지고, 배치 ack 자체가 안 이뤄진다(워커가 정말 불안정하면 배치를 통째로 재전달받는
            // 게 낫다는 판단).
            log.error(
                "indexing failed: eventId={} documentId={} partition={} offset={}",
                event?.eventId, event?.documentId, record.partition(), record.offset(), e,
            )
        }
    }

    private fun deserialize(value: String): IndexingRequestedEvent =
        try {
            objectMapper.readValue(value, IndexingRequestedEvent::class.java)
        } catch (e: Exception) {
            throw DeserializationException(e)
        }
}
