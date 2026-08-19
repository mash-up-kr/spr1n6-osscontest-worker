package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.ExecutionException
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
    @Value("\${indexing.db-health-gate.pause-nack-delay:PT5S}")
    private val nackDelay: Duration = Duration.ofSeconds(5),
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
        val futures =
            records
                .groupBy { it.key() }
                .values
                .map { sameKeyRecords -> executor.submit { sameKeyRecords.forEach(::processRecord) } }

        // P0-5: DB에 아무것도 기록하지 못한 실패는 ack이 아니라 nack한다 — always-ack 원칙의
        // 유일한 예외(FAULT_TOLERANCE.md §3 P0-5-(c)). 배치 안 다른 documentId 그룹이 아직 처리
        // 중일 수 있으므로, DB 장애를 감지해도 즉시 반환하지 않고 나머지 future도 전부 기다린다 —
        // 그래야 아직 실행 중인 작업이 고아로 남지 않고, 다른 그룹의 실패도 로그에서 안 사라진다.
        var dbFailure: Throwable? = null
        for (future in futures) {
            try {
                future.get()
            } catch (e: ExecutionException) {
                if (e.cause is DataAccessException) {
                    if (dbFailure == null) dbFailure = e.cause
                    log.warn("DB unavailable while processing batch", e.cause)
                } else {
                    // 예상 못 한 실패(DataAccessException 아님)는 기존과 동일하게 즉시 전파해
                    // 컨테이너가 배치를 통째로 재전달받게 한다. 나머지 future를 기다리지 않는다 —
                    // 워커가 불안정하면 최대한 빨리 벗어나는 게 낫다는 기존 판단을 그대로 유지한다.
                    throw e
                }
            }
        }

        if (dbFailure != null) {
            // 배치 리스너라 레코드 하나가 아니라 배치 전체(인덱스 0부터)가 되감긴다.
            // 이미 성공적으로 처리된 다른 documentId 그룹까지 재전달되지만,
            // UPSERT 수렴(§1.4-(2))으로 무해하다.
            ack.nack(0, nackDelay)
            return
        }
        ack.acknowledge()
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        var event: IndexingRequestedEvent? = null
        try {
            event = deserialize(record.value())
            MDC.put("traceId", event.traceId ?: "-")
            when (event.eventType) {
                "INDEXING_REQUESTED" -> pipelineRunner.run(event)
                "DOCUMENT_DELETED" -> deletionHandler.handle(event)
                else ->
                    throw InvalidEventException(
                        "UNKNOWN_EVENT_TYPE",
                        "eventType=${event.eventType} is not recognized",
                    )
            }
        } catch (e: DataAccessException) {
            // 여기서 삼키지 않는다 — onMessage()의 futures.forEach { it.get() }가
            // ExecutionException으로 다시 던지도록 그대로 전파한다(P0-5).
            throw e
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
        } finally {
            MDC.remove("traceId")
        }
    }

    private fun deserialize(value: String): IndexingRequestedEvent =
        try {
            objectMapper.readValue(value, IndexingRequestedEvent::class.java)
        } catch (e: Exception) {
            throw DeserializationException(e)
        }
}
