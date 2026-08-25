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
// 별도 토픽으로 쪼개면 업로드와 삭제의 순서 보장이 깨진다.
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
    // Kafka offset은 배치 단위로, DB 상태는 Job 단위로 확정한다.
    @KafkaListener(topics = ["\${indexing.consumer.topic}"], id = "indexing")
    fun onMessage(
        records: List<ConsumerRecord<String, String>>,
        ack: Acknowledgment,
    ) {
        val batchStartedAt = System.nanoTime()
        val recordsByDocument = records.groupBy { it.key() }
        log.info(
            "Kafka batch received: recordCount={} documentGroupCount={}",
            records.size,
            recordsByDocument.size,
        )
        val futures =
            recordsByDocument
                .values
                .map { sameKeyRecords -> executor.submit { sameKeyRecords.forEach(::processRecord) } }

        // DB에 처리 결과를 기록하지 못한 실패는 ack 대신 nack한다. 배치 안 다른 documentId
        // 그룹이 아직 처리 중일 수 있으므로 DB 장애를 감지해도 나머지 future를 전부 기다린다.
        // 그래야 아직 실행 중인 작업이 고아로 남지 않고, 다른 그룹의 실패도 로그에서 안 사라진다.
        var dbFailure: Throwable? = null
        for (future in futures) {
            try {
                future.get()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            } catch (e: ExecutionException) {
                if (e.cause is DataAccessException) {
                    if (dbFailure == null) dbFailure = e.cause
                    log.warn("DB unavailable while processing batch", e.cause)
                } else {
                    // 예상하지 못한 비-DB 실패는 즉시 전파해 ack하지 않고 배치를 재전달받는다.
                    throw e
                }
            }
        }

        if (dbFailure != null) {
            // 배치 리스너라 레코드 하나가 아니라 배치 전체(인덱스 0부터)가 되감긴다.
            // 이미 성공적으로 처리된 다른 documentId 그룹까지 재전달되지만,
            // 이미 성공한 결과는 UPSERT로 같은 상태에 수렴한다.
            log.warn(
                "Kafka batch nacked: recordCount={} retryDelayMs={} durationMs={}",
                records.size,
                nackDelay.toMillis(),
                elapsedMillis(batchStartedAt),
            )
            ack.nack(0, nackDelay)
            return
        }
        ack.acknowledge()
        log.info(
            "KAFKA_BATCH_ACK batchSize={} records={} durationMs={}",
            records.size,
            records.map { "${it.partition()}:${it.offset()}" },
            elapsedMillis(batchStartedAt),
        )
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        val eventStartedAt = System.nanoTime()
        var event: IndexingEvent? = null
        try {
            val traceId = extractTraceId(record)
            MDC.put("traceId", traceId ?: "-")
            event = deserialize(record.value()).copy(traceId = traceId)
            log.info(
                "INDEXING_EVENT_RECEIVED sourceEventId={} documentId={} documentVersionId={} " +
                    "workerId={} eventType={} topic={} partition={} offset={}",
                event.eventId,
                event.documentId,
                event.documentVersionId,
                pipelineRunner.currentWorkerId(),
                event.eventType,
                record.topic(),
                record.partition(),
                record.offset(),
            )
            when (event.eventType) {
                "INDEXING_REQUESTED" ->
                    pipelineRunner.run(
                        event,
                        KafkaRecordIdentity(
                            topic = record.topic(),
                            partition = record.partition(),
                            offset = record.offset(),
                        ),
                    )
                "DOCUMENT_DELETED" -> deletionHandler.handle(event)
                else ->
                    throw InvalidEventException(
                        "UNKNOWN_EVENT_TYPE",
                        "eventType=${event.eventType} is not recognized",
                    )
            }
            log.info(
                "event handling completed: eventType={} eventId={} documentId={} durationMs={}",
                event.eventType,
                event.eventId,
                event.documentId,
                elapsedMillis(eventStartedAt),
            )
        } catch (e: DataAccessException) {
            // 여기서 삼키지 않는다 — onMessage()의 futures.forEach { it.get() }가
            // ExecutionException으로 다시 던지도록 그대로 전파한다.
            log.error(
                "event handling failed due to DB error: eventType={} eventId={} documentId={} " +
                    "topic={} partition={} offset={} durationMs={} errorType={}",
                event?.eventType,
                event?.eventId,
                event?.documentId,
                record.topic(),
                record.partition(),
                record.offset(),
                elapsedMillis(eventStartedAt),
                e::class.simpleName,
                e,
            )
            throw e
        } catch (e: DeserializationException) {
            // 형식이 깨진 이벤트는 같은 record를 다시 받아도 복구되지 않는다.
            // 운영 정책에 따라 오류 로그를 남기고 소비해 나머지 배치의 진행을 보장한다.
            log.error(
                "event deserialization failed: topic={} partition={} offset={} durationMs={} errorType={}",
                record.topic(),
                record.partition(),
                record.offset(),
                elapsedMillis(eventStartedAt),
                e.cause?.let { it::class.simpleName } ?: e::class.simpleName,
                e,
            )
        } catch (e: InvalidEventException) {
            // 지원하지 않는 계약이나 식별자 불일치는 영구 오류다.
            // 운영 정책에 따라 오류 로그를 남기고 소비하며 재전달하지 않는다.
            log.error(
                "event validation failed: errorCode={} eventType={} eventId={} documentId={} " +
                    "topic={} partition={} offset={} durationMs={}",
                e.code,
                event?.eventType,
                event?.eventId,
                event?.documentId,
                record.topic(),
                record.partition(),
                record.offset(),
                elapsedMillis(eventStartedAt),
                e,
            )
        } catch (e: Exception) {
            log.error(
                "event handling failed: eventType={} eventId={} documentId={} topic={} partition={} " +
                    "offset={} durationMs={} errorType={}",
                event?.eventType,
                event?.eventId,
                event?.documentId,
                record.topic(),
                record.partition(),
                record.offset(),
                elapsedMillis(eventStartedAt),
                e::class.simpleName,
                e,
            )
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
            }
            throw e
        } finally {
            MDC.remove("traceId")
        }
    }

    private fun deserialize(value: String): IndexingEvent =
        try {
            objectMapper.readValue(value, IndexingEvent::class.java)
        } catch (e: Exception) {
            throw DeserializationException(e)
        }

    private fun extractTraceId(record: ConsumerRecord<String, String>): String? =
        record
            .headers()
            .lastHeader(TRACE_ID_HEADER)
            ?.value()
            ?.toString(Charsets.UTF_8)
            ?.takeIf { it.isNotBlank() }

    private fun elapsedMillis(startedAt: Long): Long =
        (System.nanoTime() - startedAt) / 1_000_000

    private companion object {
        const val TRACE_ID_HEADER = "traceId"
    }
}
