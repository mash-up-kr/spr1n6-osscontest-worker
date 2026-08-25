package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

/** Kafka 파티션 회수가 발생할 때 리밸런스 횟수를 기록한다. */
class RebalanceMetricsListener(
    private val meterRegistry: MeterRegistry,
    private val workerId: String,
) : ConsumerRebalanceListener {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        if (partitions.isNotEmpty()) {
            meterRegistry.counter("kafka_rebalance_total").increment()
            log.info(
                "KAFKA_PARTITIONS_REVOKED workerId={} partitions={}",
                workerId,
                partitions.toLogValues(),
            )
        }
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        if (partitions.isNotEmpty()) {
            log.info(
                "KAFKA_PARTITIONS_ASSIGNED workerId={} partitions={}",
                workerId,
                partitions.toLogValues(),
            )
        }
    }

    private fun Collection<TopicPartition>.toLogValues(): List<String> =
        map { "${it.topic()}:${it.partition()}" }.sorted()
}
