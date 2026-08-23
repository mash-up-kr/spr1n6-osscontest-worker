package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

/** Kafka 파티션 회수가 발생할 때 리밸런스 횟수를 기록한다. */
class RebalanceMetricsListener(
    private val meterRegistry: MeterRegistry,
) : ConsumerRebalanceListener {
    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        if (partitions.isNotEmpty()) {
            meterRegistry.counter("kafka_rebalance_total").increment()
        }
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) = Unit
}
