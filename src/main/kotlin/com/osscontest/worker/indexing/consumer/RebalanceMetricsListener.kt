package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

// P1-2: 오르면 P0-3의 재시도 예산 계산이 실제로 틀렸다는 신호다(FAULT_TOLERANCE.md §3 P1-2
// 핵심 지표). Kafka listener container에 연결하는 배선은 별도 Config 클래스에서 한다 —
// 이 클래스 자체는 순수하게 카운터 증가 로직만 담당해 단위 테스트가 가능하다.
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
