package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RebalanceMetricsListenerTest {
    private val meterRegistry = SimpleMeterRegistry()
    private val listener = RebalanceMetricsListener(meterRegistry)

    @Test
    fun `파티션이 회수되면 kafka_rebalance_total이 증가한다`() {
        listener.onPartitionsRevoked(listOf(TopicPartition("indexing", 0)))

        assertThat(meterRegistry.get("kafka_rebalance_total").counter().count()).isEqualTo(1.0)
    }

    @Test
    fun `빈 파티션 목록으로 호출되면(리밸런스 없음) 증가하지 않는다`() {
        listener.onPartitionsRevoked(emptyList())

        assertThat(meterRegistry.find("kafka_rebalance_total").counters()).isEmpty()
    }

    @Test
    fun `파티션 할당 콜백은 카운터를 건드리지 않는다`() {
        listener.onPartitionsAssigned(listOf(TopicPartition("indexing", 0)))

        assertThat(meterRegistry.find("kafka_rebalance_total").counters()).isEmpty()
    }
}
