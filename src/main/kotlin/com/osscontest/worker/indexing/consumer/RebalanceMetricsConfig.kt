package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ContainerCustomizer
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class RebalanceMetricsConfig {
    // 이 ContainerCustomizer는 기본 컨테이너 팩토리를 쓰는 모든 @KafkaListener에 전역으로 적용된다.
    // 지금은 리스너가 "indexing" 하나뿐이라 안전하지만, 두 번째 @KafkaListener가 같은 기본 팩토리를
    // 쓰면 이 리밸런스 리스너를 의도치 않게 그대로 물려받는다 — 두 번째 리스너를 추가할 때는
    // 전용 컨테이너 팩토리로 분리하거나 이 커스터마이저의 적용 범위를 좁혀야 한다.
    @Bean
    fun rebalanceMetricsContainerCustomizer(
        meterRegistry: MeterRegistry,
    ): ContainerCustomizer<String, String, ConcurrentMessageListenerContainer<String, String>> =
        ContainerCustomizer { container ->
            container.containerProperties.setConsumerRebalanceListener(RebalanceMetricsListener(meterRegistry))
        }
}
