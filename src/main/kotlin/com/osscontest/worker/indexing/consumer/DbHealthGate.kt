package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// DB 장애 중에는 리스너를 pause해 nack된 배치가 짧은 간격으로 반복되는 것을 막는다.
// pause는 poll만 멈추고 heartbeat는 유지하므로 불필요한 리밸런스를 피할 수 있다.
@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
    private val meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${indexing.db-health-gate.check-interval-ms:5000}")
    fun check() {
        val healthy = runCatching { jdbcTemplate.queryForObject("SELECT 1", Int::class.java) }.isSuccess
        val container = registry.getListenerContainer(LISTENER_ID) ?: return
        when {
            !healthy && container.isRunning && !container.isPauseRequested -> {
                container.pause()
                meterRegistry.counter("db_health_gate_paused_total").increment()
                log.warn("DB down — consumer paused")
            }
            healthy && container.isPauseRequested -> {
                container.resume()
                log.info("DB up — consumer resumed")
            }
        }
    }

    private companion object {
        const val LISTENER_ID = "indexing"
    }
}
