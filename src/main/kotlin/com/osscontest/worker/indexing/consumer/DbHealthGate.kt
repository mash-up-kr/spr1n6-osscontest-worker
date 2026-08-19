package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// P0-5-b: DB가 흔들리는 동안 리스너를 pause해 Task 1의 nack이 5초마다 같은 배치를
// 재전달받는 hot loop가 되는 걸 막는다. pause()는 poll()만 멈추고 heartbeat/session은
// 그대로 유지되므로 리밸런스가 나지 않는다(FAULT_TOLERANCE.md §3 P0-5-(b)).
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
