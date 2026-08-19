package com.osscontest.worker.indexing.consumer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer

class DbHealthGateTest {
    private val registry: KafkaListenerEndpointRegistry = mock()
    private val jdbcTemplate: JdbcTemplate = mock()
    private val container: MessageListenerContainer = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val gate = DbHealthGate(registry, jdbcTemplate, meterRegistry)

    @Test
    fun `DB SELECT 1이 실패하고 컨테이너가 실행 중이면 pause한다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java))
            .thenThrow(RuntimeException("connection refused"))
        whenever(container.isRunning).thenReturn(true)

        gate.check()

        verify(container).pause()
        verify(container, never()).resume()
    }

    @Test
    fun `DB가 정상이고 pause 요청 상태면 resume한다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java)).thenReturn(1)
        whenever(container.isPauseRequested).thenReturn(true)

        gate.check()

        verify(container).resume()
        verify(container, never()).pause()
    }

    @Test
    fun `DB가 정상이고 pause 상태가 아니면 아무것도 하지 않는다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java)).thenReturn(1)
        whenever(container.isPauseRequested).thenReturn(false)

        gate.check()

        verify(container, never()).pause()
        verify(container, never()).resume()
    }

    @Test
    fun `pause 전이 때 db_health_gate_paused_total이 증가한다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java))
            .thenThrow(RuntimeException("down"))
        whenever(container.isRunning).thenReturn(true)

        gate.check()

        assertThat(meterRegistry.get("db_health_gate_paused_total").counter().count()).isEqualTo(1.0)
    }

    @Test
    fun `DB가 계속 비정상이어도 이미 pause 요청 상태면 다시 pause하거나 메트릭을 증가시키지 않는다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(container)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java))
            .thenThrow(RuntimeException("down"))
        whenever(container.isRunning).thenReturn(true)
        whenever(container.isPauseRequested).thenReturn(true)

        gate.check()

        verify(container, never()).pause()
        assertThat(meterRegistry.find("db_health_gate_paused_total").counters()).isEmpty()
    }

    @Test
    fun `컨테이너를 찾지 못하면 아무것도 하지 않는다`() {
        whenever(registry.getListenerContainer("indexing")).thenReturn(null)
        whenever(jdbcTemplate.queryForObject("SELECT 1", Int::class.java)).thenReturn(1)

        gate.check()
        // 예외 없이 조용히 반환하면 통과 — verify할 대상 자체가 없다.
    }
}
