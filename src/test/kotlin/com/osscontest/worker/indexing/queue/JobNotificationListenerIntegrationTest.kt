/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import java.sql.Connection
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.postgresql.PGConnection
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource

@Tag("integration")
class JobNotificationListenerIntegrationTest {
    private val channel = "worker_notify_${UUID.randomUUID().toString().replace("-", "")}"
    private val polls = LinkedBlockingQueue<Unit>()
    private val listeningConnections = CopyOnWriteArrayList<Connection>()
    private lateinit var producer: Connection
    private lateinit var listener: JobNotificationListener

    @BeforeEach
    fun start() {
        val url = requireNotNull(System.getenv("DB_URL"))
        val properties = JobNotificationProperties(channel = channel, waitTimeout = Duration.ofMillis(50),
            healthInterval = Duration.ofMillis(100), reconnectDelay = Duration.ofMillis(20))
        val database = DataSourceProperties().apply {
            this.url = url
            username = System.getenv("DB_USERNAME")
            password = System.getenv("DB_PASSWORD").orEmpty()
        }
        val connections = PostgresJobNotificationConnections(database, "", 3, 3)
        producer = connections.open()
        assertDedicatedIntegrationDatabase(JdbcTemplate(SingleConnectionDataSource(producer, true)))
        val worker: DbJobWorker = mock()
        whenever(worker.pollRequester()).thenReturn({ polls.add(Unit); Unit })
        listener = JobNotificationListener(properties, JobNotificationConnections {
            connections.open().also(listeningConnections::add)
        }, worker, DatabaseWaitProperties())
        listener.start()
        awaitPoll()
    }

    @AfterEach
    fun stop() {
        if (::listener.isInitialized) listener.stop()
        listeningConnections.forEach { it.close() }
        if (::producer.isInitialized) producer.close()
    }

    @Test
    fun `커밋된 알림만 수신하고 여러 알림과 유휴 상태에서도 같은 물리 연결을 유지한다`() {
        producer.autoCommit = false
        notifyJob()
        assertThat(polls.poll(150, TimeUnit.MILLISECONDS)).isNull()
        producer.commit()
        awaitPoll()
        notifyJob(); producer.rollback()
        assertThat(polls.poll(150, TimeUnit.MILLISECONDS)).isNull()
        producer.autoCommit = true
        repeat(3) { notifyJob(); awaitPoll() }
        assertThat(listeningConnections).hasSize(1)
        assertThat(listeningConnections.single().isClosed).isFalse()
        listener.stop()
        await().atMost(Duration.ofSeconds(3)).untilAsserted {
            assertThat(listeningConnections.single().isClosed).isTrue()
        }
    }

    @Test
    fun `서버가 LISTEN 세션을 종료하면 재접속 직후 탐색하고 이후 알림도 받는다`() {
        val pid = listeningConnections.single().unwrap(PGConnection::class.java).backendPID
        producer.prepareStatement("SELECT pg_terminate_backend(?)").use {
            it.setInt(1, pid)
            it.executeQuery().use { result -> assertThat(result.next() && result.getBoolean(1)).isTrue() }
        }
        awaitPoll()
        assertThat(listeningConnections).hasSize(2)
        assertThat(listeningConnections.first().isClosed).isTrue()
        notifyJob(); awaitPoll()
    }

    private fun notifyJob() {
        producer.prepareStatement("SELECT pg_notify(?, ?)").use {
            it.setString(1, channel)
            it.setString(2, "")
            it.execute()
        }
    }

    private fun awaitPoll() { assertThat(polls.poll(3, TimeUnit.SECONDS)).isNotNull() }
}
