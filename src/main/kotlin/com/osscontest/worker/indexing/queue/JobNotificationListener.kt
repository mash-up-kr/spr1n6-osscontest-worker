/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component

/** 알림은 탐색 요청으로만 넘긴다. 수신 연결의 장애는 기존 claim·heartbeat의 DB 장애 상태와 분리한다. */
@Component
@ConditionalOnProperty(prefix = "indexing.queue", name = ["enabled", "notifications.enabled"],
    havingValue = "true", matchIfMissing = true)
class JobNotificationListener(
    private val properties: JobNotificationProperties,
    private val connections: JobNotificationConnections,
    private val worker: DbJobWorker,
    private val databaseProperties: DatabaseWaitProperties,
) : SmartLifecycle {
    private class Generation(val requestPoll: () -> Unit) {
        val running = AtomicBoolean(true)
        var connection: Connection? = null
        lateinit var thread: Thread
    }

    private val lifecycle = Any()
    @Volatile private var current: Generation? = null
    private val log = LoggerFactory.getLogger(javaClass)

    override fun isRunning() = current?.running?.get() == true

    // 워커가 시작된 뒤 구독하고, 워커가 멈추기 전에 수신 연결을 닫는다.
    override fun getPhase() = 1

    override fun start() = synchronized(lifecycle) {
        if (isRunning()) return@synchronized
        val generation = Generation(worker.pollRequester())
        generation.thread = Thread({ listen(generation) }, "job-notification-listener")
        current = generation
        generation.thread.start()
    }

    override fun stop() {
        val generation = synchronized(lifecycle) {
            val generation = current ?: return
            current = null
            generation.running.set(false)
            generation
        }
        generation.thread.interrupt()
        val connection = synchronized(lifecycle) {
            generation.connection.also { generation.connection = null }
        }
        // 수신 대기는 인터럽트만으로 끝나지 않으므로 소켓도 닫는다.
        if (connection != null) close(connection)
    }

    private fun listen(generation: Generation) {
        var delay = properties.reconnectDelay.toMillis()
        while (generation.running.get()) {
            try {
                val connection = connections.open()
                try {
                    synchronized(lifecycle) {
                        if (current !== generation || !generation.running.get()) return
                        generation.connection = connection
                    }
                    subscribe(connection)
                    if (!generation.running.get()) return
                    log.info("JOB_LISTENER_CONNECTED channel={}", properties.channel)
                    // LISTEN 커밋 뒤 조회해야 연결이 없던 동안의 Job과 구독 시점의 경합을 모두 보완한다.
                    generation.requestPoll()
                    delay = properties.reconnectDelay.toMillis()
                    receive(generation, connection)
                } finally {
                    synchronized(lifecycle) {
                        if (generation.connection === connection) generation.connection = null
                    }
                    close(connection)
                }
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (error: Exception) {
                if (!generation.running.get()) return
                // JDBC 오류 본문에는 접속 정보가 포함될 수 있어 오류 종류와 SQLState만 기록한다.
                log.warn("JOB_LISTENER_DISCONNECTED errorType={} sqlState={} reconnectDelayMs={}",
                    error.javaClass.simpleName, (error as? SQLException)?.sqlState, delay)
            }
            if (!generation.running.get()) return
            try { Thread.sleep(delay) } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
            val maximum = properties.maxReconnectDelay.toMillis()
            delay = if (delay > maximum / 2) maximum else delay * 2
        }
    }

    private fun subscribe(connection: Connection) {
        connection.autoCommit = true
        connection.createStatement().use {
            it.queryTimeout = databaseProperties.queryTimeout.seconds.toInt()
            it.execute("LISTEN \"${properties.channel}\"")
        }
    }

    private fun receive(generation: Generation, connection: Connection) {
        val postgres = connection.unwrap(PGConnection::class.java)
        var lastHealthCheck = System.nanoTime()
        while (generation.running.get()) {
            val notifications = postgres.getNotifications(properties.waitTimeout.toMillis().toInt())
            if (!generation.running.get()) return
            val receivedCount = notifications?.count { it.name == properties.channel } ?: 0
            if (receivedCount > 0) {
                log.info("JOB_NOTIFICATION_RECEIVED channel={} count={}", properties.channel, receivedCount)
                generation.requestPoll()
            }
            if (System.nanoTime() - lastHealthCheck >= properties.healthInterval.toNanos()) {
                // 수신 대기 timeout만으로는 무응답 네트워크를 판별하지 못한다. 같은 연결에서 왕복을 확인한다.
                connection.createStatement().use { statement ->
                    statement.queryTimeout = databaseProperties.queryTimeout.seconds.toInt()
                    statement.executeQuery("SELECT 1").use {
                        if (!it.next() || it.getInt(1) != 1) throw SQLException("LISTEN health check failed")
                    }
                }
                lastHealthCheck = System.nanoTime()
            }
        }
    }

    private fun close(connection: Connection) {
        try { connection.close() } catch (error: Exception) {
            log.warn("JOB_LISTENER_CLOSE_FAILED errorType={}", error.javaClass.simpleName)
        }
    }
}
