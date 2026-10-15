/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.postgresql.PGConnection
import org.postgresql.PGNotification

class JobNotificationListenerTest {
    private class Session {
        val connection: Connection = mock()
        val postgres: PGConnection = mock()
        val statement: Statement = mock()
        val events = LinkedBlockingQueue<() -> Array<PGNotification>?>()
        val closed = CountDownLatch(1)

        init {
            whenever(connection.createStatement()).thenReturn(statement)
            whenever(connection.unwrap(PGConnection::class.java)).thenReturn(postgres)
            whenever(postgres.getNotifications(any())).thenAnswer {
                events.poll(100, TimeUnit.MILLISECONDS)?.invoke()
            }
            doAnswer {
                closed.countDown()
                events.offer { throw SQLException("closed", "08003") }
                null
            }.whenever(connection).close()
        }

        fun notify(channel: String = "indexing_job") {
            val notification: PGNotification = mock()
            whenever(notification.name).thenReturn(channel)
            events.add { arrayOf(notification) }
        }
    }

    private class Harness(
        properties: JobNotificationProperties = JobNotificationProperties(
            reconnectDelay = Duration.ofMillis(5), maxReconnectDelay = Duration.ofMillis(20)),
        open: (() -> Connection)? = null,
    ) : AutoCloseable {
        val sessions = LinkedBlockingQueue<Session>()
        val polls = LinkedBlockingQueue<Unit>()
        val opens = AtomicInteger()
        val worker: DbJobWorker = mock()
        val listener: JobNotificationListener

        init {
            whenever(worker.pollRequester()).thenReturn({ polls.add(Unit); Unit })
            listener = JobNotificationListener(properties, JobNotificationConnections {
                opens.incrementAndGet()
                open?.invoke() ?: checkNotNull(sessions.poll(3, TimeUnit.SECONDS)).connection
            }, worker, DatabaseWaitProperties())
        }

        fun awaitPoll() { assertThat(polls.poll(3, TimeUnit.SECONDS)).isNotNull() }
        override fun close() { listener.stop() }
    }

    @Test
    fun `구독 완료 뒤 탐색하고 여러 알림을 같은 장기 연결에서 수신한다`() {
        Harness().use { h ->
            val session = Session()
            h.sessions.add(session)
            h.listener.start(); h.listener.start()
            h.awaitPoll()
            val order = inOrder(session.connection, session.statement, h.worker)
            order.verify(session.connection).setAutoCommit(true)
            order.verify(session.statement).execute("LISTEN \"indexing_job\"")
            session.notify("another_channel")
            session.notify(); h.awaitPoll()
            session.notify(); h.awaitPoll()
            assertThat(h.opens.get()).isEqualTo(1)
            assertThat(h.polls).isEmpty()
            h.listener.stop()
            assertThat(session.closed.await(3, TimeUnit.SECONDS)).isTrue()
            assertThat(h.listener.isRunning).isFalse()
        }
    }

    @Test
    fun `수신 연결이 끊어지면 새 연결에서 재구독하고 누락된 작업을 즉시 탐색한다`() {
        Harness().use { h ->
            val first = Session()
            val second = Session()
            h.sessions.add(first); h.sessions.add(second)
            h.listener.start(); h.awaitPoll()
            first.events.add { throw SQLException("connection lost", "08006") }
            h.awaitPoll()
            assertThat(first.closed.await(3, TimeUnit.SECONDS)).isTrue()
            verify(second.statement).execute("LISTEN \"indexing_job\"")
            second.notify(); h.awaitPoll()
            assertThat(h.opens.get()).isEqualTo(2)
        }
    }

    @Test
    fun `첫 연결 실패 뒤에도 리스너를 재시도하고 구독한다`() {
        val attempts = AtomicInteger()
        val session = Session()
        Harness(open = {
            if (attempts.incrementAndGet() < 3) throw SQLException("offline", "08006")
            session.connection
        }).use { h ->
            h.listener.start(); h.awaitPoll()
            assertThat(attempts.get()).isEqualTo(3)
            session.notify(); h.awaitPoll()
        }
    }

    @Test
    fun `유휴 연결도 같은 연결에서 상태를 확인하고 무응답이면 재구독한다`() {
        Harness(properties = JobNotificationProperties(healthInterval = Duration.ofMillis(1),
            reconnectDelay = Duration.ofMillis(5))).use { h ->
            val first = Session()
            val second = Session()
            val healthy = CountDownLatch(1)
            val result: ResultSet = mock()
            whenever(result.next()).thenReturn(true)
            whenever(result.getInt(1)).thenReturn(1)
            whenever(first.statement.executeQuery("SELECT 1")).thenAnswer {
                healthy.countDown(); result
            }
            h.sessions.add(first); h.sessions.add(second)
            h.listener.start(); h.awaitPoll()
            assertThat(healthy.await(3, TimeUnit.SECONDS)).isTrue()
            assertThat(h.opens.get()).isEqualTo(1)
            whenever(first.statement.executeQuery("SELECT 1")).thenThrow(SQLException("health failed", "08006"))
            h.awaitPoll()
            verify(second.statement).execute("LISTEN \"indexing_job\"")
        }
    }

    @Test
    fun `stop 뒤 늦게 열린 연결은 구독하지 않고 닫는다`() {
        val opening = CountDownLatch(1)
        val release = CountDownLatch(1)
        val session = Session()
        Harness(open = {
            opening.countDown()
            while (true) {
                try { release.await(); break } catch (_: InterruptedException) {
                    // 인터럽트 뒤에도 connect가 완료되는 JDBC 경계를 재현한다.
                }
            }
            session.connection
        }).use { h ->
            h.listener.start()
            assertThat(opening.await(3, TimeUnit.SECONDS)).isTrue()
            try { h.listener.stop() } finally { release.countDown() }
            assertThat(session.closed.await(3, TimeUnit.SECONDS)).isTrue()
            verifyNoInteractions(session.statement)
            assertThat(h.polls).isEmpty()
        }
    }

    @Test
    fun `종료 뒤 재시작은 새 워커 세대의 탐색 콜백을 사용한다`() {
        Harness().use { h ->
            val first = Session()
            val second = Session()
            h.sessions.add(first); h.sessions.add(second)
            h.listener.start(); h.awaitPoll(); h.listener.stop()
            val restarted = LinkedBlockingQueue<Unit>()
            whenever(h.worker.pollRequester()).thenReturn({ restarted.add(Unit); Unit })
            h.listener.start()
            assertThat(restarted.poll(3, TimeUnit.SECONDS)).isNotNull()
            second.notify()
            assertThat(restarted.poll(3, TimeUnit.SECONDS)).isNotNull()
            assertThat(h.polls).isEmpty()
        }
    }
}
