/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import java.sql.Connection
import org.postgresql.ds.PGSimpleDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties
import org.springframework.stereotype.Component

fun interface JobNotificationConnections {
    fun open(): Connection
}

/** 작업용 풀에서 빌리지 않는 LISTEN 전용 물리 연결이다. 연결 수 예산에 인스턴스당 한 개를 더한다. */
@Component
@ConditionalOnProperty(prefix = "indexing.queue", name = ["enabled", "notifications.enabled"],
    havingValue = "true", matchIfMissing = true)
class PostgresJobNotificationConnections(
    database: DataSourceProperties,
    @Value("\${spring.datasource.hikari.connection-init-sql:}") private val initSql: String,
    @Value("\${indexing.database.connect-timeout-seconds:3}") connectTimeout: Int,
    @Value("\${indexing.database.socket-timeout-seconds:15}") socketTimeout: Int,
) : JobNotificationConnections {
    private val source = PGSimpleDataSource().apply {
        val url = database.url
        require(url?.startsWith("jdbc:postgresql:") == true) { "LISTEN requires a PostgreSQL JDBC URL" }
        setURL(url)
        user = database.username
        password = database.password
        require(connectTimeout > 0 && socketTimeout > 0)
        this.connectTimeout = connectTimeout
        this.socketTimeout = socketTimeout
        tcpKeepAlive = true
    }

    override fun open(): Connection {
        val connection = source.connection
        try {
            connection.autoCommit = true
            if (initSql.isNotBlank()) connection.createStatement().use { it.execute(initSql) }
            return connection
        } catch (error: Exception) {
            try { connection.close() } catch (closing: Exception) { error.addSuppressed(closing) }
            throw error
        }
    }
}
