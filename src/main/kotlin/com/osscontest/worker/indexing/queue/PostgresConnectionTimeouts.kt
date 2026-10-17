/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.stereotype.Component

/** 풀 획득과 헬스 조회뿐 아니라 이미 맺은 PostgreSQL 연결의 I/O에도 시간 제한을 건다. */
@Component
class PostgresConnectionTimeouts(
    @Value("\${indexing.database.connect-timeout-seconds:3}") private val connectTimeout: Int,
    @Value("\${indexing.database.socket-timeout-seconds:15}") private val socketTimeout: Int,
) : BeanPostProcessor {
    init { require(connectTimeout > 0 && socketTimeout > 0) }

    override fun postProcessBeforeInitialization(bean: Any, beanName: String): Any {
        if (bean is HikariDataSource && bean.jdbcUrl?.startsWith("jdbc:postgresql:") == true) {
            bean.addDataSourceProperty("connectTimeout", connectTimeout)
            bean.addDataSourceProperty("socketTimeout", socketTimeout)
        }
        return bean
    }
}
