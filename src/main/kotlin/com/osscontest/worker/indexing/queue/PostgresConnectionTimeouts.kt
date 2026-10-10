package com.osscontest.worker.indexing.queue

import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.stereotype.Component

/** Bound established-connection I/O as well as pool acquisition and health queries. */
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
