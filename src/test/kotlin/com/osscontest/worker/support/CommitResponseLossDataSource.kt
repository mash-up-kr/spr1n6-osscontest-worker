package com.osscontest.worker.support

import org.springframework.jdbc.datasource.DelegatingDataSource
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/** Commits on PostgreSQL first, then loses only the acknowledgement. No simulated rollback. */
class CommitResponseLossDataSource(target: DataSource) : DelegatingDataSource(target) {
    val loseNextCommit = AtomicBoolean()
    override fun getConnection(): Connection = wrap(super.getConnection())
    override fun getConnection(username: String, password: String): Connection = wrap(super.getConnection(username, password))
    private fun wrap(connection: Connection) = Proxy.newProxyInstance(Connection::class.java.classLoader,
        arrayOf(Connection::class.java)) { _, method, args ->
        try {
            val result = method.invoke(connection, *(args ?: emptyArray()))
            if (method.name == "commit" && loseNextCommit.compareAndSet(true, false)) {
                throw SQLException("Commit acknowledgement lost after successful commit", "08006")
            }
            result
        } catch (error: InvocationTargetException) { throw error.targetException }
    } as Connection
}
