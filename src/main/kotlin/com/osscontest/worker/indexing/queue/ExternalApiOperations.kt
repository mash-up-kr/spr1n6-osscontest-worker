package com.osscontest.worker.indexing.queue

import org.springframework.stereotype.Component

@Component
class ExternalApiOperations(
    private val execution: ExecutionOperations,
    private val database: DatabaseOperations,
) {
    fun <T> execute(token: ExecutionToken, action: () -> T): T {
        database.awaitAvailable()
        execution.checkpoint(token)
        return action()
    }
}
