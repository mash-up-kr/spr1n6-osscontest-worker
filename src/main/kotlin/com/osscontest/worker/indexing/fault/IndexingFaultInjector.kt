/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.fault

import com.osscontest.worker.indexing.queue.ExecutionToken
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.CountDownLatch

data class FaultInjectionContext(
    val sourceEventId: UUID,
    val executionToken: ExecutionToken,
)

@Component
class IndexingFaultInjector(
    private val properties: FaultInjectionProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val blocker = CountDownLatch(1)

    fun blockIfNeeded(
        documentVersionId: Long,
        phase: String,
        context: FaultInjectionContext,
    ) {
        if (!properties.enabled) return
        if (properties.phase != phase) return

        log.warn(
            "FAULT_INJECTION_BLOCKED documentVersionId={} phase={} workerId={} " +
                "sourceEventId={} jobId={} executionEpoch={}",
            documentVersionId,
            phase,
            context.executionToken.workerId,
            context.sourceEventId,
            context.executionToken.jobId,
            context.executionToken.epoch,
        )
        blocker.await()
    }
}
