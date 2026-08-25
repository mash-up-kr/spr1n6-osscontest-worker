package com.osscontest.worker.indexing.fault

import com.osscontest.worker.indexing.consumer.KafkaRecordIdentity
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.CountDownLatch

data class FaultInjectionContext(
    val sourceEventId: UUID,
    val workerId: String,
    val recordIdentity: KafkaRecordIdentity,
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
                "sourceEventId={} partition={} offset={}",
            documentVersionId,
            phase,
            context.workerId,
            context.sourceEventId,
            context.recordIdentity.partition,
            context.recordIdentity.offset,
        )
        blocker.await()
    }
}
