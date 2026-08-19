package com.osscontest.worker.indexing.consumer

import java.time.Instant
import java.util.UUID

data class IndexingRequestedEvent(
    val eventId: UUID,
    val eventType: String,            // "INDEXING_REQUESTED" | "DOCUMENT_DELETED"
    val eventSchemaVersion: Int,
    val tenantId: Long,
    val documentId: Long,
    val documentVersionId: Long?,     // DOCUMENT_DELETED에는 없음 — null
    val occurredAt: Instant,
    val traceId: String?,
)
