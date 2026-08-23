package com.osscontest.worker.indexing.consumer

import java.time.Instant
import java.util.UUID

/**
 * Core 서버가 발행하는 문서 인덱싱 토픽의 이벤트 계약이다.
 *
 * Worker는 이 스키마를 읽기만 하며 변경하지 않는다. `DOCUMENT_DELETED`에는
 * [documentVersionId]가 없고, 그 밖의 인덱싱 요청에는 반드시 존재한다.
 */
data class IndexingEvent(
    val eventId: UUID,
    val eventType: String,
    val schemaVersion: Int,
    val tenantId: Long,
    val documentId: Long,
    val documentVersionId: Long?,
    val occurredAt: Instant,
    val traceId: String?,
)
