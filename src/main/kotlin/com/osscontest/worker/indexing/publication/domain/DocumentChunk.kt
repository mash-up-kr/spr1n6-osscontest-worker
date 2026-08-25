package com.osscontest.worker.indexing.publication.domain

import java.time.LocalDateTime

data class DocumentChunk(
    val documentVersionId: Long,
    val documentId: Long,
    val chunkNo: Int,
    val content: String,
    val contentTokens: String,
    val contentHash: String,
    val tokenCount: Int?,
    val pageFrom: Int?,
    val pageTo: Int?,
    val sectionPath: String?,
    val metadata: Map<String, Any>?,
    val embedding: FloatArray,
    val embeddedAt: LocalDateTime,
)
