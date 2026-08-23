package com.osscontest.worker.indexing.chunking.domain

data class Chunk(
    val chunkNo: Int,
    val content: String,
    val contentHash: String,
    val tokenCount: Int,
    val pageFrom: Int?,
    val pageTo: Int?,
    val sectionPath: String?,
    val metadata: Map<String, Any>?,
)
