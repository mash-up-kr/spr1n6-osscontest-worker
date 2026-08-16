package com.osscontest.worker.indexing.pipeline.domain

data class IndexingContext(
    val jobId: Long,
    val documentId: Long,
    val documentVersionId: Long,
    val versionNo: Long,
    val extractedMetadata: Map<String, Any>?,
)
