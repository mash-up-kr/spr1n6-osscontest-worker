package com.osscontest.worker.indexing.pipeline.domain

import com.osscontest.worker.indexing.queue.ExecutionToken

data class IndexingContext(
    val jobId: Long,
    val documentId: Long,
    val documentVersionId: Long,
    val versionNo: Long,
    val extractedMetadata: Map<String, Any>?,
    val executionToken: ExecutionToken,
    val attemptCount: Int = 1,
)
