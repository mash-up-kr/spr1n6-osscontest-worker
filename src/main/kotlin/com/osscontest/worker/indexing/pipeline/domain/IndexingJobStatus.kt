package com.osscontest.worker.indexing.pipeline.domain

enum class IndexingJobStatus {
    PENDING,
    PROCESSING,
    RETRY_WAIT,
    COMPLETED,
    FAILED,
}
