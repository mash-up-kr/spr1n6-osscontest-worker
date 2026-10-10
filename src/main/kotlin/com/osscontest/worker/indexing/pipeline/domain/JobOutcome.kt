package com.osscontest.worker.indexing.pipeline.domain

/** `indexing_job.outcome`에 기록하는 완료 사유. 값이 늘 수 있어 DB에는 CHECK 제약을 두지 않는다. */
object JobOutcome {
    const val INDEXED = "INDEXED"
    const val DELETED = "DELETED"
    const val SKIPPED_DOCUMENT_DELETED = "SKIPPED_DOCUMENT_DELETED"
}
