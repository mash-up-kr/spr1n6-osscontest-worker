package com.osscontest.worker.indexing.pipeline.domain

/** claim으로 획득한 Job. [context]로 이후의 상태 변경을 보호한다. */
data class ClaimedJob(
    val context: JobContext,
    val documentVersionId: Long,
    val jobType: IndexingJobType,
    val attemptCount: Int,
)
