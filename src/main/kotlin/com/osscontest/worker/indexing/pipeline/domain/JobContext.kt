package com.osscontest.worker.indexing.pipeline.domain

/**
 * 한 번의 실행권을 나타낸다. Job을 claim한 Worker와 그때 발급된 epoch의 조합이며,
 * 이후 모든 상태 변경은 이 값이 DB의 현재 소유 정보와 일치할 때만 반영된다.
 */
data class JobContext(
    val jobId: Long,
    val documentId: Long,
    val workerId: String,
    val epoch: Long,
)
