package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.queue.JobOutcome
import java.time.Instant

interface IndexingJobWrites {
    /** Job 종류로 outcome을 정해 완료한다(INDEX는 INDEXED, DELETE는 DELETED). 운영 코드는 outcome을 넘기는 쪽을 쓰고, 통합 테스트가 이 기본값을 검증한다. */
    fun complete(jobId: Long, workerId: String, epoch: Long): Int
    fun complete(jobId: Long, workerId: String, epoch: Long, outcome: JobOutcome): Int
    fun updatePhase(jobId: Long, phase: String, workerId: String, epoch: Long): Int
    fun fail(jobId: Long, workerId: String, epoch: Long, status: String, retryAt: Instant?,
             errorCode: String, errorMessage: String): Int
}
