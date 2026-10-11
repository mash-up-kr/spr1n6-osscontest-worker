package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.queue.JobOutcome
import java.time.Instant

interface IndexingJobWrites {
    fun complete(jobId: Long, workerId: String, epoch: Long): Int
    fun complete(jobId: Long, workerId: String, epoch: Long, outcome: JobOutcome): Int
    fun updatePhase(jobId: Long, phase: String, workerId: String, epoch: Long): Int
    fun fail(jobId: Long, workerId: String, epoch: Long, status: String, retryAt: Instant?,
             errorCode: String, errorMessage: String): Int
}
