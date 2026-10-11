package com.osscontest.worker.indexing.publication.repository

import java.time.Instant

interface IndexingJobWrites {
    fun complete(jobId: Long, workerId: String, epoch: Long): Int
    fun updatePhase(jobId: Long, phase: String, workerId: String, epoch: Long): Int
    fun fail(jobId: Long, workerId: String, epoch: Long, status: String, retryAt: Instant?,
             errorCode: String, errorMessage: String): Int
}
