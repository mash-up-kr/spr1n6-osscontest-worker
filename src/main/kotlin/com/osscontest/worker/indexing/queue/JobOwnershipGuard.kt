/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.stereotype.Component

/** 모든 호출자는 쓰기 트랜잭션이 끝날 때까지 문서·Job 잠금을 유지해야 한다. */
@Component
class JobOwnershipGuard(private val locks: JobExecutionLocks) {
    fun lockForResult(token: ExecutionToken): LockedJobExecution =
        locks.lock(token.jobId) ?: throw ExecutionLostException(token)

    fun lock(token: ExecutionToken): IndexingJobEntity {
        val job = lockForResult(token).job
        if (job.workerId != token.workerId || job.executionEpoch != token.epoch) throw ExecutionLostException(token)
        return job
    }

    fun requireProcessing(job: IndexingJobEntity, token: ExecutionToken) {
        check(job.id == token.jobId)
        requireProcessing(lockForResult(token), token)
    }

    /** 잠근 조회 결과는 같은 트랜잭션 안에서만 재사용한다. 최종 쓰기는 DB 시각으로 다시 확인한다. */
    fun requireProcessing(execution: LockedJobExecution, token: ExecutionToken) {
        locks.requireTransaction()
        if (!owns(execution, token)) throw ExecutionLostException(token)
        if (!canWrite(execution, token)) throw LeaseExpiredException()
    }

    fun owns(execution: LockedJobExecution, token: ExecutionToken): Boolean =
        execution.job.id == token.jobId && execution.snapshot().ownsProcessing(token)

    fun canWrite(execution: LockedJobExecution, token: ExecutionToken): Boolean =
        execution.job.id == token.jobId && execution.snapshot().canWrite(token)
}

/**
 * 별칭 j와 ExecutionToken 파라미터(:id, :workerId, :epoch)로 쓴다.
 * [OWNED_PROCESSING]은 외부 회수나 새 claim으로 소유권이 바뀌지 않았으면 만료된 lease의 갱신도 허용한다.
 * 결과 쓰기는 lease 만료까지 확인하는 [WRITABLE_PROCESSING]을 쓴다.
 */
internal object JobOwnershipSql {
    const val OWNED_PROCESSING = """
        j.id = :id AND j.worker_id = :workerId AND j.execution_epoch = :epoch
        AND j.status = 'PROCESSING' AND j.lease_until IS NOT NULL
        AND EXISTS (SELECT 1 FROM document d WHERE d.id = j.document_id AND d.execution_epoch = :epoch)
    """
    const val WRITABLE_PROCESSING = "$OWNED_PROCESSING AND j.lease_until > clock_timestamp()"
}
