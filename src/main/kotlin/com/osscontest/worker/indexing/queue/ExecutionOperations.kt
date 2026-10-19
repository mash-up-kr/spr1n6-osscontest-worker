/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import org.springframework.stereotype.Component

/** 실행 중인 Job의 DB 작업을 실행권 확인과 함께 수행한다. lease만 만료됐으면 갱신한 뒤 다시 실행한다. */
@Component
class ExecutionOperations(
    private val database: DatabaseOperations,
    private val store: JobQueueStore,
) {
    /**
     * 실행권을 확인하고 만료된 lease는 갱신한다. 실행권을 잃었으면 [ExecutionLostException]을 던진다.
     * 이미 완료된 실행은 [allowCompleted]면 그대로 돌아가고, 아니면 [ExecutionCompletedException]을 던진다.
     */
    fun checkpoint(token: ExecutionToken, allowCompleted: Boolean = false) {
        database.execute {
            var job = store.snapshot(token) ?: throw ExecutionLostException(token)
            if (job.completed(token)) {
                if (allowCompleted) return@execute
                throw ExecutionCompletedException(token)
            }
            if (!job.ownsProcessing(token)) throw ExecutionLostException(token)
            if (!job.canWrite(token)) {
                if (store.renew(token) != 1) throw ExecutionLostException(token)
                job = store.snapshot(token) ?: throw ExecutionLostException(token)
                if (!job.ownsProcessing(token)) throw ExecutionLostException(token)
                if (!job.canWrite(token)) throw LeaseExpiredException()
            }
        }
    }

    /** 재시도할 때마다 완료 여부를 먼저 확인한다. 이미 완료됐으면 [completed]를, 아니면 [action]을 실행한다. */
    fun <T> completion(token: ExecutionToken, identity: ExecutionIdentity, completed: () -> T, action: () -> T): T {
        while (true) {
            try {
                return database.execute {
                    val snapshot = store.snapshot(token) ?: throw ExecutionLostException(token)
                    if (!identity.matches(snapshot)) throw ExecutionLostException(token)
                    if (snapshot.completed(token)) completed()
                    else {
                        checkpoint(token)
                        action()
                    }
                }
            } catch (_: LeaseExpiredException) {
                checkpoint(token, allowCompleted = true)
            }
        }
    }

    fun <T> database(token: ExecutionToken, action: () -> T): T {
        while (true) {
            try {
                return database.execute(afterRecovery = { checkpoint(token) }, action = action)
            } catch (_: LeaseExpiredException) {
                // 실패한 트랜잭션은 이미 풀렸다. lease를 갱신한 뒤 다시 실행한다.
                checkpoint(token)
            }
        }
    }

    /**
     * 실패 기록을 실행한다. 이미 반영된 실패는 소유자가 비어 있어 복구 checkpoint가 거절하므로
     * checkpoint를 거치지 않고 [action]이 자기 실패인지 직접 확인한다.
     */
    fun <T> failure(token: ExecutionToken, action: () -> T): T {
        while (true) {
            try {
                return database.execute(action = action)
            } catch (_: LeaseExpiredException) {
                checkpoint(token, allowCompleted = true)
            }
        }
    }
}

data class ExecutionIdentity(val type: JobType, val documentId: Long, val documentVersionId: Long?) {
    constructor(job: ClaimedJob) : this(job.type, job.documentId, job.documentVersionId)
    fun matches(snapshot: JobSnapshot) = snapshot.type == type && snapshot.documentId == documentId &&
        snapshot.documentVersionId == documentVersionId
}
