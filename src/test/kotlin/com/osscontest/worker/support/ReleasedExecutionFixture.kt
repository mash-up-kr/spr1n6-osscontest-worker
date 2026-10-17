/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.support

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.ExecutionToken
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant

/** 외부에서 해제된 실행 상태를 만든다. 회수 정책은 구현하지 않는다. */
fun seedReleasedExecution(jdbc: JdbcTemplate, token: ExecutionToken, status: IndexingJobStatus, retryAt: Instant? = null) {
    require(status == IndexingJobStatus.RETRY_WAIT || status == IndexingJobStatus.FAILED)
    require((status == IndexingJobStatus.RETRY_WAIT) == (retryAt != null))
    check(jdbc.update("""
        UPDATE indexing_job SET status = ?, worker_id = NULL, lease_until = NULL, outcome = NULL,
            next_retry_at = CAST(? AS timestamptz), last_error_code = 'LEASE_EXPIRED_FIXTURE',
            completed_at = CASE WHEN ? = 'FAILED' THEN clock_timestamp() ELSE NULL END,
            updated_at = clock_timestamp()
        WHERE id = ? AND execution_epoch = ?
    """.trimIndent(), status.name, retryAt?.toString(), status.name, token.jobId, token.epoch) == 1)
}
