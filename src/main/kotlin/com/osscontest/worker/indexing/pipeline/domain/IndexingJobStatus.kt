/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.pipeline.domain

enum class IndexingJobStatus {
    PENDING,
    PROCESSING,
    RETRY_WAIT,
    COMPLETED,
    FAILED,
}
