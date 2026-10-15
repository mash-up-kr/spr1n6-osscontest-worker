/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.retrieval

class ContentIntegrityException(
    val expectedHash: String,
    val actualHash: String,
) : RuntimeException("Content hash mismatch: expected=$expectedHash actual=$actualHash") {
    // indexing_job.last_error_code에 기록되는 코드다. 클래스 simpleName 폴백에 기대지 않도록 직접 둔다.
    val code: String = "HASH_MISMATCH"
}
