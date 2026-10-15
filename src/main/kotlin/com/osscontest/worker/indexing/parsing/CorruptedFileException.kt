/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.parsing

class CorruptedFileException(
    mimeType: String,
    cause: Throwable,
) : RuntimeException("Failed to parse document (mimeType=$mimeType): ${cause.message}", cause) {
    val code: String = "CORRUPTED_FILE"
}
