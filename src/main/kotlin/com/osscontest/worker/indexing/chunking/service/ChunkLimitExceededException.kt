/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.chunking.service

class ChunkLimitExceededException(
    val actualChunkCount: Int,
    val maxChunks: Int,
) : RuntimeException("chunk count $actualChunkCount exceeds max $maxChunks") {
    val code: String = "CHUNK_LIMIT_EXCEEDED"
}
