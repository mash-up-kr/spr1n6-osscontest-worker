/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.embedding.service

class InvalidEmbeddingException(
    message: String,
) : RuntimeException(message) {
    val code: String = "INVALID_EMBEDDING"
}
