/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.embedding.service

class EmbeddingRateLimitedException(val retryAfter: String?, cause: Throwable) : RuntimeException("Embedding rate limited", cause)
