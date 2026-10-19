/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.embedding.service

/** 결제·크레딧 한도 초과(429 `insufficient_quota`). 충전 전까지 모든 호출이 실패하므로 rate limit과 구분한다. */
class EmbeddingQuotaExceededException(cause: Throwable) : RuntimeException("Embedding quota exceeded", cause)
