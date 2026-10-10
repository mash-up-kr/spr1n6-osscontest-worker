package com.osscontest.worker.indexing.embedding.service

class EmbeddingRateLimitedException(val retryAfter: String?, cause: Throwable) : RuntimeException("Embedding rate limited", cause)
