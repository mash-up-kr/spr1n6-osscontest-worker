package com.osscontest.worker.indexing.embedding.service

/** Shared provider credentials/permissions; distinct from per-request input errors and rate limits. */
class EmbeddingConfigurationException(val code: String, cause: Throwable) : RuntimeException(code, cause)
