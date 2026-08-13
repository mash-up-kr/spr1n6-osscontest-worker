package com.osscontest.worker.indexing.embedding.service

class InvalidEmbeddingException(
    message: String,
) : RuntimeException(message) {
    val code: String = "INVALID_EMBEDDING"
}
