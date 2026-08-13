package com.osscontest.worker.indexing.embedding.service

class EmbeddingRequestRejectedException(
    cause: Throwable,
) : RuntimeException(
        "Embedding request rejected by provider: ${cause.message ?: "HTTP 400 Bad Request"}",
        cause,
    ) {
    val code: String = "EMBEDDING_REQUEST_REJECTED"
}
