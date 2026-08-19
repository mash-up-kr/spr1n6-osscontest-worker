package com.osscontest.worker.indexing.embedding.usecase

interface EmbeddingUseCase {
    fun embed(contents: List<String>): List<FloatArray>
}
