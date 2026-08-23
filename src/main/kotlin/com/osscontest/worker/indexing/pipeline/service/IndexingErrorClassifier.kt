package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.service.ChunkLimitExceededException
import com.osscontest.worker.indexing.chunking.service.EmptyExtractionException
import com.osscontest.worker.indexing.chunking.service.TotalTokenLimitExceededException
import com.osscontest.worker.indexing.consumer.InvalidEventException
import com.osscontest.worker.indexing.embedding.service.EmbeddingRequestRejectedException
import com.osscontest.worker.indexing.embedding.service.InvalidEmbeddingException
import com.osscontest.worker.indexing.parsing.CorruptedFileException
import com.osscontest.worker.indexing.parsing.UnsupportedMimeTypeException
import com.osscontest.worker.indexing.retrieval.ContentIntegrityException
import com.osscontest.worker.indexing.retrieval.FileTooLargeException

/** 같은 입력에서 반복될 영구 실패와 일시 실패를 분류한다. 알 수 없는 실패는 재시도한다. */
internal object IndexingErrorClassifier {
    fun classify(error: Exception): IndexingError =
        when (error) {
            is InvalidEventException -> permanent(error.code)
            is ContentIntegrityException -> permanent(error.code)
            is EmptyExtractionException -> permanent(error.code)
            is ChunkLimitExceededException -> permanent(error.code)
            is TotalTokenLimitExceededException -> permanent(error.code)
            is UnsupportedMimeTypeException -> permanent(error.code)
            is FileTooLargeException -> permanent(error.code)
            is CorruptedFileException -> permanent(error.code)
            is EmbeddingRequestRejectedException -> permanent(error.code)
            is InvalidEmbeddingException -> permanent(error.code)
            else -> IndexingError(error::class.simpleName ?: "INDEXING_ERROR", retryable = true)
        }

    private fun permanent(code: String) = IndexingError(code, retryable = false)
}

internal data class IndexingError(
    val code: String,
    val retryable: Boolean,
)
