package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.service.ChunkLimitExceededException
import com.osscontest.worker.indexing.chunking.service.EmptyExtractionException
import com.osscontest.worker.indexing.chunking.service.TotalTokenLimitExceededException
import com.osscontest.worker.indexing.queue.InvalidJobException
import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import com.osscontest.worker.indexing.embedding.service.EmbeddingRequestRejectedException
import com.osscontest.worker.indexing.embedding.service.InvalidEmbeddingException
import com.osscontest.worker.indexing.parsing.CorruptedFileException
import com.osscontest.worker.indexing.parsing.UnsupportedMimeTypeException
import com.osscontest.worker.indexing.retrieval.ContentIntegrityException
import com.osscontest.worker.indexing.retrieval.FileTooLargeException

import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception

/** 같은 입력에서 반복될 영구 실패와 일시 실패를 분류한다. 알 수 없는 실패는 재시도한다. */
internal object IndexingErrorClassifier {
    fun classify(error: Exception, jobAfterUploadConfirmed: Boolean = false): IndexingError =
        when (error) {
            is S3Exception -> if (jobAfterUploadConfirmed &&
                (error is NoSuchKeyException ||
                    error.awsErrorDetails()?.errorCode() == "NoSuchKey")) permanent("NoSuchKey")
                else IndexingError(error::class.simpleName ?: "STORAGE_ERROR", retryable = true)
            is EmbeddingRateLimitedException -> IndexingError("RATE_LIMITED", retryable = true)
            is InvalidJobException -> permanent(error.code)
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
