/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.embedding.service.EmbeddingConfigurationException
import com.osscontest.worker.indexing.embedding.service.EmbeddingQuotaExceededException
import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception

class IndexingErrorClassifierTest {
    @Test
    fun `업로드 계약 확인 후에만 NoSuchKey를 영구 오류로 분류한다`() {
        val missing = NoSuchKeyException.builder().build()
        assertThat(IndexingErrorClassifier.classify(missing).retryable).isTrue()
        assertThat(IndexingErrorClassifier.classify(missing, true)).isEqualTo(IndexingError("NoSuchKey", false))
        val coded = S3Exception.builder().statusCode(404)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build()).build()
        assertThat(IndexingErrorClassifier.classify(coded, true).retryable).isFalse()
    }

    @Test
    fun `업로드 계약 확인 후에도 일반 404와 스토리지 서버 오류는 재시도한다`() {
        for (status in listOf(404, 500, 503)) {
            assertThat(IndexingErrorClassifier.classify(S3Exception.builder().statusCode(status).build(), true).retryable)
                .isTrue()
        }
    }

    @Test
    fun `파싱 청크 제한 임베딩 거절 등 누락된 영구 오류는 정해진 코드로 분류한다`() {
        val cause = IllegalArgumentException("invalid input")
        val cases = listOf(
            com.osscontest.worker.indexing.parsing.UnsupportedMimeTypeException("unknown") to "UNSUPPORTED_MIME_TYPE",
            com.osscontest.worker.indexing.parsing.CorruptedFileException("application/pdf", cause) to "CORRUPTED_FILE",
            com.osscontest.worker.indexing.chunking.service.EmptyExtractionException() to "EMPTY_EXTRACTION",
            com.osscontest.worker.indexing.chunking.service.ChunkLimitExceededException(2, 1) to "CHUNK_LIMIT_EXCEEDED",
            com.osscontest.worker.indexing.chunking.service.TotalTokenLimitExceededException(2, 1) to "TOTAL_TOKEN_LIMIT_EXCEEDED",
            com.osscontest.worker.indexing.retrieval.FileTooLargeException(2, 1) to "FILE_TOO_LARGE",
            com.osscontest.worker.indexing.embedding.service.EmbeddingRequestRejectedException(cause) to "EMBEDDING_REQUEST_REJECTED",
            com.osscontest.worker.indexing.embedding.service.InvalidEmbeddingException("invalid dimensions") to "INVALID_EMBEDDING",
        )
        cases.forEach { (error, code) ->
            assertThat(IndexingErrorClassifier.classify(error)).describedAs(error::class.simpleName)
                .isEqualTo(IndexingError(code, retryable = false))
        }
    }

    @Test
    fun `파싱 timeout 포화와 알 수 없는 IO 오류는 재시도 가능한 오류다`() {
        val cases = listOf(
            com.osscontest.worker.indexing.parsing.ParseTimeoutException("text/plain", java.time.Duration.ofSeconds(1)) to "PARSE_TIMEOUT",
            com.osscontest.worker.indexing.parsing.ParseCapacityException(IllegalStateException("full")) to "PARSE_CAPACITY",
            java.io.IOException("temporary failure") to "IOException",
        )
        cases.forEach { (error, code) ->
            assertThat(IndexingErrorClassifier.classify(error)).isEqualTo(IndexingError(code, retryable = true))
        }
    }

    @Test
    fun `임베딩 rate limit quota 인증 권한 오류는 정해진 코드의 재시도 가능 오류다`() {
        val cause = RuntimeException("provider error")
        val cases = listOf(
            EmbeddingRateLimitedException("10", cause) to "RATE_LIMITED",
            EmbeddingQuotaExceededException(cause) to "EMBEDDING_QUOTA_EXCEEDED",
            EmbeddingConfigurationException("EMBEDDING_AUTHENTICATION_FAILED", cause) to "EMBEDDING_AUTHENTICATION_FAILED",
            EmbeddingConfigurationException("EMBEDDING_PERMISSION_DENIED", cause) to "EMBEDDING_PERMISSION_DENIED",
        )
        cases.forEach { (error, code) ->
            assertThat(IndexingErrorClassifier.classify(error)).describedAs(code).isEqualTo(IndexingError(code, retryable = true))
        }
    }
}
