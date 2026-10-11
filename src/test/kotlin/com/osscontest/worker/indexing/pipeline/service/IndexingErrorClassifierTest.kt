package com.osscontest.worker.indexing.pipeline.service

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
}
