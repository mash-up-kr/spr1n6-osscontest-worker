package com.osscontest.worker.indexing.retrieval

import com.osscontest.worker.indexing.retrieval.config.StorageProperties
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.time.Duration

class S3DocumentDownloadClientTest {
    private val s3Client: S3Client = mock()
    private val properties =
        StorageProperties(bucket = "test-bucket", endpoint = null, region = "us-east-1", downloadTimeout = Duration.ofSeconds(30))
    private val client = S3DocumentDownloadClient(s3Client, properties)

    @Test
    fun `objectKey로 바이트를 내려받는다`() {
        val body = "hello world".toByteArray()
        val response = ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), body)
        whenever(s3Client.getObjectAsBytes(any<GetObjectRequest>())).thenReturn(response)

        val result = client.download("docs/42/v1.txt")

        assertThat(result).isEqualTo(body)
    }

    @Test
    fun `bucket과 key를 담아 요청한다`() {
        val body = "hello world".toByteArray()
        val response = ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), body)
        val captor = argumentCaptor<GetObjectRequest>()
        whenever(s3Client.getObjectAsBytes(captor.capture())).thenReturn(response)

        client.download("docs/42/v1.txt")

        assertThat(captor.firstValue.bucket()).isEqualTo("test-bucket")
        assertThat(captor.firstValue.key()).isEqualTo("docs/42/v1.txt")
    }

    @Test
    fun `S3 예외는 그대로 전파된다`() {
        whenever(s3Client.getObjectAsBytes(any<GetObjectRequest>())).thenThrow(NoSuchKeyException.builder().build())

        assertThatThrownBy { client.download("docs/missing/v1.txt") }
            .isInstanceOf(NoSuchKeyException::class.java)
    }
}
