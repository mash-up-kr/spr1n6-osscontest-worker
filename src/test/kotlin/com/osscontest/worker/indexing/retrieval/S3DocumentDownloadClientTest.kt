package com.osscontest.worker.indexing.retrieval

import com.osscontest.worker.indexing.retrieval.config.StorageProperties
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

class S3DocumentDownloadClientTest {
    private val s3Client: S3Client = mock()
    private val properties =
        StorageProperties(bucket = "test-bucket", endpoint = null, region = "us-east-1", downloadTimeout = Duration.ofSeconds(30))
    private val client = S3DocumentDownloadClient(s3Client, properties)

    // 실제 S3 SDK가 ResponseTransformer.toFile()이 반환하는 내부 트랜스포머로 응답 스트림을
    // 파일에 스트리밍하는 동작을 흉내낸다 — 목이 트랜스포머를 실제로 실행해서 파일에 바이트를
    // 써야 client.download()가 반환한 Path가 실제 내용을 담게 된다.
    // ResponseTransformer.toFile(Path)의 ReturnT는 Path가 아니라 ResponseT(GetObjectResponse) 그
    // 자체다 — 파일 쓰기는 부수효과이고, transform()은 응답 객체를 그대로 반환한다.
    private fun stubS3Response(body: ByteArray): (org.mockito.invocation.InvocationOnMock) -> GetObjectResponse {
        return { invocation ->
            val transformer = invocation.getArgument<ResponseTransformer<GetObjectResponse, GetObjectResponse>>(1)
            transformer.transform(GetObjectResponse.builder().build(), AbortableInputStream.create(ByteArrayInputStream(body)))
        }
    }

    @Test
    fun `objectKey로 파일을 내려받는다`() {
        val body = "hello world".toByteArray()
        whenever(s3Client.getObject(any<GetObjectRequest>(), any<ResponseTransformer<GetObjectResponse, GetObjectResponse>>()))
            .thenAnswer(stubS3Response(body))

        val result = client.download("docs/42/v1.txt")

        assertThat(Files.readAllBytes(result)).isEqualTo(body)
        Files.deleteIfExists(result)
    }

    @Test
    fun `bucket과 key를 담아 요청한다`() {
        val body = "hello world".toByteArray()
        val captor = argumentCaptor<GetObjectRequest>()
        whenever(s3Client.getObject(captor.capture(), any<ResponseTransformer<GetObjectResponse, GetObjectResponse>>()))
            .thenAnswer(stubS3Response(body))

        val result = client.download("docs/42/v1.txt")

        assertThat(captor.firstValue.bucket()).isEqualTo("test-bucket")
        assertThat(captor.firstValue.key()).isEqualTo("docs/42/v1.txt")
        Files.deleteIfExists(result)
    }

    @Test
    fun `S3 예외는 그대로 전파된다`() {
        whenever(s3Client.getObject(any<GetObjectRequest>(), any<ResponseTransformer<GetObjectResponse, GetObjectResponse>>()))
            .thenThrow(NoSuchKeyException.builder().build())

        assertThatThrownBy { client.download("docs/missing/v1.txt") }
            .isInstanceOf(NoSuchKeyException::class.java)
    }

    @Test
    fun `다운로드 중 실패하면 부분 임시 파일을 삭제한다`() {
        val filesBefore = downloadTempFiles()
        val failingStream =
            object : InputStream() {
                private var reads = 0

                override fun read(): Int {
                    if (reads++ < 3) return 'x'.code
                    throw IOException("stream failed")
                }
            }
        whenever(s3Client.getObject(any<GetObjectRequest>(), any<ResponseTransformer<GetObjectResponse, GetObjectResponse>>()))
            .thenAnswer { invocation ->
                val transformer = invocation.getArgument<ResponseTransformer<GetObjectResponse, GetObjectResponse>>(1)
                transformer.transform(
                    GetObjectResponse.builder().build(),
                    AbortableInputStream.create(failingStream),
                )
            }

        assertThatThrownBy { client.download("docs/partial/v1.txt") }
            .isInstanceOf(Exception::class.java)
        assertThat(downloadTempFiles()).isEqualTo(filesBefore)
    }

    private fun downloadTempFiles(): Set<Path> =
        Files.list(Path.of(System.getProperty("java.io.tmpdir"))).use { paths ->
            paths
                .filter { it.fileName.toString().startsWith("indexing-download-") }
                .toList()
                .toSet()
        }
}
