package com.osscontest.worker.indexing.retrieval

import com.osscontest.worker.indexing.retrieval.config.StorageProperties
import org.springframework.stereotype.Component
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.nio.file.Files
import java.nio.file.Path

@Component
class S3DocumentDownloadClient(
    private val s3Client: S3Client,
    private val properties: StorageProperties,
) : DocumentDownloadClient {
    override fun download(objectKey: String): Path {
        val request =
            GetObjectRequest.builder()
                .bucket(properties.bucket)
                .key(objectKey)
                .build()
        // createTempFile은 고유한 경로를 얻는 데만 쓴다. ResponseTransformer.toFile은 대상 파일이 이미 있으면
        // FileAlreadyExistsException을 던지므로 빈 파일은 SDK가 쓰기 전에 지운다.
        val tempFile = Files.createTempFile("indexing-download-", ".tmp")
        Files.delete(tempFile)
        return try {
            s3Client.getObject(request, ResponseTransformer.toFile(tempFile))
            tempFile
        } catch (failure: Throwable) {
            try {
                Files.deleteIfExists(tempFile)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }
}
