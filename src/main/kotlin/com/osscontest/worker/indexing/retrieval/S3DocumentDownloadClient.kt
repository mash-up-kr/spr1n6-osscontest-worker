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
        // Files.createTempFile()는 유일한 경로를 "예약"하는 용도로만 쓴다 — 이 SDK 버전
        // (sdk-core 2.29.1)의 ResponseTransformer.toFile(Path)는 내부적으로
        // Files.copy(InputStream, Path)를 REPLACE_EXISTING 옵션 없이 호출하기 때문에,
        // 대상 파일이 이미 존재하면(createTempFile이 만들어 놓은 빈 파일) FileAlreadyExistsException을
        // 던진다. 그래서 경로만 얻고 파일 자체는 SDK가 쓰기 전에 지운다.
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
