package com.osscontest.worker.indexing.retrieval

import com.osscontest.worker.indexing.retrieval.config.StorageProperties
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

@Component
class S3DocumentDownloadClient(
    private val s3Client: S3Client,
    private val properties: StorageProperties,
) : DocumentDownloadClient {
    override fun download(objectKey: String): ByteArray {
        val request =
            GetObjectRequest.builder()
                .bucket(properties.bucket)
                .key(objectKey)
                .build()
        return s3Client.getObjectAsBytes(request).asByteArray()
    }
}
