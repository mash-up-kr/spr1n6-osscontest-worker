package com.osscontest.worker.indexing.retrieval

interface DocumentDownloadClient {
    /**
     * AWS SDK exceptions (e.g. [software.amazon.awssdk.services.s3.model.NoSuchKeyException],
     * [software.amazon.awssdk.core.exception.SdkClientException]) propagate unmodified — this is
     * an intentional contract, not an oversight; error handling is deferred to the pipeline runner.
     */
    fun download(objectKey: String): ByteArray
}
