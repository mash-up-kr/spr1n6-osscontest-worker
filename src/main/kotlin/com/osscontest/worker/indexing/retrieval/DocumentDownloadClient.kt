package com.osscontest.worker.indexing.retrieval

import java.nio.file.Path

interface DocumentDownloadClient {
    /**
     * 반환된 [Path]는 호출자가 다 쓴 뒤 반드시 삭제해야 하는 임시 파일이다.
     * AWS SDK exceptions (e.g. [software.amazon.awssdk.services.s3.model.NoSuchKeyException],
     * [software.amazon.awssdk.core.exception.SdkClientException]) propagate unmodified — this is
     * an intentional contract, not an oversight; error handling is deferred to the pipeline runner.
     */
    fun download(objectKey: String): Path
}
