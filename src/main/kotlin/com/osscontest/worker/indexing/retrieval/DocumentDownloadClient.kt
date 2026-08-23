package com.osscontest.worker.indexing.retrieval

import java.nio.file.Path

interface DocumentDownloadClient {
    /**
     * 반환된 [Path]는 호출자가 다 쓴 뒤 반드시 삭제해야 하는 임시 파일이다.
     * AWS SDK 예외는 변환하지 않고 전파하며 파이프라인이 재시도 가능 여부를 판단한다.
     */
    fun download(objectKey: String): Path
}
