package com.osscontest.worker.indexing.chunking.service

class EmptyExtractionException :
    RuntimeException("파싱 결과가 비어 있습니다. 스캔 문서이거나 텍스트 레이어가 없을 수 있습니다.") {
    // indexing_job.last_error_code에 기록되는 스펙 명시 코드(§3.6).
    val code: String = "EMPTY_EXTRACTION"
}
