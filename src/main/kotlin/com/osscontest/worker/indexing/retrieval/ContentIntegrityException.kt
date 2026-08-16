package com.osscontest.worker.indexing.retrieval

class ContentIntegrityException(
    val expectedHash: String,
    val actualHash: String,
) : RuntimeException("Content hash mismatch: expected=$expectedHash actual=$actualHash") {
    // indexing_job.last_error_code에 기록되는 스펙 명시 코드(§3.3). InvalidEventException과 같은
    // 규약으로 SCREAMING_SNAKE 코드를 노출해, 클래스 simpleName 폴백에 의존하지 않게 한다.
    val code: String = "HASH_MISMATCH"
}
