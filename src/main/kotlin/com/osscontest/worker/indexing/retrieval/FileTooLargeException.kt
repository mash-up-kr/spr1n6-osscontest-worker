package com.osscontest.worker.indexing.retrieval

class FileTooLargeException(
    val actualBytes: Long,
    val maxBytes: Long,
) : RuntimeException("file size $actualBytes exceeds max $maxBytes") {
    val code: String = "FILE_TOO_LARGE"
}
