package com.osscontest.worker.indexing.parsing

class CorruptedFileException(
    mimeType: String,
    cause: Throwable,
) : RuntimeException("Failed to parse document (mimeType=$mimeType): ${cause.message}", cause) {
    val code: String = "CORRUPTED_FILE"
}
