package com.osscontest.worker.indexing.parsing

class UnsupportedMimeTypeException(
    val mimeType: String,
) : RuntimeException("No parser registered for mimeType=$mimeType") {
    val code: String = "UNSUPPORTED_MIME_TYPE"
}
