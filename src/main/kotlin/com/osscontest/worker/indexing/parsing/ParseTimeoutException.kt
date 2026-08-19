package com.osscontest.worker.indexing.parsing

import java.time.Duration

class ParseTimeoutException(
    mimeType: String,
    timeout: Duration,
) : RuntimeException("Parsing timed out after $timeout (mimeType=$mimeType)") {
    val code: String = "PARSE_TIMEOUT"
}
