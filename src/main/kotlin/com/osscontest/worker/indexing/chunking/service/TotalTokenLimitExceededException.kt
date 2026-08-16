package com.osscontest.worker.indexing.chunking.service

class TotalTokenLimitExceededException(
    val actualTotalTokens: Int,
    val maxTotalTokens: Int,
) : RuntimeException("total token count $actualTotalTokens exceeds max $maxTotalTokens") {
    val code: String = "TOTAL_TOKEN_LIMIT_EXCEEDED"
}
