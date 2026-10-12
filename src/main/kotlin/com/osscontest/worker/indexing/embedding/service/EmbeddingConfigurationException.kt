package com.osscontest.worker.indexing.embedding.service

/** 공급자 자격 증명·권한 오류. 요청별 입력 오류나 rate limit과 달리 모든 호출이 실패한다. */
class EmbeddingConfigurationException(val code: String, cause: Throwable) : RuntimeException(code, cause)
