package com.osscontest.worker.indexing.embedding.usecase

/**
 * 입력 문자열을 같은 순서와 개수의 임베딩 벡터로 변환한다.
 *
 * 부분 결과는 반환하지 않으며 공급자 거부, 네트워크 장애 등으로 완료할 수 없으면 예외를 던진다.
 */
interface EmbeddingUseCase {
    fun embed(contents: List<String>): List<FloatArray>
}
