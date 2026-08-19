package com.osscontest.worker.indexing.embedding.service

import com.openai.errors.BadRequestException
import com.osscontest.worker.indexing.embedding.usecase.EmbeddingUseCase
import org.springframework.ai.embedding.EmbeddingModel
import org.springframework.stereotype.Service

@Service
class EmbeddingService(
    private val embeddingModel: EmbeddingModel,
) : EmbeddingUseCase {
    override fun embed(contents: List<String>): List<FloatArray> =
        try {
            embeddingModel.embed(contents)
        } catch (e: BadRequestException) {
            // 입력 개수/토큰 상한, 잘못된 dimensions 등 HTTP 400은 같은 요청으로 재시도해도
            // 성공하지 않는다. 파이프라인이 즉시 FAILED로 종결하고 공급자 원인을 기록할 수
            // 있도록 전용 영구 실패 예외로 변환한다. 429/5xx/네트워크 오류는 그대로 전파해
            // 기존 인라인 재시도 대상에 남긴다.
            throw EmbeddingRequestRejectedException(e)
        }
}
