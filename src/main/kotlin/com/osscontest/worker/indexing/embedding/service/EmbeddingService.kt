package com.osscontest.worker.indexing.embedding.service

import com.openai.errors.BadRequestException
import com.openai.errors.RateLimitException
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
        } catch (e: RateLimitException) {
            // 결제 한도 초과도 429로 온다. 충전 전까지 계속 실패하므로 rate limit 경로(시도 횟수 소비)가 아니라
            // circuit breaker로 보내 Job이 대기하게 한다.
            if (e.code().orElse(null) == INSUFFICIENT_QUOTA) throw EmbeddingQuotaExceededException(e)
            throw EmbeddingRateLimitedException(e.headers().values("retry-after").firstOrNull(), e)
        }

    private companion object {
        const val INSUFFICIENT_QUOTA = "insufficient_quota"
    }
}
