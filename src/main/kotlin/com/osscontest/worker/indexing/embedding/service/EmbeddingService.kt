package com.osscontest.worker.indexing.embedding.service

import com.openai.errors.BadRequestException
import com.openai.errors.RateLimitException
import com.openai.errors.UnauthorizedException
import com.openai.errors.PermissionDeniedException
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
            // HTTP 400(입력 개수·토큰 상한, 잘못된 dimensions)은 같은 요청으로 다시 보내도 성공하지 않는다.
            // 영구 실패 예외로 바꿔 바로 FAILED로 끝낸다. 5xx·네트워크 오류는 그대로 전파해 breaker가 센다.
            throw EmbeddingRequestRejectedException(e)
        } catch (e: RateLimitException) {
            // Shared quota/configuration failures open the circuit and still use the bounded Job retry path.
            if (e.code().orElse(null) == INSUFFICIENT_QUOTA) throw EmbeddingQuotaExceededException(e)
            throw EmbeddingRateLimitedException(e.headers().values("retry-after").firstOrNull(), e)
        } catch (e: UnauthorizedException) {
            throw EmbeddingConfigurationException("EMBEDDING_AUTHENTICATION_FAILED", e)
        } catch (e: PermissionDeniedException) {
            throw EmbeddingConfigurationException("EMBEDDING_PERMISSION_DENIED", e)
        }

    private companion object {
        const val INSUFFICIENT_QUOTA = "insufficient_quota"
    }
}
