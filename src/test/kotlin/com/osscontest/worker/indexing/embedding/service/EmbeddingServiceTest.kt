package com.osscontest.worker.indexing.embedding.service

import com.openai.core.http.Headers
import com.openai.errors.BadRequestException
import com.openai.errors.RateLimitException
import com.openai.models.ErrorObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.ai.embedding.EmbeddingModel

class EmbeddingServiceTest {
    private val embeddingModel = Mockito.mock(EmbeddingModel::class.java)
    private val embeddingService = EmbeddingService(embeddingModel)

    @Test
    fun `입력 청크마다 동일한 차원의 임베딩을 반환한다`() {
        val contents =
            listOf(
                "PostgreSQL은 오픈소스 관계형 데이터베이스이다.",
                "pgvector는 PostgreSQL에서 벡터 검색을 지원한다.",
                "벡터 검색은 임베딩 간의 유사도를 이용한다.",
            )
        val expected =
            listOf(
                FloatArray(1536) { 0.1f },
                FloatArray(1536) { 0.2f },
                FloatArray(1536) { 0.3f },
            )
        Mockito.`when`(embeddingModel.embed(contents)).thenReturn(expected)

        val embeddings = embeddingService.embed(contents)

        assertEquals(contents.size, embeddings.size)
        assertTrue(embeddings.all { it.isNotEmpty() })
        assertEquals(1, embeddings.map(FloatArray::size).distinct().size)
        Mockito.verify(embeddingModel).embed(contents)
    }

    @Test
    fun `OpenAI가 HTTP 400으로 요청을 거부하면 영구 실패 예외로 변환한다`() {
        val contents = listOf("oversized content")
        val cause = Mockito.mock(BadRequestException::class.java)
        Mockito.`when`(cause.message).thenReturn("maximum input tokens exceeded")
        Mockito.`when`(embeddingModel.embed(contents)).thenThrow(cause)

        val exception =
            assertThrows(EmbeddingRequestRejectedException::class.java) {
                embeddingService.embed(contents)
            }

        assertEquals("EMBEDDING_REQUEST_REJECTED", exception.code)
        assertTrue(exception.message!!.contains("maximum input tokens exceeded"))
        assertSame(cause, exception.cause)
    }

    private fun rateLimit(code: String): RateLimitException =
        RateLimitException.builder()
            .headers(Headers.builder().put("retry-after", "7").build())
            .error(ErrorObject.builder().code(code).message("429").param(null as String?).type("requests").build())
            .build()

    @Test
    fun `OpenAI가 결제 한도 초과 429를 보내면 rate limit이 아닌 한도 초과 예외로 변환한다`() {
        val contents = listOf("content")
        val cause = rateLimit("insufficient_quota")
        Mockito.`when`(embeddingModel.embed(contents)).thenThrow(cause)

        val exception = assertThrows(EmbeddingQuotaExceededException::class.java) { embeddingService.embed(contents) }

        assertSame(cause, exception.cause)
    }

    @Test
    fun `일반 429는 Retry-After와 함께 rate limit 예외로 변환한다`() {
        val contents = listOf("content")
        val cause = rateLimit("rate_limit_exceeded")
        Mockito.`when`(embeddingModel.embed(contents)).thenThrow(cause)

        val exception = assertThrows(EmbeddingRateLimitedException::class.java) { embeddingService.embed(contents) }

        assertEquals("7", exception.retryAfter)
    }
}
