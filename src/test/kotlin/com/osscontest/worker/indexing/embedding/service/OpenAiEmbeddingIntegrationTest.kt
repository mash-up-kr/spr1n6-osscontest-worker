package com.osscontest.worker.indexing.embedding.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@Tag("integration")
@EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+")
@SpringBootTest
class OpenAiEmbeddingIntegrationTest
    @Autowired
    constructor(
        private val embeddingService: EmbeddingService,
    ) {
        @Test
        fun `OpenAI가 입력 청크마다 동일한 차원의 임베딩을 반환한다`() {
            val contents =
                listOf(
                    "PostgreSQL은 오픈소스 관계형 데이터베이스이다.",
                    "pgvector는 PostgreSQL에서 벡터 검색을 지원한다.",
                    "벡터 검색은 임베딩 간의 유사도를 이용한다.",
                )

            val embeddings = embeddingService.embed(contents)

            assertEquals(contents.size, embeddings.size)
            assertTrue(embeddings.all { it.isNotEmpty() })
            assertEquals(1, embeddings.map(FloatArray::size).distinct().size)
            assertTrue(embeddings.all { it.size == 1536 })
        }
    }
