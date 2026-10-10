package com.osscontest.worker.indexing.embedding.config

import io.micrometer.observation.ObservationRegistry
import org.springframework.ai.embedding.EmbeddingModel
import org.springframework.ai.openai.OpenAiEmbeddingModel
import org.springframework.ai.openai.OpenAiEmbeddingOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class EmbeddingClientConfiguration {
    @Bean
    fun embeddingModel(
        @Value("\${spring.ai.openai.api-key}") apiKey: String,
        @Value("\${spring.ai.openai.base-url:https://api.openai.com/v1}") baseUrl: String,
        @Value("\${spring.ai.openai.embedding.model:text-embedding-3-small}") model: String,
        @Value("\${spring.ai.openai.embedding.timeout:PT60S}") timeout: Duration,
        observations: ObservationRegistry,
    ): EmbeddingModel = OpenAiEmbeddingModel.builder().options(
        OpenAiEmbeddingOptions.builder().apiKey(apiKey).baseUrl(baseUrl).model(model).dimensions(1536)
            .encodingFormat(OpenAiEmbeddingOptions.EncodingFormat.FLOAT).timeout(timeout).maxRetries(0).build(),
    ).observationRegistry(observations).build()
}
