package com.osscontest.worker.indexing.embedding.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.embedding.usecase.EmbeddingUseCase
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.assertj.core.api.Assertions.assertThat
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class EmbeddingIndexingProcessorTest {
    private val embeddingUseCase: EmbeddingUseCase = mock()
    private val publicationService: IndexingPublicationService = mock()
    private val processor =
        EmbeddingIndexingProcessor(
            embeddingUseCase = embeddingUseCase,
            indexingPublicationService = publicationService,
            clock = Clock.fixed(Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC),
            maxTokensPerRequest = 250,
        )

    @Test
    fun `임베딩 결과를 publication에 위임한다`() {
        val chunks = listOf(chunk(no = 0), chunk(no = 1))
        whenever(embeddingUseCase.embed(chunks.map(Chunk::content)))
            .thenReturn(listOf(embedding(0.1f), embedding(0.2f)))

        processor.process(context(), chunks)

        verify(publicationService).publish(any(), any<List<DocumentChunk>>())
    }

    @Test
    fun `임베딩 차원 불일치는 영구 실패 예외로 종결한다`() {
        val chunks = listOf(chunk(no = 0))
        whenever(embeddingUseCase.embed(chunks.map(Chunk::content)))
            .thenReturn(listOf(FloatArray(1535) { 0.1f }))

        assertThatThrownBy { processor.process(context(), chunks) }
            .isInstanceOf(InvalidEmbeddingException::class.java)

        verify(publicationService, never()).publish(any(), any())
    }

    @Test
    fun `청크 토큰 합이 요청당 상한을 넘으면 여러 번 나눠 임베딩을 요청한다`() {
        val chunks = listOf(chunkWithTokens(no = 0, tokens = 100), chunkWithTokens(no = 1, tokens = 100), chunkWithTokens(no = 2, tokens = 100))
        whenever(embeddingUseCase.embed(listOf("content-0", "content-1")))
            .thenReturn(listOf(embedding(0.1f), embedding(0.2f)))
        whenever(embeddingUseCase.embed(listOf("content-2")))
            .thenReturn(listOf(embedding(0.3f)))

        processor.process(context(), chunks)

        verify(embeddingUseCase, times(2)).embed(any())
        val captor = org.mockito.kotlin.argumentCaptor<List<DocumentChunk>>()
        verify(publicationService).publish(any(), captor.capture())
        assertThat(captor.firstValue.map { it.chunkNo }).containsExactly(0, 1, 2)
        assertThat(captor.firstValue.map { it.embedding[0] }).containsExactly(0.1f, 0.2f, 0.3f)
    }

    private fun context() =
        IndexingContext(
            jobId = 1L,
            documentId = 2L,
            documentVersionId = 3L,
            versionNo = 1L,
            extractedMetadata = null,
        )

    private fun chunk(no: Int) = chunkWithTokens(no, tokens = 1)

    private fun chunkWithTokens(
        no: Int,
        tokens: Int,
    ) = Chunk(
        chunkNo = no,
        content = "content-$no",
        contentHash = "hash-$no",
        tokenCount = tokens,
        pageFrom = null,
        pageTo = null,
        sectionPath = null,
        metadata = null,
    )

    private fun embedding(value: Float): FloatArray = FloatArray(1536) { value }
}
