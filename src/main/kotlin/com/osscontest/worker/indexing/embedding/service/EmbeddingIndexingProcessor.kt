package com.osscontest.worker.indexing.embedding.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.embedding.usecase.EmbeddingUseCase
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.service.IndexingProcessor
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.LocalDateTime

@Service
class EmbeddingIndexingProcessor(
    private val embeddingUseCase: EmbeddingUseCase,
    private val indexingPublicationService: IndexingPublicationService,
    private val clock: Clock,
) : IndexingProcessor {
    override fun process(
        context: IndexingContext,
        chunks: List<Chunk>,
    ) {
        validateChunks(chunks)
        val embeddings = embeddingUseCase.embed(chunks.map(Chunk::content))
        validateEmbeddings(chunks, embeddings)

        val embeddedAt = LocalDateTime.now(clock)
        val documentChunks =
            chunks.indices.map { index ->
                chunks[index].toDocumentChunk(
                    context = context,
                    embedding = embeddings[index],
                    embeddedAt = embeddedAt,
                )
            }

        indexingPublicationService.publish(context, documentChunks)
    }

    private fun validateChunks(chunks: List<Chunk>) {
        if (chunks.isEmpty()) {
            throw InvalidEmbeddingException("At least one chunk is required")
        }
        if (chunks.map(Chunk::chunkNo) != chunks.indices.toList()) {
            throw InvalidEmbeddingException("Chunk numbers must be contiguous and start at zero")
        }
        if (chunks.any { it.content.isBlank() }) {
            throw InvalidEmbeddingException("Chunk content must not be blank")
        }
        if (chunks.any { it.contentHash.isBlank() }) {
            throw InvalidEmbeddingException("Chunk content hash must not be blank")
        }
    }

    private fun validateEmbeddings(
        chunks: List<Chunk>,
        embeddings: List<FloatArray>,
    ) {
        if (embeddings.size != chunks.size) {
            throw InvalidEmbeddingException(
                "Embedding count mismatch: expected=${chunks.size}, actual=${embeddings.size}",
            )
        }
        if (embeddings.any { it.size != EMBEDDING_DIMENSIONS }) {
            throw InvalidEmbeddingException("Every embedding must have $EMBEDDING_DIMENSIONS dimensions")
        }
        if (embeddings.any { embedding -> embedding.any { !it.isFinite() } }) {
            throw InvalidEmbeddingException("Embedding values must be finite")
        }
    }

    private fun Chunk.toDocumentChunk(
        context: IndexingContext,
        embedding: FloatArray,
        embeddedAt: LocalDateTime,
    ): DocumentChunk =
        DocumentChunk(
            documentVersionId = context.documentVersionId,
            documentId = context.documentId,
            chunkNo = chunkNo,
            content = content,
            contentHash = contentHash,
            tokenCount = tokenCount,
            pageFrom = pageFrom,
            pageTo = pageTo,
            sectionPath = sectionPath,
            metadata = metadata,
            embedding = embedding,
            embeddedAt = embeddedAt,
        )

    private companion object {
        const val EMBEDDING_DIMENSIONS = 1536
    }
}
