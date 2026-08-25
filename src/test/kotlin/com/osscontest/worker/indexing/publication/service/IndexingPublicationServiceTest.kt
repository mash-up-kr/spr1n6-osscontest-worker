package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.DocumentChunkWriter
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.LocalDateTime

class IndexingPublicationServiceTest {
    private val documentChunkWriter: DocumentChunkWriter = mock()
    private val documentChunkRepository: DocumentChunkRepository = mock()
    private val documentVersionRepository: DocumentVersionRepository = mock()
    private val documentRepository: DocumentRepository = mock()
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val service =
        IndexingPublicationService(
            documentChunkWriter,
            documentChunkRepository,
            documentVersionRepository,
            documentRepository,
            indexingJobRepository,
        )

    @Test
    fun `검색 버전 승격이 스킵되어도 Job은 COMPLETED로 종결한다`() {
        val context = context()
        val chunks = listOf(chunk(0), chunk(1))
        whenever(documentChunkRepository.countByDocumentVersionId(context.documentVersionId)).thenReturn(2)
        whenever(
            documentVersionRepository.complete(
                context.documentVersionId,
                context.documentId,
                chunks.size,
                context.extractedMetadata,
            ),
        ).thenReturn(1)
        whenever(documentRepository.promoteSearchableVersion(context.documentId, context.documentVersionId))
            .thenReturn(0)
        whenever(indexingJobRepository.complete(context.jobId)).thenReturn(1)

        val status = service.publish(context, chunks)

        assertThat(status).isEqualTo(IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter).upsertAll(chunks)
        verify(documentChunkRepository).deleteTrailingChunks(context.documentVersionId, 1)
        verify(indexingJobRepository).complete(context.jobId)
    }

    private fun context() =
        IndexingContext(
            jobId = 1L,
            documentId = 2L,
            documentVersionId = 3L,
            versionNo = 1L,
            extractedMetadata = mapOf("language" to "ko"),
        )

    private fun chunk(no: Int) =
        DocumentChunk(
            documentVersionId = 3L,
            documentId = 2L,
            chunkNo = no,
            content = "content-$no",
            contentTokens = "content $no",
            contentHash = "hash-$no",
            tokenCount = 1,
            pageFrom = null,
            pageTo = null,
            sectionPath = null,
            metadata = null,
            embedding = FloatArray(1536) { 0.1f },
            embeddedAt = LocalDateTime.of(2026, 8, 19, 0, 0),
        )
}
