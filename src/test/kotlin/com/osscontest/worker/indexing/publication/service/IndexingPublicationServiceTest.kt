package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.JobOutcome
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.DocumentChunkWriter
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.assertj.core.api.Assertions.assertThat
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.mockito.kotlin.times
import org.mockito.kotlin.never
import org.mockito.kotlin.any
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
    private val store: JobQueueStore = mock<JobQueueStore>().apply { whenever(dbNow()).thenReturn(testNow) }
    private val ownership = JobOwnershipGuard(indexingJobRepository, store)
    private val deletion = DocumentDeletionService(indexingJobRepository, documentChunkRepository, documentRepository, ownership)
    private val service =
        IndexingPublicationService(
            documentChunkWriter,
            documentChunkRepository,
            documentVersionRepository,
            documentRepository,
            indexingJobRepository,
            ownership,
            deletion,
        )

    @Test
    fun `청크와 검색 버전 저장 후 같은 실행만 완료한다`() {
        val context = context()
        whenever(indexingJobRepository.findByIdForUpdate(1)).thenReturn(processingJob())
        whenever(documentRepository.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, null))
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
            .thenReturn(1)
        whenever(indexingJobRepository.complete(context.jobId, testToken.workerId, testToken.epoch, JobOutcome.INDEXED)).thenReturn(1)

        val status = service.publish(context, chunks)

        assertThat(status).isEqualTo(IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter).upsertAll(chunks)
        verify(documentChunkRepository).deleteTrailingChunks(context.documentVersionId, 1)
        verify(indexingJobRepository).complete(context.jobId, testToken.workerId, testToken.epoch, JobOutcome.INDEXED)
    }

    @Test
    fun `커밋 응답 유실 후 같은 완료 실행은 결과를 다시 쓰지 않는다`() {
        val job = processingJob(status = com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.COMPLETED)
        whenever(indexingJobRepository.findByIdForUpdate(1)).thenReturn(job)
        assertThat(service.publish(context(), listOf(chunk(0))))
            .isEqualTo(com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter, never()).upsertAll(any())
        verify(indexingJobRepository, never()).complete(any(), any(), any(), any())
    }

    @Test
    fun `회수된 이전 epoch의 결과는 쓰지 않는다`() {
        val job = processingJob().apply { executionEpoch++ }
        whenever(indexingJobRepository.findByIdForUpdate(1)).thenReturn(job)
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(ExecutionLostException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
    }

    @Test
    fun `만료된 lease이면 청크를 쓰기 전에 트랜잭션을 종료한다`() {
        val job = processingJob().apply { leaseUntil = testNow.minusSeconds(1) }
        whenever(indexingJobRepository.findByIdForUpdate(1)).thenReturn(job)
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(LeaseExpiredException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
    }

    @Test
    fun `저장 시점에 문서가 삭제됐으면 청크를 쓰지 않고 SKIPPED로 완료한다`() {
        whenever(indexingJobRepository.findByIdForUpdate(1)).thenReturn(processingJob())
        whenever(documentRepository.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, LocalDateTime.now()))
        whenever(
            indexingJobRepository.complete(1, testToken.workerId, testToken.epoch, JobOutcome.SKIPPED_DOCUMENT_DELETED),
        ).thenReturn(1)

        val status = service.publish(context(), listOf(chunk(0)))

        assertThat(status).isEqualTo(IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter, never()).upsertAll(any())
        verify(documentRepository, never()).promoteSearchableVersion(any(), any())
        verify(indexingJobRepository).complete(1, testToken.workerId, testToken.epoch, JobOutcome.SKIPPED_DOCUMENT_DELETED)
    }

    private fun context() =
        IndexingContext(
            jobId = 1L,
            documentId = 2L,
            documentVersionId = 3L,
            versionNo = 1L,
            extractedMetadata = mapOf("language" to "ko"),
            executionToken = testToken,
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
