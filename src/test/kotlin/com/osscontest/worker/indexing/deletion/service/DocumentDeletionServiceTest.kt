package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.publication.repository.*
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.support.*
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.LocalDateTime

class DocumentDeletionServiceTest {
    private val jobs: IndexingJobRepository = mock()
    private val chunks: DocumentChunkRepository = mock()
    private val documents: DocumentRepository = mock()
    private val store: JobQueueStore = mock<JobQueueStore>().apply { whenever(dbNow()).thenReturn(testNow) }
    private val service = DocumentDeletionService(jobs, chunks, documents, JobOwnershipGuard(jobs, store))
    private val job = ClaimedJob(testToken, JobType.DELETE, java.util.UUID.randomUUID(), 2, null, null)

    @Test
    fun `실행권과 삭제 문서를 확인하고 청크 삭제 뒤 Job을 완료한다`() {
        whenever(jobs.findByIdForUpdate(1)).thenReturn(processingJob().apply { jobType = JobType.DELETE; documentVersionId = null })
        whenever(documents.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, LocalDateTime.now()))
        whenever(jobs.complete(1, testToken.workerId, testToken.epoch)).thenReturn(1)
        service.delete(job)
        inOrder(chunks, jobs).apply {
            verify(chunks).deleteByDocumentId(2)
            verify(jobs).complete(1, testToken.workerId, testToken.epoch)
        }
    }

    @Test
    fun `이미 완료된 삭제 Job을 재호출하면 삭제하지 않는다`() {
        whenever(jobs.findByIdForUpdate(1)).thenReturn(processingJob(status = IndexingJobStatus.COMPLETED))
        service.delete(job)
        verifyNoInteractions(chunks)
    }
}
