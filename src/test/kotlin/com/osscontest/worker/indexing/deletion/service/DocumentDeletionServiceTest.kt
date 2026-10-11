package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.publication.repository.*
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.JobOutcome
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.LocalDateTime

class DocumentDeletionServiceTest {
    private val jobs: IndexingJobRepository = mock()
    private val chunks: DocumentChunkRepository = mock()
    private val documents: DocumentRepository = mock()
    private val locks: JobExecutionLocks = mock()
    private val service = DocumentDeletionService(jobs, chunks, documents, JobOwnershipGuard(locks))
    private val job = ClaimedJob(testToken, JobType.DELETE, java.util.UUID.randomUUID(), 2, null, null)

    @Test
    fun `실행권과 삭제 문서를 확인하고 청크 삭제 뒤 Job을 완료한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob().apply { jobType = JobType.DELETE; documentVersionId = null }))
        whenever(documents.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, LocalDateTime.now()))
        whenever(jobs.complete(1, testToken.workerId, testToken.epoch, JobOutcome.DELETED)).thenReturn(1)
        service.delete(job)
        inOrder(chunks, jobs).apply {
            verify(chunks).deleteByDocumentId(2)
            verify(jobs).complete(1, testToken.workerId, testToken.epoch, JobOutcome.DELETED)
        }
    }

    @Test
    fun `이미 완료된 삭제 Job을 재호출하면 삭제하지 않는다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob(status = IndexingJobStatus.COMPLETED)))
        service.delete(job)
        verifyNoInteractions(chunks)
    }

    @Test
    fun `삭제된 문서의 INDEX Job은 SKIPPED로 완료한다`() {
        whenever(jobs.findByIdForUpdate(1)).thenReturn(processingJob())
        whenever(documents.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, LocalDateTime.now()))
        whenever(jobs.complete(1, testToken.workerId, testToken.epoch, JobOutcome.SKIPPED_DOCUMENT_DELETED)).thenReturn(1)

        assertThat(service.skipIndexForDeletedDocument(testToken)).isTrue()

        verify(jobs).complete(1, testToken.workerId, testToken.epoch, JobOutcome.SKIPPED_DOCUMENT_DELETED)
    }

    @Test
    fun `삭제되지 않은 문서는 완료하지 않고 false를 돌려준다`() {
        whenever(jobs.findByIdForUpdate(1)).thenReturn(processingJob())
        whenever(documents.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, null))

        assertThat(service.skipIndexForDeletedDocument(testToken)).isFalse()

        verify(jobs, never()).complete(any(), any(), any(), any())
    }

    @Test
    fun `이미 완료된 Job은 아무것도 쓰지 않고 true를 돌려준다`() {
        whenever(jobs.findByIdForUpdate(1)).thenReturn(processingJob(status = IndexingJobStatus.COMPLETED))

        assertThat(service.skipIndexForDeletedDocument(testToken)).isTrue()

        verifyNoInteractions(documents)
        verify(jobs, never()).complete(any(), any(), any(), any())
    }

    @Test
    fun `완료 갱신이 0행이면 lease 만료로 보고 예외를 던진다`() {
        whenever(jobs.findByIdForUpdate(1)).thenReturn(processingJob())
        whenever(documents.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, LocalDateTime.now()))
        whenever(jobs.complete(1, testToken.workerId, testToken.epoch, JobOutcome.SKIPPED_DOCUMENT_DELETED)).thenReturn(0)

        assertThatThrownBy { service.skipIndexForDeletedDocument(testToken) }.isInstanceOf(LeaseExpiredException::class.java)
    }
}
