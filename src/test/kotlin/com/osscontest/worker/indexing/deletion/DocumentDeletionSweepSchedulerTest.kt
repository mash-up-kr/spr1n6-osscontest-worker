package com.osscontest.worker.indexing.deletion

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class DocumentDeletionSweepSchedulerTest {
    private val documentChunkRepository: DocumentChunkRepository = mock()
    private val documentDeletionService: DocumentDeletionService = mock()
    private val scheduler =
        DocumentDeletionSweepScheduler(documentChunkRepository, documentDeletionService, batchSize = 50)

    @Test
    fun `삭제됐는데 chunk가 남은 문서를 찾아 정리한다`() {
        whenever(documentChunkRepository.findDeletedDocumentsWithRemainingChunks(50)).thenReturn(listOf(1L, 2L))

        scheduler.sweepUndeletedChunks()

        verify(documentDeletionService, times(1)).handleDocumentDeleted(1L)
        verify(documentDeletionService, times(1)).handleDocumentDeleted(2L)
    }

    @Test
    fun `한 문서 정리가 실패해도 나머지는 계속 처리한다`() {
        whenever(documentChunkRepository.findDeletedDocumentsWithRemainingChunks(50)).thenReturn(listOf(1L, 2L))
        whenever(documentDeletionService.handleDocumentDeleted(1L)).thenThrow(RuntimeException("boom"))

        scheduler.sweepUndeletedChunks()

        verify(documentDeletionService, times(1)).handleDocumentDeleted(2L)
    }
}
