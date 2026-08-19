package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.junit.jupiter.api.Test
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock

class DocumentDeletionServiceTest {
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val documentChunkRepository: DocumentChunkRepository = mock()
    private val service = DocumentDeletionService(indexingJobRepository, documentChunkRepository)

    @Test
    fun `활성 Job을 먼저 종결한 뒤 청크를 지운다`() {
        service.handleDocumentDeleted(42L)

        val order = inOrder(indexingJobRepository, documentChunkRepository)
        order.verify(indexingJobRepository).failActiveJobsForDocument(42L)
        order.verify(documentChunkRepository).deleteByDocumentId(42L)
    }
}
