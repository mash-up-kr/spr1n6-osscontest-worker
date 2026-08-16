package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.springframework.stereotype.Service

@Service
class DocumentDeletionService(
    private val indexingJobRepository: IndexingJobRepository,
    private val documentChunkRepository: DocumentChunkRepository,
) {
    /**
     * 멱등하다 — DOCUMENT_DELETED 이벤트 경로와 정리 스윕(DocumentDeletionSweepScheduler)이
     * 둘 다 안전하게, 몇 번이든 호출할 수 있다(§3.9). 원본 파일 삭제와 document.purged_at은
     * API 서버 책임이라 여기서 다루지 않는다.
     */
    fun handleDocumentDeleted(documentId: Long) {
        indexingJobRepository.failActiveJobsForDocument(documentId)
        documentChunkRepository.deleteByDocumentId(documentId)
    }
}
