package com.osscontest.worker.indexing.deletion

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class DocumentDeletionSweepScheduler(
    private val documentChunkRepository: DocumentChunkRepository,
    private val documentDeletionService: DocumentDeletionService,
    @Value("\${indexing.deletion.batch-size:50}")
    private val batchSize: Int = 50,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // Task 17/18에서 발견된 버그(IndexingRetryScheduler의 fixedDelayString에 initialDelay가
    // 없으면 컨텍스트 기동 즉시 첫 실행이 발생 — 다른 기동 초기화와 경쟁)와 같은 패턴을 여기서도
    // 선제적으로 피한다. initialDelayString을 명시해 기동 직후 즉시 실행을 방지한다.
    @Scheduled(
        initialDelayString = "\${indexing.deletion.initial-delay-ms:60000}",
        fixedDelayString = "\${indexing.deletion.sweep-interval-ms:60000}",
    )
    fun sweepUndeletedChunks() {
        val documentIds = documentChunkRepository.findDeletedDocumentsWithRemainingChunks(batchSize)
        for (documentId in documentIds) {
            try {
                documentDeletionService.handleDocumentDeleted(documentId)
            } catch (e: Exception) {
                log.warn("deletion sweep failed for document {}", documentId, e)
            }
        }
    }
}
