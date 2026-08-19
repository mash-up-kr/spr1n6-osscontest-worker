package com.osscontest.worker.indexing.deletion

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// DOCUMENT_DELETED 이벤트가 유실돼도 유령 청크가 검색에 남지 않도록, 삭제됐는데 아직
// document_chunk가 안 지워진 문서를 주기적으로 찾아 정리하는 내구성 백업 스윕이다.
// 이벤트 경로(DocumentDeletionHandler)와 이 스윕 둘 다 같은 멱등 함수(handleDocumentDeleted)를
// 공유한다 — 인덱싱 재시도 스케줄러(IndexingRetryScheduler, 삭제됨)와는 무관한 별개 컴포넌트다.
@Component
class DocumentDeletionSweepScheduler(
    private val documentChunkRepository: DocumentChunkRepository,
    private val documentDeletionService: DocumentDeletionService,
    @Value("\${indexing.deletion.batch-size:50}")
    private val batchSize: Int = 50,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // fixedDelayString만 쓰고 initialDelay를 안 주면 컨텍스트 기동 즉시 첫 실행이 발생해
    // 다른 기동 초기화와 경쟁하는 버그 패턴이 있다. initialDelayString을
    // 명시해 기동 직후 즉시 실행을 방지한다.
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
