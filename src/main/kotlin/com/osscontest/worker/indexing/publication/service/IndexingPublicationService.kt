package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.JobOutcome
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.LeaseExpiredException
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.DocumentChunkWriter
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class IndexingPublicationService(
    private val documentChunkWriter: DocumentChunkWriter,
    private val documentChunkRepository: DocumentChunkRepository,
    private val documentVersionRepository: DocumentVersionRepository,
    private val documentRepository: DocumentRepository,
    private val indexingJobRepository: IndexingJobRepository,
    private val ownership: JobOwnershipGuard,
    private val deletion: DocumentDeletionService,
) {
    /**
     * 외부 임베딩 호출은 이 트랜잭션 밖에서 끝난다. 이 메서드는 성공 결과를
     * UPSERT로 수렴시키고 이전 Job이 최신 검색 버전을 덮어쓰지 못하게 fencing한다.
     */
    @Transactional
    fun publish(
        context: IndexingContext,
        documentChunks: List<DocumentChunk>,
    ): IndexingJobStatus {
        check(context.executionToken.jobId == context.jobId)
        val job = ownership.lock(context.executionToken)
        if (job.status == IndexingJobStatus.COMPLETED) return IndexingJobStatus.COMPLETED
        ownership.requireProcessing(job, context.executionToken)
        check(job.documentId == context.documentId && job.documentVersionId == context.documentVersionId)
        val document = documentRepository.findByIdForUpdate(context.documentId)
            ?: error("Document ${context.documentId} does not exist")
        if (document.deletedAt != null) {
            // 삭제된 문서의 결과는 저장하지 않고 Job을 SKIPPED로 종결한다. 문서 행은 이미 잠겨 있다.
            check(deletion.skipIndexForDeletedDocument(context.executionToken)) { "Deleted document must be skipped" }
            return IndexingJobStatus.COMPLETED
        }
        check(documentChunks.isNotEmpty()) { "At least one document chunk is required" }
        check(documentChunks.all { it.documentId == context.documentId }) {
            "Every chunk must belong to document ${context.documentId}"
        }
        check(documentChunks.all { it.documentVersionId == context.documentVersionId }) {
            "Every chunk must belong to document version ${context.documentVersionId}"
        }

        documentChunkWriter.upsertAll(documentChunks)
        val lastChunkNo = documentChunks.maxOf(DocumentChunk::chunkNo)
        documentChunkRepository.deleteTrailingChunks(context.documentVersionId, lastChunkNo)

        val storedChunkCount = documentChunkRepository.countByDocumentVersionId(context.documentVersionId)
        check(storedChunkCount == documentChunks.size.toLong()) {
            "Stored chunk count mismatch: expected=${documentChunks.size}, actual=$storedChunkCount"
        }

        check(
            documentVersionRepository.complete(
                documentVersionId = context.documentVersionId,
                documentId = context.documentId,
                chunkCount = documentChunks.size,
                extractedMetadata = context.extractedMetadata,
            ) == 1,
        ) {
            "Document version ${context.documentVersionId} could not be completed"
        }

        check(documentRepository.promoteSearchableVersion(context.documentId, context.documentVersionId) == 1) {
            "Document version cannot be promoted"
        }
        if (indexingJobRepository.complete(context.jobId, context.executionToken.workerId, context.executionToken.epoch, JobOutcome.INDEXED) != 1) {
            throw LeaseExpiredException()
        }
        return IndexingJobStatus.COMPLETED
    }
}
