package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.DocumentChunkWriter
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.publication.repository.PublicationStateReader
import com.osscontest.worker.indexing.queue.ClaimedJob
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.JobOutcome
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.JobType
import com.osscontest.worker.indexing.queue.LeaseExpiredException
import com.osscontest.worker.indexing.queue.LockedJobExecution
import com.osscontest.worker.indexing.queue.isCompletedExecution
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional

@Service
class IndexingPublicationService(
    private val documentChunkWriter: DocumentChunkWriter,
    private val documentChunkRepository: DocumentChunkRepository,
    private val documentVersionRepository: DocumentVersionRepository,
    private val documentRepository: DocumentRepository,
    private val indexingJobRepository: IndexingJobRepository,
    private val ownership: JobOwnershipGuard,
    private val state: PublicationStateReader,
) {
    /**
     * 남은 INDEX 작업이 없으면 true를 돌려준다. 이미 완료했거나 삭제된 문서라 생략 완료한 경우다.
     * 호출자는 외부 호출 전에 이 트랜잭션을 끝낸다.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun preflight(claim: ClaimedJob): Boolean {
        check(claim.type == JobType.INDEX)
        val execution = ownership.lockForResult(claim.token)
        val job = execution.job
        check(job.jobType == claim.type && job.documentId == claim.documentId &&
            job.documentVersionId == claim.documentVersionId)
        return finishDeleted(execution, claim.token)
    }

    /**
     * 외부 API 호출 전과 DB 복구 뒤마다 다시 확인한다. 같은 문서 → Job 잠금과 쓰기 검증을 거친다.
     * 그 사이 문서가 삭제됐으면 생략 완료하고 true를 돌려준다.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun preflight(token: ExecutionToken): Boolean = finishDeleted(ownership.lockForResult(token), token)

    private fun finishDeleted(execution: LockedJobExecution, token: ExecutionToken): Boolean {
        check(execution.job.jobType == JobType.INDEX)
        if (execution.job.isCompletedExecution(token)) return true
        ownership.requireProcessing(execution, token)
        if (!state.document(execution.job.documentId).deleted) return false
        complete(token, JobOutcome.SKIPPED_DOCUMENT_DELETED)
        return true
    }

    /**
     * 외부 임베딩 호출은 이 트랜잭션 밖에서 끝난다. 이 메서드는 성공 결과를
     * UPSERT로 수렴시키고 이전 Job이 최신 검색 버전을 덮어쓰지 못하게 fencing한다.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    fun publish(
        context: IndexingContext,
        documentChunks: List<DocumentChunk>,
    ): IndexingJobStatus {
        check(context.executionToken.jobId == context.jobId)
        val execution = ownership.lockForResult(context.executionToken)
        val job = execution.job
        check(job.jobType == JobType.INDEX && job.documentId == context.documentId &&
            job.documentVersionId == context.documentVersionId)
        if (job.isCompletedExecution(context.executionToken)) return IndexingJobStatus.COMPLETED
        ownership.requireProcessing(execution, context.executionToken)
        val document = state.document(context.documentId)
        if (document.deleted) {
            complete(context.executionToken, JobOutcome.SKIPPED_DOCUMENT_DELETED)
            return IndexingJobStatus.COMPLETED
        }
        val candidate = state.version(context.documentVersionId)
        check(candidate.documentId == context.documentId && candidate.versionNo == context.versionNo) {
            "Candidate version does not match the indexing context"
        }
        val current = document.searchableVersionId?.let { state.version(it) }
        check(current == null || current.documentId == context.documentId) { "Invalid searchable version" }
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

        if (current == null || candidate.versionNo > current.versionNo) {
            check(documentRepository.promoteSearchableVersion(context.documentId, context.documentVersionId) == 1) {
                "Document version cannot be promoted"
            }
        }
        complete(context.executionToken, JobOutcome.INDEXED)
        return IndexingJobStatus.COMPLETED
    }

    private fun complete(token: ExecutionToken, outcome: JobOutcome) {
        if (indexingJobRepository.complete(token.jobId, token.workerId, token.epoch, outcome) != 1) {
            throw LeaseExpiredException()
        }
    }
}
