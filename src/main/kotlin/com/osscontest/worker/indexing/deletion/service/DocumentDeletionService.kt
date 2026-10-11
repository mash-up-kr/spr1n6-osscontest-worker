package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.JobOutcome
import com.osscontest.worker.indexing.queue.ClaimedJob
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.JobType
import com.osscontest.worker.indexing.queue.JobOwnershipGuard
import com.osscontest.worker.indexing.queue.LeaseExpiredException
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class DocumentDeletionService(
    private val indexingJobRepository: IndexingJobRepository,
    private val documentChunkRepository: DocumentChunkRepository,
    private val documentRepository: DocumentRepository,
    private val ownership: JobOwnershipGuard,
) {
    @Transactional
    fun delete(job: ClaimedJob): IndexingJobStatus {
        require(job.type == JobType.DELETE)
        val owned = ownership.lock(job.token)
        if (owned.status == IndexingJobStatus.COMPLETED) return IndexingJobStatus.COMPLETED
        ownership.requireProcessing(owned, job.token)
        check(owned.jobType == JobType.DELETE && owned.documentId == job.documentId)
        val document = checkNotNull(documentRepository.findByIdForUpdate(job.documentId))
        check(document.deletedAt != null) { "Delete Job requires a deleted document" }
        documentChunkRepository.deleteByDocumentId(job.documentId)
        if (indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch, JobOutcome.DELETED) != 1) {
            throw LeaseExpiredException()
        }
        return IndexingJobStatus.COMPLETED
    }

    /**
     * 삭제된 문서의 `INDEX` Job을 `COMPLETED` + `SKIPPED_DOCUMENT_DELETED`로 종결한다.
     *
     * 문서 행을 잠근 채 삭제 여부를 확인하고 완료하므로 확인과 완료 사이에 틈이 없다.
     * 삭제되지 않은 문서이면 아무것도 쓰지 않고 false를 돌려준다. 이미 완료된 Job이면 true를 돌려준다.
     * lease가 만료됐으면 [LeaseExpiredException]을 던지며, 호출자의 `ExecutionOperations.database()`가 연장 후 재호출한다.
     */
    @Transactional
    fun skipIndexForDeletedDocument(token: ExecutionToken): Boolean {
        val job = ownership.lock(token)
        if (job.status == IndexingJobStatus.COMPLETED) return true
        ownership.requireProcessing(job, token)
        val document = checkNotNull(documentRepository.findByIdForUpdate(job.documentId)) {
            "Document ${job.documentId} does not exist"
        }
        if (document.deletedAt == null) return false
        if (indexingJobRepository.complete(token.jobId, token.workerId, token.epoch, JobOutcome.SKIPPED_DOCUMENT_DELETED) != 1) {
            throw LeaseExpiredException()
        }
        return true
    }
}
