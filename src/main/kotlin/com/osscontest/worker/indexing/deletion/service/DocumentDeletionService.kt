package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.JobOutcome
import com.osscontest.worker.indexing.queue.ClaimedJob
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
}
