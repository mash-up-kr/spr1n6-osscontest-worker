package com.osscontest.worker.indexing.retry

import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class RetryEventSource(
    private val indexingJobRepository: IndexingJobRepository,
    private val documentRepository: DocumentRepository,
) {
    fun toEvent(jobId: Long): IndexingRequestedEvent? {
        val job = indexingJobRepository.findById(jobId).orElse(null) ?: return null
        val document = documentRepository.findById(job.documentId).orElse(null) ?: return null
        return IndexingRequestedEvent(
            eventId = job.sourceEventId,
            eventType = "INDEXING_REQUESTED",
            eventSchemaVersion = 1,
            tenantId = document.tenantId,
            documentId = job.documentId,
            documentVersionId = job.documentVersionId,
            occurredAt = Instant.now(),
            traceId = job.traceId,
        )
    }
}
