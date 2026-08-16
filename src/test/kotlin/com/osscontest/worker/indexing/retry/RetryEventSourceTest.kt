package com.osscontest.worker.indexing.retry

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.util.Optional
import java.util.UUID

class RetryEventSourceTest {
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val documentRepository: DocumentRepository = mock()
    private val retryEventSource = RetryEventSource(indexingJobRepository, documentRepository)

    @Test
    fun `Job과 Document가 모두 존재하면 IndexingRequestedEvent를 재구성한다`() {
        val sourceEventId = UUID.randomUUID()
        val job =
            IndexingJobEntity(
                id = 101L,
                sourceEventId = sourceEventId,
                documentId = 42L,
                documentVersionId = 7L,
                status = IndexingJobStatus.RETRY_WAIT,
                attemptCount = 1,
                nextRetryAt = LocalDateTime.now(),
                workerId = "worker-1",
                lastErrorCode = "SOME_ERROR",
                lastErrorMessage = "boom",
                traceId = "trace-abc",
                startedAt = LocalDateTime.now(),
                completedAt = null,
                updatedAt = LocalDateTime.now(),
            )
        val document = DocumentEntity(id = 42L, tenantId = 7L, searchableVersionId = null, deletedAt = null)
        whenever(indexingJobRepository.findById(101L)).thenReturn(Optional.of(job))
        whenever(documentRepository.findById(42L)).thenReturn(Optional.of(document))

        val before = Instant.now()
        val event = retryEventSource.toEvent(101L)
        val after = Instant.now()

        assertThat(event).isNotNull
        assertThat(event!!.eventId).isEqualTo(sourceEventId)
        assertThat(event.eventType).isEqualTo("INDEXING_REQUESTED")
        assertThat(event.eventSchemaVersion).isEqualTo(1)
        assertThat(event.tenantId).isEqualTo(7L)
        assertThat(event.documentId).isEqualTo(42L)
        assertThat(event.documentVersionId).isEqualTo(7L)
        assertThat(event.traceId).isEqualTo("trace-abc")
        // occurredAt은 최초 발행 시각을 보존하지 않고 재구성 시점("now")을 그대로 쓴다.
        assertThat(event.occurredAt).isBetween(before.minus(Duration.ofSeconds(2)), after.plus(Duration.ofSeconds(2)))
    }

    @Test
    fun `Job이 존재하지 않으면 null을 반환한다`() {
        whenever(indexingJobRepository.findById(999L)).thenReturn(Optional.empty())

        val event = retryEventSource.toEvent(999L)

        assertThat(event).isNull()
    }

    @Test
    fun `Job은 있지만 Document가 존재하지 않으면 null을 반환한다`() {
        val job =
            IndexingJobEntity(
                id = 101L,
                sourceEventId = UUID.randomUUID(),
                documentId = 42L,
                documentVersionId = 7L,
                status = IndexingJobStatus.RETRY_WAIT,
                attemptCount = 1,
                nextRetryAt = LocalDateTime.now(),
                workerId = null,
                lastErrorCode = null,
                lastErrorMessage = null,
                traceId = null,
                startedAt = null,
                completedAt = null,
                updatedAt = LocalDateTime.now(),
            )
        whenever(indexingJobRepository.findById(101L)).thenReturn(Optional.of(job))
        whenever(documentRepository.findById(42L)).thenReturn(Optional.empty())

        val event = retryEventSource.toEvent(101L)

        assertThat(event).isNull()
    }
}
