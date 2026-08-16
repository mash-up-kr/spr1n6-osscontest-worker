package com.osscontest.worker.indexing.retry

import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID

class IndexingRetrySchedulerTest {
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val retryEventSource: RetryEventSource = mock()
    private val pipelineRunner: IndexingPipelineRunner = mock()
    private val scheduler =
        IndexingRetryScheduler(indexingJobRepository, retryEventSource, pipelineRunner, batchSize = 50)

    @Test
    fun `due한 RETRY_WAIT Job을 찾아 다시 처리한다`() {
        whenever(indexingJobRepository.findRetryWaitDue(org.mockito.kotlin.any(), org.mockito.kotlin.eq(50)))
            .thenReturn(listOf(101L, 102L))
        val event101 = sampleEvent(101L)
        val event102 = sampleEvent(102L)
        whenever(retryEventSource.toEvent(101L)).thenReturn(event101)
        whenever(retryEventSource.toEvent(102L)).thenReturn(event102)

        scheduler.retryDueJobs()

        verify(pipelineRunner, times(1)).run(event101)
        verify(pipelineRunner, times(1)).run(event102)
    }

    @Test
    fun `한 Job이 예외를 던져도 나머지 Job은 계속 처리한다`() {
        whenever(indexingJobRepository.findRetryWaitDue(org.mockito.kotlin.any(), org.mockito.kotlin.eq(50)))
            .thenReturn(listOf(101L, 102L))
        val event101 = sampleEvent(101L)
        val event102 = sampleEvent(102L)
        whenever(retryEventSource.toEvent(101L)).thenReturn(event101)
        whenever(retryEventSource.toEvent(102L)).thenReturn(event102)
        whenever(pipelineRunner.run(event101)).thenThrow(RuntimeException("boom"))

        scheduler.retryDueJobs()

        verify(pipelineRunner, times(1)).run(event102)
    }

    @Test
    fun `toEvent가 null을 반환하면 해당 Job은 건너뛰고 나머지는 계속 처리한다`() {
        whenever(indexingJobRepository.findRetryWaitDue(org.mockito.kotlin.any(), org.mockito.kotlin.eq(50)))
            .thenReturn(listOf(101L, 102L))
        val event102 = sampleEvent(102L)
        whenever(retryEventSource.toEvent(101L)).thenReturn(null)
        whenever(retryEventSource.toEvent(102L)).thenReturn(event102)

        scheduler.retryDueJobs()

        // toEvent(101L)이 null이라 job 101에 대응하는 이벤트 자체가 만들어지지 않으므로,
        // run()이 정확히 한 번(102에 대해서만) 호출됐는지로 101이 건너뛰어졌음을 검증한다.
        verify(pipelineRunner, times(1)).run(org.mockito.kotlin.any())
        verify(pipelineRunner, times(1)).run(event102)
    }

    private fun sampleEvent(jobId: Long) =
        IndexingRequestedEvent(
            eventId = UUID.randomUUID(),
            eventType = "INDEXING_REQUESTED",
            eventSchemaVersion = 1,
            tenantId = 7L,
            documentId = 42L,
            documentVersionId = jobId,
            occurredAt = Instant.now(),
            traceId = null,
        )
}
