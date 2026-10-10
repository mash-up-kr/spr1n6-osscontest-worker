package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.embedding.service.*
import com.osscontest.worker.indexing.embedding.usecase.EmbeddingUseCase
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.time.*

class ExternalApiOperationsTest {
    private class MutableClock(var now: Instant) : Clock() {
        override fun instant() = now
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
    }

    @Test
    fun `429 후 실패한 batch만 재시도하며 이전 embeddings를 보존한다`() {
        val clock = MutableClock(testNow)
        val store: JobQueueStore = mock()
        val failure: IndexingFailureService = mock()
        val database = directDatabase()
        var status = IndexingJobStatus.PROCESSING
        var deadline: Instant? = null
        var attempts = 1
        whenever(store.snapshot(any())).thenAnswer {
            JobSnapshot(testToken.workerId, testToken.epoch, status, testNow.plusSeconds(60), deadline, clock.now)
        }
        whenever(store.dbNow()).thenAnswer { clock.now }
        whenever(store.cooldown()).thenAnswer { deadline?.takeIf { it > clock.now } }
        whenever(store.hasRetries()).thenAnswer { deadline != null }
        whenever(store.resumeRetry(testToken, 5)).thenAnswer { status = IndexingJobStatus.PROCESSING; attempts++; 1 }
        whenever(failure.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull())).thenAnswer {
            status = IndexingJobStatus.RETRY_WAIT
            deadline = it.getArgument(6)
            status
        }
        val waiter = RetryWaiter { clock.now = clock.now.plus(it) }
        val execution = ExecutionOperations(database, store, waiter, DatabaseWaitProperties(), 5)
        val api = ExternalApiOperations(execution, database, store, failure, waiter, clock, DatabaseWaitProperties(), 5, Duration.ofSeconds(30))
        val embedding: EmbeddingUseCase = mock()
        val saved: IndexingPublicationService = mock()
        val first = listOf("first")
        val second = listOf("second")
        whenever(embedding.embed(first)).thenReturn(listOf(FloatArray(1536) { 0.1f }))
        whenever(embedding.embed(second)).thenThrow(EmbeddingRateLimitedException("10", RuntimeException("429")))
            .thenReturn(listOf(FloatArray(1536) { 0.2f }))
        val processor = EmbeddingIndexingProcessor(embedding, saved, NoriTokenizer(), clock, 1, api, execution)
        val context = IndexingContext(1, 2, 3, 1, null, testToken)
        processor.process(context, listOf(
            Chunk(0, "first", "h1", 1, null, null, null, null),
            Chunk(1, "second", "h2", 1, null, null, null, null),
        ))
        verify(embedding).embed(first)
        verify(embedding, times(2)).embed(second)
        verify(saved).publish(eq(context), any())
        assertThat(attempts).isEqualTo(2)
        assertThat(clock.now).isEqualTo(testNow.plusSeconds(10))
        assertThat(api.retryOnly()).isTrue() // PROCESSING resume does not release admission
        api.refreshTargets()
        assertThat(api.retryOnly()).isTrue()
        status = IndexingJobStatus.COMPLETED
        api.refreshTargets()
        assertThat(api.retryOnly()).isFalse()
    }

    @Test
    fun `Retry After의 초 날짜 누락 잘못된 값은 절대 deadline으로 정규화한다`() {
        val fallback = Duration.ofSeconds(30)
        assertThat(ExternalApiOperations.retryDeadline("10", testNow, fallback)).isEqualTo(testNow.plusSeconds(10))
        assertThat(ExternalApiOperations.retryDeadline("Sat, 10 Oct 2026 00:01:00 GMT", testNow, fallback))
            .isEqualTo(testNow.plusSeconds(60))
        assertThat(ExternalApiOperations.retryDeadline("Sat, 10 Oct 2026 00:00:00 GMT", testNow, fallback)).isEqualTo(testNow)
        listOf(null, "garbage", "-1").forEach {
            assertThat(ExternalApiOperations.retryDeadline(it, testNow, fallback)).isEqualTo(testNow.plus(fallback))
        }
    }
}
