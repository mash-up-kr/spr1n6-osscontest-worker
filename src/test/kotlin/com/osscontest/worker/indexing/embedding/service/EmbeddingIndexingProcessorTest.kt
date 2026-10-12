package com.osscontest.worker.indexing.embedding.service

import com.osscontest.worker.support.directDatabase
import com.osscontest.worker.support.closedCircuitBreaker
import com.osscontest.worker.indexing.queue.DatabaseWaitProperties

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.embedding.usecase.EmbeddingUseCase
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.assertj.core.api.Assertions.assertThat
import com.osscontest.worker.indexing.queue.ExternalApiOperations
import com.osscontest.worker.support.directExecution
import com.osscontest.worker.support.testToken
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.verifyNoInteractions
import com.osscontest.worker.indexing.queue.DatabaseOperations
import com.osscontest.worker.indexing.queue.ExecutionLostException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class EmbeddingIndexingProcessorTest {
    private val embeddingUseCase: EmbeddingUseCase = mock()
    private val publicationService: IndexingPublicationService = mock()
    private val externalApi: ExternalApiOperations = mock<ExternalApiOperations>().apply {
        whenever(execute<Any?>(any(), any(), any())).thenAnswer { it.getArgument<() -> Any?>(2)() }
    }
    private val processor =
        EmbeddingIndexingProcessor(
            embeddingUseCase = embeddingUseCase,
            indexingPublicationService = publicationService,
            noriTokenizer = NoriTokenizer(),
            clock = Clock.fixed(Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC),
            maxTokensPerRequest = 250,
            externalApi = externalApi,
            execution = directExecution(),
        )

    @Test
    fun `publish 응답 유실은 완료 증거 확인 후 임베딩과 저장을 반복하지 않는다`() {
        publishWithDatabaseFault(committed = true)
    }

    @Test
    fun `publish 롤백 DB 장애는 보관한 결과 쓰기만 다시 호출한다`() {
        publishWithDatabaseFault(committed = false)
    }

    private fun publishWithDatabaseFault(committed: Boolean) {
        val store: com.osscontest.worker.indexing.queue.JobQueueStore = mock()
        var receipt = com.osscontest.worker.indexing.queue.JobSnapshot(testToken.workerId, 1,
            com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.PROCESSING,
            com.osscontest.worker.support.testNow.plusSeconds(60), null, com.osscontest.worker.support.testNow, 1,
            type = com.osscontest.worker.indexing.queue.JobType.INDEX, documentId = 2, documentVersionId = 3)
        whenever(store.snapshot(testToken)).thenAnswer { receipt }
        val retained = mutableListOf<List<DocumentChunk>>()
        whenever(publicationService.publish(any(), any())).thenAnswer {
            retained.add(it.getArgument(1))
            if (retained.size == 1) {
                if (committed) receipt = receipt.copy(workerId = null, leaseUntil = null, documentEpoch = 2,
                    status = com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.COMPLETED, outcome = "INDEXED")
                throw org.springframework.dao.DataAccessResourceFailureException("save response lost")
            }
            com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.COMPLETED
        }
        whenever(embeddingUseCase.embed(any())).thenReturn(listOf(embedding(0.1f)))
        val actual = EmbeddingIndexingProcessor(embeddingUseCase, publicationService, NoriTokenizer(),
            Clock.fixed(Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC), 250, externalApi,
            com.osscontest.worker.indexing.queue.ExecutionOperations(com.osscontest.worker.support.retryingDatabase(), store))
        actual.process(context(), listOf(chunk(0)))
        verify(embeddingUseCase, times(1)).embed(any())
        assertThat(retained).hasSize(if (committed) 1 else 2)
        if (!committed) assertThat(retained[1]).isSameAs(retained[0])
    }

    @Test
    fun `임베딩 결과를 publication에 위임한다`() {
        val chunks = listOf(chunk(no = 0), chunk(no = 1))
        whenever(embeddingUseCase.embed(chunks.map(Chunk::content)))
            .thenReturn(listOf(embedding(0.1f), embedding(0.2f)))

        processor.process(context(), chunks)

        verify(publicationService).publish(any(), any<List<DocumentChunk>>())
    }

    @Test
    fun `임베딩 차원 불일치는 영구 실패 예외로 종결한다`() {
        val chunks = listOf(chunk(no = 0))
        whenever(embeddingUseCase.embed(chunks.map(Chunk::content)))
            .thenReturn(listOf(FloatArray(1535) { 0.1f }))

        assertThatThrownBy { processor.process(context(), chunks) }
            .isInstanceOf(InvalidEmbeddingException::class.java)

        verify(publicationService, never()).publish(any(), any())
    }

    @Test
    fun `청크 토큰 합이 요청당 상한을 넘으면 여러 번 나눠 임베딩을 요청한다`() {
        val chunks = listOf(chunkWithTokens(no = 0, tokens = 100), chunkWithTokens(no = 1, tokens = 100), chunkWithTokens(no = 2, tokens = 100))
        whenever(embeddingUseCase.embed(listOf("content-0", "content-1")))
            .thenReturn(listOf(embedding(0.1f), embedding(0.2f)))
        whenever(embeddingUseCase.embed(listOf("content-2")))
            .thenReturn(listOf(embedding(0.3f)))

        processor.process(context(), chunks)

        verify(embeddingUseCase, times(2)).embed(any())
        val captor = org.mockito.kotlin.argumentCaptor<List<DocumentChunk>>()
        verify(publicationService).publish(any(), captor.capture())
        assertThat(captor.firstValue.map { it.chunkNo }).containsExactly(0, 1, 2)
        assertThat(captor.firstValue.map { it.embedding[0] }).containsExactly(0.1f, 0.2f, 0.3f)
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = [false, true])
    fun `배치 사이 DB 대기에는 다음 외부 호출을 시작하지 않고 복구 뒤 실행권부터 확인한다`(lost: Boolean) {
        val database: DatabaseOperations = directDatabase()
        val execution = directExecution()
        val waiting = CountDownLatch(1)
        val recover = CountDownLatch(1)
        val gates = AtomicInteger()
        val checkpoints = AtomicInteger()
        doAnswer {
            if (gates.incrementAndGet() == 2) {
                waiting.countDown()
                check(recover.await(5, TimeUnit.SECONDS))
            }
            null
        }.whenever(database).awaitAvailable()
        doAnswer {
            if (checkpoints.incrementAndGet() == 3 && lost) throw ExecutionLostException(testToken)
            null
        }.whenever(execution).checkpoint(testToken)
        whenever(embeddingUseCase.embed(listOf("content-0"))).thenReturn(listOf(embedding(0.1f)))
        whenever(embeddingUseCase.embed(listOf("content-1"))).thenReturn(listOf(embedding(0.2f)))
        whenever(publicationService.publish(any(), any())).thenReturn(com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.COMPLETED)
        val actual = EmbeddingIndexingProcessor(embeddingUseCase, publicationService, NoriTokenizer(),
            Clock.systemUTC(), 100, ExternalApiOperations(execution, database, closedCircuitBreaker(), com.osscontest.worker.indexing.pipeline.service.RetryWaiter {}, DatabaseWaitProperties(), publicationService), execution)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val running = pool.submit {
                actual.process(context(), listOf(chunkWithTokens(0, 100), chunkWithTokens(1, 100)))
            }
            check(waiting.await(3, TimeUnit.SECONDS))
            verify(embeddingUseCase, times(1)).embed(listOf("content-0"))
            verify(embeddingUseCase, never()).embed(listOf("content-1"))
            verify(publicationService, never()).publish(any(), any())
            assertThat(checkpoints.get()).isEqualTo(2)
            recover.countDown()
            if (lost) {
                assertThatThrownBy { running.get(3, TimeUnit.SECONDS) }.hasCauseInstanceOf(ExecutionLostException::class.java)
                verify(embeddingUseCase, never()).embed(listOf("content-1"))
                verify(publicationService, never()).publish(any(), any())
            } else {
                running.get(3, TimeUnit.SECONDS)
                verify(embeddingUseCase, times(1)).embed(listOf("content-1"))
                val results = org.mockito.kotlin.argumentCaptor<List<DocumentChunk>>()
                verify(publicationService).publish(any(), results.capture())
                assertThat(results.firstValue.map { it.embedding[0] }).containsExactly(0.1f, 0.2f)
            }
            assertThat(checkpoints.get()).isEqualTo(if (lost) 3 else 4)
            verify(embeddingUseCase, times(1)).embed(listOf("content-0"))
        } finally { recover.countDown(); pool.shutdownNow() }
    }

    private fun context() =
        IndexingContext(
            jobId = 1L,
            documentId = 2L,
            documentVersionId = 3L,
            versionNo = 1L,
            extractedMetadata = null,
            executionToken = testToken,
        )

    private fun chunk(no: Int) = chunkWithTokens(no, tokens = 1)

    private fun chunkWithTokens(
        no: Int,
        tokens: Int,
    ) = Chunk(
        chunkNo = no,
        content = "content-$no",
        contentHash = "hash-$no",
        tokenCount = tokens,
        pageFrom = null,
        pageTo = null,
        sectionPath = null,
        metadata = null,
    )

    private fun embedding(value: Float): FloatArray = FloatArray(1536) { value }
}
