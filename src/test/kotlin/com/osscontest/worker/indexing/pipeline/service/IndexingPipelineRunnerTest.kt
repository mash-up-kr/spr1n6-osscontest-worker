package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.consumer.IndexingEventValidator
import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.consumer.InvalidEventException
import com.osscontest.worker.indexing.parsing.DocumentParser
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.util.Optional
import java.util.UUID

class IndexingPipelineRunnerTest {
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val documentRepository: DocumentRepository = mock()
    private val eventValidator: IndexingEventValidator = mock()
    private val downloadClient: DocumentDownloadClient = mock()
    private val parserRegistry: DocumentParserRegistry = mock()
    private val chunkingService: ChunkingService = mock()
    private val chunkGuard: ChunkGuard = mock()
    private val indexingProcessor = FakeIndexingProcessor()
    private val indexingFailureService: IndexingFailureService = mock()
    private val maxAttempts = 5
    private val baseDelay: Duration = Duration.ofSeconds(30)

    private val runner = newRunner()

    private fun newRunner(strategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN) =
        IndexingPipelineRunner(
            indexingJobRepository = indexingJobRepository,
            documentRepository = documentRepository,
            eventValidator = eventValidator,
            downloadClient = downloadClient,
            parserRegistry = parserRegistry,
            chunkingService = chunkingService,
            chunkGuard = chunkGuard,
            indexingProcessor = indexingProcessor,
            indexingFailureService = indexingFailureService,
            workerId = "worker-test",
            maxAttempts = maxAttempts,
            baseDelay = baseDelay,
            chunkingStrategy = strategy,
        )

    @Test
    fun `이미 더 최신 버전이 searchable이면 다운로드하지 않고 완료 처리한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(documentRepository.findSearchableEmbeddingVersionNo(42L)).thenReturn(5L) // 3보다 큼 → STALE

        runner.run(sampleEvent())

        verify(downloadClient, never()).download(any())
        assertThat(indexingProcessor.calls).isEmpty()
        verify(indexingJobRepository).complete(5001L)
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any())
    }

    @Test
    fun `정상 흐름이면 다운로드부터 청킹까지 수행하고 IndexingProcessor를 호출한다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        // searchable이 없거나(null) 현재 버전보다 낮음 → STALE 아님 → 정상 진행
        whenever(documentRepository.findSearchableEmbeddingVersionNo(42L)).thenReturn(null)
        whenever(downloadClient.download("k")).thenReturn(bytes)

        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)

        val chunk = Chunk(
            chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2,
            pageFrom = null, pageTo = null, sectionPath = null, metadata = null,
        )
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.FIXED_TOKEN))).thenReturn(listOf(chunk))

        runner.run(sampleEvent())

        assertThat(indexingProcessor.calls).hasSize(1)
        val (context, chunks) = indexingProcessor.calls.single()
        assertThat(context.jobId).isEqualTo(5001L)
        assertThat(context.documentId).isEqualTo(42L)
        assertThat(context.documentVersionId).isEqualTo(1001L)
        assertThat(context.versionNo).isEqualTo(1L)
        assertThat(chunks).containsExactly(chunk)
        verify(chunkGuard).assertValid(listOf(chunk))
        verify(indexingJobRepository, never()).complete(any())
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any())
    }

    @Test
    fun `IndexingProcessor가 예외를 던지면 run은 예외를 삼키고 recordFailure를 호출한다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(documentRepository.findSearchableEmbeddingVersionNo(42L)).thenReturn(null)
        whenever(downloadClient.download("k")).thenReturn(bytes)

        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)

        val chunk = Chunk(
            chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2,
            pageFrom = null, pageTo = null, sectionPath = null, metadata = null,
        )
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.FIXED_TOKEN))).thenReturn(listOf(chunk))

        indexingProcessor.throwOnNextCall = RuntimeException("simulated failure")
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any()))
            .thenReturn(com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.RETRY_WAIT)

        // 예외가 밖으로 새어나가지 않아야 한다.
        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("RuntimeException"),
            eq("simulated failure"),
            eq(maxAttempts),
            // 선형 백오프 계산은 recordFailure가 담당한다 — 러너는 base delay만 그대로 넘긴다.
            eq(baseDelay),
            any(),
        )
        verify(indexingJobRepository, never()).complete(any())
    }

    @Test
    fun `콘텐츠 해시가 일치하지 않으면 HASH_MISMATCH로 recordFailure를 호출한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex("hello world".toByteArray()),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(documentRepository.findSearchableEmbeddingVersionNo(42L)).thenReturn(null)
        // 다운로드된 바이트가 documentVersion.contentHash와 일치하지 않도록 다른 내용을 반환한다.
        whenever(downloadClient.download("k")).thenReturn("corrupted content".toByteArray())
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)

        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("HASH_MISMATCH"),
            any(),
            eq(maxAttempts),
            eq(baseDelay),
            any(),
        )
        assertThat(indexingProcessor.calls).isEmpty()
        verify(indexingJobRepository, never()).complete(any())
    }

    // Fix 1 회귀 방지: 검증 실패가 Job 획득 "이후"에 일어나야 attempt_count가 올라가고,
    // recordFailure가 상한 도달 시 FAILED로 종결시켜 재시도 핫 루프를 끊을 수 있다.
    // 예전 구조(검증 → 획득)에서는 이 예외가 run() 밖으로 그대로 전파되고 start()에도
    // 도달하지 못해, RETRY_WAIT 상태의 Job이 매 폴링마다 영원히 다시 잡혔다.
    @Test
    fun `재시도 경로에서 검증이 실패해도 예외를 삼키고 recordFailure를 호출한다`() {
        stubActiveJobAcquisition(
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            ),
        )
        whenever(eventValidator.validate(any()))
            .thenThrow(InvalidEventException("DOCUMENT_VERSION_NOT_FOUND", "document_version 1001 does not exist"))
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)

        runner.run(sampleEvent())

        // attempt_count를 올리는 start()가 먼저 실행돼야 한다.
        verify(indexingJobRepository).start(5001L, "worker-test", maxAttempts)
        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("DOCUMENT_VERSION_NOT_FOUND"),
            eq("document_version 1001 does not exist"),
            eq(maxAttempts),
            eq(baseDelay),
            any(),
        )
        verify(downloadClient, never()).download(any())
        assertThat(indexingProcessor.calls).isEmpty()
    }

    @Test
    fun `테넌트가 일치하지 않으면 Job 획득 뒤 실패로 기록한다`() {
        stubActiveJobAcquisition(
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            ),
        )
        whenever(documentRepository.findById(42L))
            .thenReturn(Optional.of(DocumentEntity(id = 42L, tenantId = 999L, searchableVersionId = null, deletedAt = null)))
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        verify(indexingJobRepository).start(5001L, "worker-test", maxAttempts)
        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("TENANT_MISMATCH"),
            any(),
            eq(maxAttempts),
            eq(baseDelay),
            any(),
        )
        verify(downloadClient, never()).download(any())
    }

    // documentVersionId가 없는 이벤트는 indexing_job 행 자체를 만들 수 없다(NOT NULL FK).
    // 재시도 경로에서는 나올 수 없는 최초 수신 1회성 케이스라 조용히 버린다.
    @Test
    fun `documentVersionId가 없으면 Job을 만들지 않고 조용히 반환한다`() {
        runner.run(sampleEvent().copy(documentVersionId = null))

        verify(indexingJobRepository, never()).insertIfAbsent(any(), any(), any(), anyOrNull())
        verify(indexingJobRepository, never()).start(any(), any(), any())
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any())
        assertThat(indexingProcessor.calls).isEmpty()
    }

    @Test
    fun `청킹 전략은 설정값을 따른다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(documentRepository.findSearchableEmbeddingVersionNo(42L)).thenReturn(null)
        whenever(downloadClient.download("k")).thenReturn(bytes)

        val parser: DocumentParser = mock()
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parser.parse(any())).thenReturn(sequenceOf(block))
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)

        val chunk = Chunk(
            chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2,
            pageFrom = null, pageTo = null, sectionPath = null, metadata = null,
        )
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.PARAGRAPH))).thenReturn(listOf(chunk))

        newRunner(strategy = ChunkingStrategy.PARAGRAPH).run(sampleEvent())

        verify(chunkingService).chunk(listOf(block), ChunkingStrategy.PARAGRAPH)
        assertThat(indexingProcessor.calls).hasSize(1)
    }

    @Test
    fun `insertIfAbsent 후 findBySourceEventId가 null이면 다른 job이 활성 처리중이므로 조용히 반환한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            )
        whenever(eventValidator.validate(any())).thenReturn(documentVersion)
        whenever(documentRepository.findById(42L))
            .thenReturn(Optional.of(DocumentEntity(id = 42L, tenantId = 7L, searchableVersionId = 2001L, deletedAt = null)))
        whenever(indexingJobRepository.insertIfAbsent(any(), any(), any(), anyOrNull())).thenReturn(0)
        whenever(indexingJobRepository.findBySourceEventId(any())).thenReturn(null)

        runner.run(sampleEvent())

        verify(indexingJobRepository, never()).start(any(), any(), any())
        verify(downloadClient, never()).download(any())
        assertThat(indexingProcessor.calls).isEmpty()
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any())
    }

    @Test
    fun `start가 1을 반환하지 않으면 failIfAttemptsExceeded를 호출하고 반환한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(indexingJobRepository.start(5001L, "worker-test", maxAttempts)).thenReturn(0)

        runner.run(sampleEvent())

        verify(indexingJobRepository).failIfAttemptsExceeded(5001L, maxAttempts)
        verify(downloadClient, never()).download(any())
        assertThat(indexingProcessor.calls).isEmpty()
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any())
    }

    /** eventValidator/insertIfAbsent/findBySourceEventId/documentRepository.findById/start를 표준 성공 값으로 스텁한다. */
    private fun stubActiveJobAcquisition(documentVersion: DocumentVersionEntity) {
        whenever(eventValidator.validate(any())).thenReturn(documentVersion)
        whenever(indexingJobRepository.insertIfAbsent(any(), any(), any(), anyOrNull())).thenReturn(1)
        whenever(indexingJobRepository.findBySourceEventId(any()))
            .thenReturn(
                IndexingJobEntity(
                    id = 5001L, sourceEventId = UUID.randomUUID(), documentId = 42L, documentVersionId = 1001L,
                    status = com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.PENDING,
                    attemptCount = 0, nextRetryAt = null, workerId = null, lastErrorCode = null,
                    lastErrorMessage = null, traceId = null, startedAt = null, completedAt = null,
                    updatedAt = LocalDateTime.now(),
                ),
            )
        whenever(indexingJobRepository.start(5001L, "worker-test", maxAttempts)).thenReturn(1)
        whenever(documentRepository.findById(42L))
            .thenReturn(Optional.of(DocumentEntity(id = 42L, tenantId = 7L, searchableVersionId = 2001L, deletedAt = null)))
    }

    private fun sampleEvent() =
        IndexingRequestedEvent(
            eventId = UUID.randomUUID(), eventType = "INDEXING_REQUESTED", eventSchemaVersion = 1, tenantId = 7L,
            documentId = 42L, documentVersionId = 1001L, occurredAt = Instant.now(), traceId = null,
        )

    private fun sha256Hex(bytes: ByteArray): String =
        "sha256:" + MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    private fun <T> anyOrNull(): T? = org.mockito.kotlin.any()
}
