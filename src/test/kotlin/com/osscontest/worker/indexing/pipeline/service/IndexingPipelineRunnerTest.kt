package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.consumer.IndexingEventValidator
import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.consumer.InvalidEventException
import com.osscontest.worker.indexing.parsing.CorruptedFileException
import com.osscontest.worker.indexing.parsing.DocumentParser
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard
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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.nio.file.Files
import java.nio.file.Path
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
    private val parsingTimeoutGuard: ParsingTimeoutGuard = mock()
    private val chunkingService: ChunkingService = mock()
    private val chunkGuard: ChunkGuard = mock()
    private val indexingProcessor = FakeIndexingProcessor()
    private val indexingFailureService: IndexingFailureService = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val maxAttempts = 5
    private val baseDelay: Duration = Duration.ofSeconds(30)
    private val maxFileSizeBytes = 209_715_200L // 200MB — 스펙 기본값

    private val runner = newRunner()

    private fun newRunner(strategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN) =
        IndexingPipelineRunner(
            indexingJobRepository = indexingJobRepository,
            documentRepository = documentRepository,
            eventValidator = eventValidator,
            downloadClient = downloadClient,
            parserRegistry = parserRegistry,
            parsingTimeoutGuard = parsingTimeoutGuard,
            chunkingService = chunkingService,
            chunkGuard = chunkGuard,
            indexingProcessor = indexingProcessor,
            indexingFailureService = indexingFailureService,
            workerId = "worker-test",
            maxAttempts = maxAttempts,
            baseDelay = baseDelay,
            maxFileSizeBytes = maxFileSizeBytes,
            chunkingStrategy = strategy,
            meterRegistry = meterRegistry,
        )

    // 이전 버전으로 되돌리기 기능이 그 버전의 청크/임베딩이 실제로 저장돼 있어야 성립하므로,
    // embeddingVersionNo가 현재 searchable 버전보다 낮다고 해서 임베딩 자체를 건너뛰면 안 된다.
    // "최신 아닌 버전이 검색을 덮어쓰지 않는다"는 §1.4-(3)의 승격 UPDATE(IndexingProcessor 내부)가
    // 맡고, IndexingPipelineRunner는 그 판단 없이 항상 끝까지 처리한다.
    @Test
    fun `이미 더 최신 버전이 searchable이어도 임베딩까지 끝까지 처리한다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))

        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))

        val chunk = Chunk(
            chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2,
            pageFrom = null, pageTo = null, sectionPath = null, metadata = null,
        )
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.FIXED_TOKEN))).thenReturn(listOf(chunk))

        runner.run(sampleEvent())

        verify(downloadClient).download("k")
        assertThat(indexingProcessor.calls).hasSize(1)
        verify(indexingJobRepository, never()).complete(any())
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any(), any())
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
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))

        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))

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
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any(), any())

        val order = inOrder(indexingJobRepository)
        order.verify(indexingJobRepository).updatePhase(5001L, "DOWNLOADING")
        order.verify(indexingJobRepository).updatePhase(5001L, "PARSING")
        order.verify(indexingJobRepository).updatePhase(5001L, "CHUNKING")
        order.verify(indexingJobRepository).updatePhase(5001L, "EMBEDDING")
        verify(indexingJobRepository, times(4)).updatePhase(any(), any())
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
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))

        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))

        val chunk = Chunk(
            chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2,
            pageFrom = null, pageTo = null, sectionPath = null, metadata = null,
        )
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.FIXED_TOKEN))).thenReturn(listOf(chunk))

        indexingProcessor.throwOnNextCall = RuntimeException("simulated failure")
        // RuntimeException은 영구 실패 화이트리스트에 없으므로 permanent=false로 재시도 가능
        // 취급된다. 여기서는 recordFailure 인자 검증이 목적이라 FAILED로 스텁해 루프를
        // 한 번에 끝낸다(RETRY_WAIT을 스텁하면 findById가 안 되어 있어 두 번째 루프에서
        // NoSuchElementException이 나거나 start()가 두 번 불려 다른 검증이 깨진다).
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.FAILED)

        // 예외가 밖으로 새어나가지 않아야 한다.
        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("RuntimeException"),
            eq("simulated failure"),
            eq(false),
            eq(maxAttempts),
            // 선형 백오프 계산은 recordFailure가 담당한다 — 러너는 base delay만 그대로 넘긴다.
            eq(baseDelay),
            any(),
        )
        verify(indexingJobRepository).currentDbTimestamp()
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
        // 다운로드된 바이트가 documentVersion.contentHash와 일치하지 않도록 다른 내용을 반환한다.
        whenever(downloadClient.download("k")).thenReturn(tempFileOf("corrupted content".toByteArray()))
        // ContentIntegrityException은 영구 실패 화이트리스트에 있으므로 permanent=true.
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("HASH_MISMATCH"),
            any(),
            eq(true),
            eq(maxAttempts),
            eq(baseDelay),
            any(),
        )
        assertThat(indexingProcessor.calls).isEmpty()
        verify(indexingJobRepository, never()).complete(any())
    }

    @Test
    fun `파싱 중 손상된 파일이면 CORRUPTED_FILE로 recordFailure를 호출한다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))
        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain")))
            .thenThrow(CorruptedFileException("text/plain", java.io.IOException("bad file")))
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L), eq("CORRUPTED_FILE"), any(), eq(true), eq(maxAttempts), eq(baseDelay), any(),
        )
        assertThat(indexingProcessor.calls).isEmpty()
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
        // InvalidEventException은 영구 실패 화이트리스트에 있으므로 permanent=true.
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        // attempt_count를 올리는 start()가 먼저 실행돼야 한다.
        verify(indexingJobRepository).start(5001L, "worker-test", maxAttempts)
        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("DOCUMENT_VERSION_NOT_FOUND"),
            eq("document_version 1001 does not exist"),
            eq(true),
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
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        verify(indexingJobRepository).start(5001L, "worker-test", maxAttempts)
        verify(indexingFailureService).recordFailure(
            eq(5001L),
            eq("TENANT_MISMATCH"),
            any(),
            eq(true),
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
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any(), any())
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
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))

        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))

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
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any(), any())
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
        verify(indexingFailureService, never()).recordFailure(any(), any(), any(), any(), any(), any(), any())
    }

    @Test
    fun `문서 크기가 상한을 넘으면 다운로드 전에 FILE_TOO_LARGE로 종결한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L, fileSize = maxFileSizeBytes + 1,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(sampleEvent())

        verify(indexingFailureService).recordFailure(
            eq(5001L), eq("FILE_TOO_LARGE"), any(), eq(true), eq(maxAttempts), eq(baseDelay), any(),
        )
        verify(downloadClient, never()).download(any())
        assertThat(indexingProcessor.calls).isEmpty()
    }

    @Test
    fun `RETRY_WAIT으로 인라인 재시도할 때만 indexing_inline_retry_total이 attempt 태그로 증가한다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        val failedAt = LocalDateTime.of(2099, 1, 1, 0, 0)
        val nextRetryAt = failedAt.plusSeconds(30)
        val retryWaitJob =
            IndexingJobEntity(
                id = 5001L, sourceEventId = UUID.randomUUID(), documentId = 42L, documentVersionId = 1001L,
                status = IndexingJobStatus.RETRY_WAIT, attemptCount = 1, nextRetryAt = nextRetryAt,
                workerId = "worker-test", lastErrorCode = "RuntimeException", lastErrorMessage = "temporary",
                traceId = null, startedAt = failedAt, completedAt = null, updatedAt = failedAt,
            )
        whenever(indexingJobRepository.start(5001L, "worker-test", maxAttempts)).thenReturn(1, 0)
        whenever(indexingJobRepository.currentDbTimestamp()).thenReturn(failedAt, nextRetryAt)
        whenever(indexingJobRepository.findById(5001L)).thenReturn(Optional.of(retryWaitJob))
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        whenever(eventValidator.validate(any()))
            .thenThrow(RuntimeException("temporary"))

        // nextRetryAt은 실제 앱 시각보다 먼 미래지만 DB 기준 현재 시각과는 같다. 앱 시각을
        // 사용하면 장시간 sleep하므로, 제한시간 안에 끝나는지가 DB 시각 사용의 회귀 검증이다.
        assertTimeoutPreemptively(Duration.ofSeconds(1)) { runner.run(sampleEvent()) }

        assertThat(meterRegistry.get("indexing_inline_retry_total").tag("attempt", "1").counter().count())
            .isEqualTo(1.0)
        verify(indexingJobRepository, times(2)).currentDbTimestamp()
    }

    @Test
    fun `영구 실패로 종결하면 indexing_inline_retry_total이 증가하지 않는다`() {
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = "h",
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(indexingFailureService.recordFailure(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(IndexingJobStatus.FAILED)
        whenever(eventValidator.validate(any()))
            .thenThrow(InvalidEventException("DOCUMENT_VERSION_NOT_FOUND", "not found"))

        runner.run(sampleEvent())

        assertThat(meterRegistry.find("indexing_inline_retry_total").counters()).isEmpty()
    }

    @Test
    fun `성공하면 indexing_job_duration_seconds가 phase=total 태그로 기록된다`() {
        val bytes = "hello world".toByteArray()
        val documentVersion =
            DocumentVersionEntity(
                id = 1001L, documentId = 42L, versionNo = 1L,
                sourceObjectKey = "k", mimeType = "text/plain", contentHash = sha256Hex(bytes),
                embeddingVersionNo = 3L,
            )
        stubActiveJobAcquisition(documentVersion)
        whenever(downloadClient.download("k")).thenReturn(tempFileOf(bytes))
        val parser: DocumentParser = mock()
        whenever(parserRegistry.findParser("text/plain")).thenReturn(parser)
        val block = ParsedBlock(order = 0, type = BlockType.PARAGRAPH, text = "hello world", pageNo = null, headingPath = emptyList())
        whenever(parsingTimeoutGuard.parse(eq(parser), any(), eq("text/plain"))).thenReturn(listOf(block))
        val chunk = Chunk(chunkNo = 0, content = "hello world", contentHash = "ch", tokenCount = 2, pageFrom = null, pageTo = null, sectionPath = null, metadata = null)
        whenever(chunkingService.chunk(eq(listOf(block)), eq(ChunkingStrategy.FIXED_TOKEN))).thenReturn(listOf(chunk))

        runner.run(sampleEvent())

        assertThat(meterRegistry.get("indexing_job_duration_seconds").tag("phase", "total").timer().count())
            .isEqualTo(1L)
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
        // P2-2: recordFailure()가 DB 시각을 쓰도록 바뀌었으므로, unstub 상태(mock 기본값 null)로
        // 인해 recordFailure(...) 검증의 any() 매처가 null 인자와 불일치하지 않도록 스텁한다.
        whenever(indexingJobRepository.currentDbTimestamp()).thenReturn(LocalDateTime.now())
    }

    private fun sampleEvent() =
        IndexingRequestedEvent(
            eventId = UUID.randomUUID(), eventType = "INDEXING_REQUESTED", eventSchemaVersion = 1, tenantId = 7L,
            documentId = 42L, documentVersionId = 1001L, occurredAt = Instant.now(), traceId = null,
        )

    private fun sha256Hex(bytes: ByteArray): String =
        "sha256:" + MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    private fun tempFileOf(bytes: ByteArray): Path {
        val path = Files.createTempFile("test-download-", ".tmp")
        Files.write(path, bytes)
        return path
    }

    private fun <T> anyOrNull(): T? = org.mockito.kotlin.any()
}
