package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.*
import com.osscontest.worker.indexing.parsing.*
import com.osscontest.worker.indexing.publication.entity.*
import com.osscontest.worker.indexing.publication.repository.*
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import com.osscontest.worker.support.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.nio.file.Files
import java.util.Optional
import java.util.UUID

class IndexingPipelineRunnerTest {
    private val jobs: IndexingJobRepository = mock()
    private val documents: DocumentRepository = mock()
    private val versions: DocumentVersionRepository = mock()
    private val download: DocumentDownloadClient = mock()
    private val parsers: DocumentParserRegistry = mock()
    private val parse: ParsingTimeoutGuard = mock()
    private val chunker: ChunkingService = mock()
    private val processor = FakeIndexingProcessor()
    private val failures: IndexingFailureService = mock()
    private val publication: com.osscontest.worker.indexing.publication.service.IndexingPublicationService = mock()
    private val deletion: com.osscontest.worker.indexing.deletion.service.DocumentDeletionService = mock()
    private val execution = directExecution()
    private val store: JobQueueStore = mock()
    private val runner get() = createRunner(execution)
    private fun createRunner(operation: ExecutionOperations, parsing: ParsingTimeoutGuard = parse) = IndexingPipelineRunner(jobs, documents, versions, download, parsers, parsing,
        chunker, mock(), processor, failures, mock(), operation, deletion, 5, java.time.Duration.ofMillis(1), 10000,
        ChunkingStrategy.FIXED_TOKEN, SimpleMeterRegistry(), publication = publication)
    private val job = ClaimedJob(testToken, JobType.INDEX, UUID.randomUUID(), 2, 3, null)

    private fun setup() {
        whenever(store.snapshot(testToken)).thenReturn(JobSnapshot(testToken.workerId, testToken.epoch,
            IndexingJobStatus.PROCESSING, testNow.plusSeconds(60), null, testNow, testToken.epoch, type = JobType.INDEX, documentId = 2, documentVersionId = 3))
        whenever(documents.findById(2)).thenReturn(Optional.of(DocumentEntity(2, 1, null, null)))
        whenever(versions.findById(3)).thenReturn(Optional.of(DocumentVersionEntity(3, 2, 1, "key", "text/plain",
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", fileSize = 11)))
        whenever(jobs.updatePhase(any(), any(), any(), any())).thenReturn(1)
        whenever(parsers.findParser(any())).thenReturn(mock())
        whenever(parse.parse(any(), any(), any())).thenReturn(emptyList())
        whenever(chunker.chunk(any(), any())).thenReturn(listOf(Chunk(0, "hello world", "hash", 2, null, null, null, null)))
        whenever(download.download("key")).thenAnswer { Files.createTempFile("runner-test-", ".txt").also {
            Files.writeString(it, "hello world")
        } }
    }

    @Test
    fun `다운로드 직후 파일 크기 조회 실패도 임시 파일을 정리한다`() {
        setup()
        val path = Files.createTempFile("runner-size-", ".txt").also { Files.writeString(it, "hello world") }
        whenever(download.download("key")).thenReturn(path)
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        try {
            org.mockito.Mockito.mockStatic(Files::class.java, org.mockito.Mockito.CALLS_REAL_METHODS).use { files ->
                files.`when`<Long> { Files.size(path) }.thenThrow(java.io.IOException("size unavailable"))
                runner.run(job)
            }
            assertThat(Files.exists(path)).isFalse()
            verify(failures).recordFailure(eq(testToken), eq("IOException"), any(), eq(false), eq(5), any(), anyOrNull())
        } finally { Files.deleteIfExists(path) }
    }

    @Test
    fun `parser 포화는 일시 실패를 기록하고 파일과 실행을 반환한다`() {
        setup()
        whenever(parse.parse(any(), any(), any())).thenThrow(ParseCapacityException(IllegalStateException("full")))
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        runner.run(job)
        verify(failures).recordFailure(eq(testToken), eq("PARSE_CAPACITY"), any(), eq(false), eq(5), any(), anyOrNull())
        val source = argumentCaptor<com.osscontest.worker.indexing.retrieval.TemporaryDocumentFile>()
        verify(parse).parse(any(), source.capture(), any())
        assertThat(Files.exists(source.firstValue.path)).isFalse()
        assertThat(processor.calls).isEmpty()
    }

    @Test
    fun `runner 취소는 실패를 쓰지 않고 parser 실제 종료까지 파일을 유지한다`() {
        setup()
        val entered = java.util.concurrent.CountDownLatch(1); val finish = java.util.concurrent.CountDownLatch(1)
        val source = java.util.concurrent.atomic.AtomicReference<java.nio.file.Path>()
        whenever(download.download("key")).thenAnswer {
            Files.createTempFile("runner-cancel-", ".txt").also { Files.writeString(it, "hello world"); source.set(it) }
        }
        val parser: DocumentParser = mock()
        whenever(parsers.findParser(any())).thenReturn(parser)
        whenever(parser.parse(any())).thenReturn(sequence {
            entered.countDown()
            while (finish.count > 0) {
                try { finish.await() } catch (_: InterruptedException) { }
            }
        })
        val guard = ParsingTimeoutGuard(java.time.Duration.ofSeconds(10), 1, SimpleMeterRegistry())
        val activeRunner = createRunner(execution, guard)
        val thread = Thread { activeRunner.run(job) }
        thread.start()
        try {
            check(entered.await(3, java.util.concurrent.TimeUnit.SECONDS))
            thread.interrupt(); thread.join(3000)
            assertThat(thread.isAlive).isFalse()
            verifyNoInteractions(failures)
            assertThat(Files.exists(source.get())).isTrue()
            finish.countDown()
            val until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3)
            while (Files.exists(source.get())) { check(System.nanoTime() < until); Thread.sleep(1) }
            assertThat(processor.calls).isEmpty()
        } finally { finish.countDown(); thread.interrupt(); thread.join(3000); guard.shutdown() }
    }

    @Test
    fun `삭제 INDEX는 외부 호출 없이 preflight 완료로 종료한다`() {
        whenever(publication.preflight(job)).thenReturn(true)
        runner.run(job)
        verify(publication).preflight(job)
        verifyNoInteractions(download, parsers, parse, chunker, versions, failures, deletion)
        assertThat(processor.calls).isEmpty()
    }

    @Test
    fun `DELETE는 INDEX preflight를 거치지 않고 기존 삭제 처리기에 위임한다`() {
        val delete = job.copy(type = JobType.DELETE, documentVersionId = null)
        runner.run(delete)
        verify(deletion).delete(delete)
        verifyNoInteractions(publication, download, parsers, parse, chunker, versions, failures)
    }

    @Test
    fun `claim 실행 토큰을 그대로 전달하고 임시 파일을 정리한다`() {
        setup()
        runner.run(job)
        assertThat(processor.calls).hasSize(1)
        assertThat(processor.calls.single().first.executionToken).isEqualTo(testToken)
        verify(jobs).updatePhase(1, "DOWNLOADING", testToken.workerId, testToken.epoch)
        verify(jobs).updatePhase(1, "PARSING", testToken.workerId, testToken.epoch)
        verify(jobs).updatePhase(1, "CHUNKING", testToken.workerId, testToken.epoch)
        verify(jobs).updatePhase(1, "EMBEDDING", testToken.workerId, testToken.epoch)
        val path = argumentCaptor<com.osscontest.worker.indexing.retrieval.TemporaryDocumentFile>()
        verify(parse).parse(any(), path.capture(), any())
        assertThat(Files.exists(path.firstValue.path)).isFalse()
        verifyNoInteractions(failures)
    }

    @Test
    fun `일반 실패는 재시도를 기록하고 같은 실행에서 반복하지 않는다`() {
        setup()
        processor.failuresBeforeSuccess = 2
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        runner.run(job)
        assertThat(processor.calls).hasSize(1)
        verify(download).download("key")
        verify(failures).recordFailure(eq(testToken), eq("RuntimeException"), any(), eq(false), eq(5), any(), anyOrNull())
    }

    @Test
    fun `실행권을 잃으면 실패 상태를 쓰지 않고 종료한다`() {
        setup()
        processor.throwOnNextCall = ExecutionLostException(testToken)
        runner.run(job)
        verifyNoInteractions(failures)
        assertThat(processor.calls).hasSize(1)
    }

    @Test
    fun `내용 해시 불일치는 재시도 없이 종결한다`() {
        setup()
        whenever(download.download("key")).thenAnswer { Files.createTempFile("runner-test-", ".txt").also {
            Files.writeString(it, "wrong bytes")
        } }
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.FAILED)
        runner.run(job)
        verify(failures).recordFailure(eq(testToken), eq("HASH_MISMATCH"), any(), eq(true), eq(5), any(), anyOrNull())
        assertThat(processor.calls).isEmpty()
        verify(download).download("key")
    }

    @Test
    fun `Job 버전이 없으면 외부 호출 없이 영구 실패로 종결한다`() {
        setup()
        whenever(versions.findById(3)).thenReturn(Optional.empty())
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(job)

        verify(failures).recordFailure(eq(testToken), eq("DOCUMENT_VERSION_NOT_FOUND"), any(), eq(true), eq(5), any(), anyOrNull())
        verifyNoInteractions(download)
        assertThat(processor.calls).isEmpty()
    }

    @Test
    fun `Job과 버전의 문서가 다르면 외부 호출 없이 영구 실패로 종결한다`() {
        setup()
        val version = versions.findById(3).orElseThrow()
        version.documentId = 99
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.FAILED)

        runner.run(job)

        verify(failures).recordFailure(eq(testToken), eq("DOCUMENT_MISMATCH"), any(), eq(true), eq(5), any(), anyOrNull())
        verifyNoInteractions(download)
        assertThat(processor.calls).isEmpty()
    }

    @Test
    @org.junit.jupiter.api.Tag("database")
    fun `다운로드 이후 DB 장애는 파일을 유지하고 실패한 phase 기록부터 재개한다`() {
        setup()
        var downloaded: java.nio.file.Path? = null
        whenever(download.download("key")).thenAnswer {
            Files.createTempFile("db-resume-", ".txt").also {
                downloaded = it
                Files.writeString(it, "hello world")
            }
        }
        whenever(jobs.updatePhase(1, "PARSING", testToken.workerId, testToken.epoch))
            .thenThrow(org.springframework.dao.DataAccessResourceFailureException("DB down")).thenReturn(1)
        var waits = 0
        val waiter = RetryWaiter {
            assertThat(Files.exists(checkNotNull(downloaded))).isTrue()
            waits++
        }
        val db = DatabaseOperations(org.springframework.jdbc.core.JdbcTemplate(
            org.springframework.jdbc.datasource.DriverManagerDataSource("jdbc:h2:mem:phase-resume", "sa", "")),
            waiter, DatabaseWaitProperties(java.time.Duration.ofMillis(1), java.time.Duration.ofSeconds(1)), SimpleMeterRegistry())
        createRunner(ExecutionOperations(db, store)).run(job)
        assertThat(waits).isEqualTo(1)
        verify(download).download("key")
        assertThat(processor.calls).hasSize(1)
        verifyNoInteractions(failures)
        assertThat(Files.exists(checkNotNull(downloaded))).isFalse()
    }

    @Test
    fun `429는 실패 예약을 한 번 기록하고 전체 시도를 종료한다`() {
        setup()
        processor.throwOnNextCall = com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException(
            "120", RuntimeException("429"))
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        runner.run(job)
        verify(failures).recordFailure(eq(testToken), eq("RATE_LIMITED"), any(), eq(false), eq(5), any(), eq("120"))
        assertThat(processor.calls).hasSize(1)
        verify(download).download("key")
    }

}
