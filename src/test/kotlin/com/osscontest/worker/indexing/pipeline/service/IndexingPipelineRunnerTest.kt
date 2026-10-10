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
    private val execution = directExecution()
    private val store: JobQueueStore = mock()
    private val runner get() = createRunner(execution)
    private fun createRunner(operation: ExecutionOperations) = IndexingPipelineRunner(jobs, documents, versions, download, parsers, parse,
        chunker, mock(), processor, failures, mock(), operation, store, mock(), 5, java.time.Duration.ofMillis(1), 10000,
        ChunkingStrategy.FIXED_TOKEN, SimpleMeterRegistry())
    private val job = ClaimedJob(testToken, JobType.INDEX, UUID.randomUUID(), 2, 3, null)

    private fun setup() {
        whenever(store.snapshot(testToken)).thenReturn(JobSnapshot(testToken.workerId, testToken.epoch,
            IndexingJobStatus.PROCESSING, testNow.plusSeconds(60), null, testNow))
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
    fun `claim 실행 토큰을 그대로 전달하고 임시 파일을 정리한다`() {
        setup()
        runner.run(job)
        assertThat(processor.calls).hasSize(1)
        assertThat(processor.calls.single().first.executionToken).isEqualTo(testToken)
        verify(jobs).updatePhase(1, "DOWNLOADING", testToken.workerId, testToken.epoch)
        verify(jobs).updatePhase(1, "PARSING", testToken.workerId, testToken.epoch)
        verify(jobs).updatePhase(1, "CHUNKING", testToken.workerId, testToken.epoch)
        verify(jobs).updatePhase(1, "EMBEDDING", testToken.workerId, testToken.epoch)
        val path = argumentCaptor<java.nio.file.Path>()
        verify(parse).parse(any(), path.capture(), any())
        assertThat(Files.exists(path.firstValue)).isFalse()
        verifyNoInteractions(failures)
    }

    @Test
    fun `일반 처리 실패만 attempt 전체를 재시도한다`() {
        setup()
        processor.failuresBeforeSuccess = 2
        whenever(failures.recordFailure(any(), any(), any(), any(), any(), any(), anyOrNull()))
            .thenReturn(IndexingJobStatus.RETRY_WAIT)
        runner.run(job)
        assertThat(processor.calls).hasSize(3)
        verify(execution, times(2)).awaitRetry(testToken)
        verify(download, times(3)).download("key")
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
        createRunner(ExecutionOperations(db, store, waiter, DatabaseWaitProperties(), 5)).run(job)
        assertThat(waits).isEqualTo(1)
        verify(download).download("key")
        assertThat(processor.calls).hasSize(1)
        verifyNoInteractions(failures)
        assertThat(Files.exists(checkNotNull(downloaded))).isFalse()
    }

}
