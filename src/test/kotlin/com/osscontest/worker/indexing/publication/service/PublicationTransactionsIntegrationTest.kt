package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.RetryPolicy
import com.osscontest.worker.indexing.pipeline.service.RandomRetryJitter
import com.osscontest.worker.indexing.pipeline.service.RetryWaiter
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.publication.repository.*
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.support.CommitResponseLossDataSource
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import com.osscontest.worker.support.callSpiedMethod
import com.osscontest.worker.support.seedReleasedExecution
import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.embedding.service.EmbeddingIndexingProcessor
import com.osscontest.worker.indexing.embedding.service.NoriTokenizer
import com.osscontest.worker.indexing.embedding.usecase.EmbeddingUseCase
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.persistence.EntityManagerFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.*
import org.springframework.boot.convert.ApplicationConversionService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.transaction.support.TransactionSynchronizationManager
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Clock
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

/** Worker 전용 스키마에서 실제 JDBC Writer와 JPA 리포지토리를 한 트랜잭션 매니저로 검증한다. */
@Tag("integration")
@SpringJUnitConfig(PublicationTransactionsIntegrationTest.Config::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PublicationTransactionsIntegrationTest(
    private val queue: JobQueueStore,
    private val publication: IndexingPublicationService,
    private val failures: IndexingFailureService,
    private val jdbc: JdbcTemplate,
    private val transactionManager: PlatformTransactionManager,
    private val source: CommitResponseLossDataSource,
    private val emf: EntityManagerFactory,
    private val breaker: CircuitBreaker,
    private val deletion: DocumentDeletionService,
) {
    @MockitoSpyBean private lateinit var writer: DocumentChunkWriter
    @MockitoSpyBean private lateinit var chunks: DocumentChunkRepository
    @MockitoSpyBean private lateinit var versions: DocumentVersionRepository
    @MockitoSpyBean private lateinit var documents: DocumentRepository
    @MockitoSpyBean private lateinit var jobs: IndexingJobRepository
    @MockitoSpyBean private lateinit var state: PublicationStateReader
    @MockitoSpyBean private lateinit var store: JobQueueStore

    @Configuration
    @EnableTransactionManagement(proxyTargetClass = true)
    @EnableJpaRepositories(basePackageClasses = [IndexingJobRepository::class])
    @Import(JobExecutionLocks::class, JobOwnershipGuard::class, JobClaimTransactions::class, JobQueueStore::class,
        DocumentDeletionService::class,
        IndexingJobWritesImpl::class, PublicationStateReader::class, DocumentChunkWriter::class,
        IndexingPublicationService::class, IndexingFailureService::class, ExecutionResults::class,
        RetryPolicy::class, RandomRetryJitter::class)
    class Config {
        @Bean fun circuitBreaker() = com.osscontest.worker.support.closedCircuitBreaker()
        // Boot 없이 띄운 컨텍스트라 @Value의 "PT30S" 같은 Duration 문자열을 바꿀 변환기를 직접 등록한다.
        @Bean fun conversionService() = ApplicationConversionService.getSharedInstance()
        @Bean fun dataSource(): CommitResponseLossDataSource {
            val url = requireNotNull(System.getenv("INTEGRATION_DB_URL"))
            val user = requireNotNull(System.getenv("INTEGRATION_DB_USERNAME"))
            val password = System.getenv("INTEGRATION_DB_PASSWORD").orEmpty()
            val base = DriverManagerDataSource(url, user, password)
            assertDedicatedIntegrationDatabase(JdbcTemplate(base))
            // 이전 실행이 컨텍스트 생성 중 실패하면 스키마가 남으므로 먼저 지운다.
            JdbcTemplate(base).execute("DROP SCHEMA IF EXISTS worker_c3_publication CASCADE")
            JdbcTemplate(base).execute("CREATE SCHEMA worker_c3_publication")
            // pgvector는 전용 DB의 사전 조건이며 이 fixture가 만들지 않는다.
            return CommitResponseLossDataSource(DriverManagerDataSource(url +
                (if ('?' in url) "&" else "?") + "currentSchema=worker_c3_publication,public", user, password))
        }
        @Bean fun jdbc(dataSource: DataSource) = JdbcTemplate(dataSource)
        @Bean fun namedJdbc(jdbc: JdbcTemplate) = NamedParameterJdbcTemplate(jdbc)
        @Bean fun mapper() = JsonMapper.builder().build()
        @Bean fun meters() = SimpleMeterRegistry()
        @Bean fun properties() = QueueProperties(workerId = "publication")
        @Bean fun entityManagerFactory(dataSource: DataSource) = LocalContainerEntityManagerFactoryBean().apply {
            setDataSource(dataSource)
            setPackagesToScan("com.osscontest.worker.indexing.publication.entity")
            jpaVendorAdapter = HibernateJpaVendorAdapter()
            setJpaPropertyMap(mapOf("hibernate.hbm2ddl.auto" to "none", "hibernate.jdbc.time_zone" to "UTC"))
        }
        @Bean fun transactionManager(entityManagerFactory: EntityManagerFactory) = JpaTransactionManager(entityManagerFactory)
    }

    @BeforeAll fun schema() {
        jdbc.execute("""CREATE TABLE document (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL,
            execution_epoch BIGINT NOT NULL DEFAULT 0, searchable_version_id BIGINT,
            latest_embedding_version_no BIGINT, deleted_at TIMESTAMP, updated_at TIMESTAMPTZ)""")
        jdbc.execute("""CREATE TABLE document_version (id BIGINT PRIMARY KEY, document_id BIGINT NOT NULL REFERENCES document(id),
            version_no BIGINT NOT NULL, source_object_key TEXT NOT NULL DEFAULT 'key', mime_type TEXT NOT NULL DEFAULT 'text/plain',
            content_hash TEXT NOT NULL DEFAULT 'hash', file_size BIGINT NOT NULL DEFAULT 1,
            chunk_count INTEGER, extracted_metadata JSONB, indexed_at TIMESTAMP)""")
        jdbc.execute("""CREATE TABLE indexing_job (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_event_id UUID NOT NULL UNIQUE, document_id BIGINT NOT NULL REFERENCES document(id), document_version_id BIGINT,
            job_type TEXT NOT NULL, enqueue_seq BIGINT GENERATED BY DEFAULT AS IDENTITY UNIQUE, status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0, execution_epoch BIGINT NOT NULL DEFAULT 0, worker_id TEXT,
            lease_until TIMESTAMPTZ, next_retry_at TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL,
            started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, trace_id TEXT, phase TEXT, outcome TEXT,
            last_error_code TEXT, last_error_message TEXT)""")
        jdbc.execute("""CREATE TABLE document_chunk (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            tenant_id BIGINT NOT NULL, document_id BIGINT NOT NULL REFERENCES document(id),
            document_version_id BIGINT NOT NULL REFERENCES document_version(id), chunk_no INTEGER NOT NULL,
            content TEXT NOT NULL, content_tokens TEXT, content_hash TEXT NOT NULL, token_count INTEGER,
            page_from INTEGER, page_to INTEGER, section_path TEXT, metadata JSONB, embedding vector(1536) NOT NULL,
            embedded_at TIMESTAMP NOT NULL, updated_at TIMESTAMPTZ DEFAULT clock_timestamp(),
            UNIQUE(document_version_id, chunk_no))""")
    }
    @AfterAll fun dropSchema() { jdbc.execute("DROP SCHEMA worker_c3_publication CASCADE") }
    @BeforeEach fun fixture() {
        org.mockito.Mockito.reset(breaker)
        doReturn(ClaimAdmission.NORMAL).whenever(breaker).admission()
        com.osscontest.worker.support.allowCircuitCalls(breaker)
        source.loseNextCommit.set(false)
        jdbc.update("DELETE FROM document_chunk")
        jdbc.update("DELETE FROM indexing_job")
        jdbc.update("DELETE FROM document_version")
        jdbc.update("DELETE FROM document")
        jdbc.update("INSERT INTO document(id, tenant_id) VALUES (1,1),(2,1)")
        jdbc.update("INSERT INTO document_version(id,document_id,version_no) VALUES (1,1,1),(2,1,2),(3,2,3)")
    }
    private fun claim(version: Long = 1): ClaimedJob {
        queue.enqueue(UUID.randomUUID(), 1, version, JobType.INDEX, null)
        return checkNotNull(queue.claim(5, "instance-1"))
    }
    private fun context(job: ClaimedJob) = IndexingContext(job.token.jobId, 1, job.documentVersionId!!,
        job.documentVersionId, mapOf("language" to "ko"), job.token)
    private fun chunk(job: ClaimedJob, no: Int = 0) = DocumentChunk(job.documentVersionId!!, 1, no,
        "saved", "saved", "hash", 1, null, null, null, null, FloatArray(1536) { 0.1f }, LocalDateTime.now())
    private fun row(job: ClaimedJob) = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id=?", job.token.jobId)
    private fun execution() = ExecutionOperations(DatabaseOperations(jdbc, RetryWaiter {}, DatabaseWaitProperties(
        checkInterval = Duration.ofMillis(1)), SimpleMeterRegistry()), queue)
    private fun publish(job: ClaimedJob) = publication.publish(context(job), listOf(chunk(job)))
    private fun assertRolledBack(job: ClaimedJob, before: Map<String, Any?>) {
        assertThat(row(job)).isEqualTo(before)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
        assertThat(jdbc.queryForObject("SELECT indexed_at FROM document_version WHERE id=?",
            java.sql.Timestamp::class.java, job.documentVersionId)).isNull()
        assertThat(jdbc.queryForObject("SELECT searchable_version_id FROM document WHERE id=1", Long::class.java)).isNull()
    }

    @ParameterizedTest @ValueSource(strings = ["upsert", "trailing", "version", "promotion", "completion"])
    fun `각 저장 단계 중단은 청크 버전 검색 Job을 함께 롤백한다`(stage: String) {
        val job = claim(); val before = row(job)
        val allBefore = databaseState()
        val interrupt = { error("stop after $stage") }
        when (stage) {
            "upsert" -> doAnswer { it.callSpiedMethod(); interrupt() }.whenever(writer).upsertAll(any())
            "trailing" -> doAnswer { it.callSpiedMethod(); interrupt() }.whenever(chunks).deleteTrailingChunks(any(), any())
            "version" -> doAnswer { it.callSpiedMethod(); interrupt() }.whenever(versions).complete(any(), any(), any(), anyOrNull())
            "promotion" -> doAnswer { it.callSpiedMethod(); interrupt() }.whenever(documents).promoteSearchableVersion(any(), any())
            else -> doAnswer { it.callSpiedMethod(); interrupt() }.whenever(jobs).complete(any(), any(), any(), any())
        }
        assertThatThrownBy { publish(job) }.isInstanceOf(IllegalStateException::class.java)
        assertRolledBack(job, before)
        assertThat(databaseState()).isEqualTo(allBefore)
    }

    @Test fun `완료 직전 lease 만료는 이미 쓴 결과 전체를 롤백한다`() {
        val job = claim(); val before = row(job)
        val allBefore = databaseState()
        doAnswer {
            jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=?", job.token.jobId)
            it.callSpiedMethod()
        }.whenever(jobs).complete(any(), any(), any(), any())
        assertThatThrownBy { publish(job) }.isInstanceOf(LeaseExpiredException::class.java)
        assertRolledBack(job, before)
        assertThat(databaseState()).isEqualTo(allBefore)
    }

    @ParameterizedTest @ValueSource(longs = [1, 2])
    fun `구버전 동일 버전도 저장 완료하고 검색 포인터와 메타데이터를 보존한다`(searchable: Long) {
        jdbc.update("UPDATE document SET searchable_version_id=?,latest_embedding_version_no=? WHERE id=1", searchable, searchable)
        val job = claim(); publish(job)
        assertThat(row(job)["outcome"]).isEqualTo("INDEXED")
        assertThat(row(job)["worker_id"]).isNull()
        assertThat(row(job)["lease_until"]).isNull()
        assertThat(jdbc.queryForObject("SELECT searchable_version_id FROM document WHERE id=1", Long::class.java)).isEqualTo(searchable)
        assertThat(jdbc.queryForObject("SELECT latest_embedding_version_no FROM document WHERE id=1", Long::class.java)).isEqualTo(searchable)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isEqualTo(1)
        assertThat(TransactionTemplate(transactionManager).execute { documents.promoteSearchableVersion(1, 1) }).isZero()
    }

    @Test fun `신버전만 검색 버전을 승격하고 완료 시 순서 횟수 epoch를 유지한다`() {
        jdbc.update("UPDATE document SET searchable_version_id=1,latest_embedding_version_no=1 WHERE id=1")
        val job = claim(2); val before = row(job); publish(job)
        val after = row(job)
        assertThat(after["status"]).isEqualTo("COMPLETED")
        assertThat(after["outcome"]).isEqualTo("INDEXED")
        assertThat(after["worker_id"]).isNull()
        assertThat(after["lease_until"]).isNull()
        assertThat(jdbc.queryForObject("SELECT chunk_count FROM document_version WHERE id=2", Int::class.java)).isEqualTo(1)
        assertThat(jdbc.queryForObject("SELECT indexed_at FROM document_version WHERE id=2", java.sql.Timestamp::class.java)).isNotNull()
        assertThat(jdbc.queryForObject("SELECT extracted_metadata->>'language' FROM document_version WHERE id=2", String::class.java)).isEqualTo("ko")
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk WHERE document_version_id=2", Int::class.java)).isEqualTo(1)
        assertThat(jdbc.queryForObject("SELECT searchable_version_id FROM document WHERE id=1", Long::class.java)).isEqualTo(2)
        for (column in listOf("enqueue_seq", "execution_epoch", "attempt_count", "source_event_id")) {
            assertThat(after[column]).isEqualTo(before[column])
        }
    }

    @ParameterizedTest @ValueSource(longs = [3, 999])
    fun `다른 문서 또는 끊어진 검색 포인터는 정상 미승격으로 처리하지 않는다`(searchable: Long) {
        val job = claim()
        jdbc.update("UPDATE document SET searchable_version_id=? WHERE id=1", searchable)
        assertThatThrownBy { publish(job) }.isInstanceOf(RuntimeException::class.java)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
        assertThat(row(job)["status"]).isEqualTo("PROCESSING")
    }

    @Test fun `preflight와 finalize는 삭제 INDEX를 결과 없이 완료한다`() {
        val job = claim()
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
        assertThat(publication.preflight(job)).isTrue()
        assertThat(row(job)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
        assertThat(row(job)["worker_id"]).isNull()
        assertThat(row(job)["lease_until"]).isNull()
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
        publish(job) // 같은 결과는 쓰기 없이 완료로 확인
        verify(writer, never()).upsertAll(any())
    }

    @Test fun `활성 preflight 뒤 삭제된 문서는 계산 결과를 finalize에서 반영하지 않는다`() {
        val job = claim()
        assertThat(publication.preflight(job)).isFalse()
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
        publish(job)
        assertThat(row(job)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
        verify(writer, never()).upsertAll(any())
        verify(versions, never()).complete(any(), any(), any(), anyOrNull())
    }

    @Test fun `완료된 늦은 실행 재호출은 다음 버전의 결과를 덮어쓰지 않는다`() {
        val old = claim(); publish(old)
        val next = claim(2); publish(next)
        publication.publish(context(old), listOf(chunk(old).copy(content = "late")))
        assertThat(jdbc.queryForObject("SELECT searchable_version_id FROM document WHERE id=1", Long::class.java)).isEqualTo(2)
        assertThat(jdbc.queryForObject("SELECT content FROM document_chunk WHERE document_version_id=2", String::class.java)).isEqualTo("saved")
        verify(writer, times(2)).upsertAll(any())
    }

    @Test fun `미커밋 DB 오류는 전체 롤백 뒤 동일 결과 쓰기만 재시도한다`() {
        val job = claim()
        var calls = 0
        doAnswer {
            it.callSpiedMethod()
            if (++calls == 1) throw org.springframework.dao.DataAccessResourceFailureException("DB connection lost before commit")
            1
        }.whenever(versions).complete(any(), any(), any(), anyOrNull())
        val retained = listOf(chunk(job))
        execution().completion(job.token, ExecutionIdentity(job), { IndexingJobStatus.COMPLETED }) {
            publication.publish(context(job), retained)
        }
        verify(writer, times(2)).upsertAll(same(retained))
        assertThat(row(job)["outcome"]).isEqualTo("INDEXED")
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isEqualTo(1)
    }

    @Test fun `커밋 응답 유실 후 Primary 완료 확인은 publish를 반복하지 않는다`() {
        val job = claim()
        source.loseNextCommit.set(true)
        assertThat(execution().completion(job.token, ExecutionIdentity(job), { IndexingJobStatus.COMPLETED }) { publish(job) })
            .isEqualTo(IndexingJobStatus.COMPLETED)
        val committed = row(job)
        jdbc.update("UPDATE document SET execution_epoch=execution_epoch+1 WHERE id=1")
        execution().completion(job.token, ExecutionIdentity(job), { IndexingJobStatus.COMPLETED }) { error("must not replay") }
        assertThat(row(job)).isEqualTo(committed)
        verify(writer, times(1)).upsertAll(any())
        verify(jobs, times(1)).complete(any(), any(), any(), any())
    }

    @Test fun `실패 커밋 응답 유실은 최초 deadline과 오류를 확인하고 재기록하지 않는다`() {
        val job = claim()
        source.loseNextCommit.set(true)
        val result = execution().failure(job.token) {
            failures.recordFailure(job.token, "TEMP", "temporary", false, 5, Duration.ofSeconds(30), "120")
        }
        assertThat(result).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        val committed = row(job)
        assertThat(committed["outcome"]).isEqualTo("RETRY_SCHEDULED")
        failures.recordFailure(job.token, "OTHER", "changed", true, 5, Duration.ofSeconds(1))
        assertThat(row(job)).isEqualTo(committed)
        verify(jobs, times(1)).fail(any(), any(), any(), any(), anyOrNull(), any(), any())
    }

    @Test fun `잠금 전 캐시 조회 후 대기 중 삭제 커밋도 fresh finalize에서 확인한다`() {
        val job = claim()
        val deleted = CountDownLatch(1); val commitDeletion = CountDownLatch(1); val cached = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val deletion = pool.submit {
                TransactionTemplate(transactionManager).execute {
                    jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
                    deleted.countDown(); check(commitDeletion.await(5, TimeUnit.SECONDS))
                }
            }
            check(deleted.await(5, TimeUnit.SECONDS))
            val result = pool.submit<IndexingJobStatus> {
                TransactionTemplate(transactionManager).execute {
                    val em = org.springframework.orm.jpa.EntityManagerFactoryUtils.getTransactionalEntityManager(emf)!!
                    assertThat(em.find(DocumentEntity::class.java, 1L).deletedAt).isNull()
                    cached.countDown()
                    publish(job)
                }
            }
            check(cached.await(5, TimeUnit.SECONDS)); waitForLock(); commitDeletion.countDown()
            deletion.get(5, TimeUnit.SECONDS)
            assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(IndexingJobStatus.COMPLETED)
            assertThat(row(job)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
            assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
        } finally { commitDeletion.countDown(); pool.shutdownNow() }
    }

    @Test fun `finalize가 먼저 문서를 잠그면 완료 커밋 이후 삭제가 진행된다`() {
        val job = claim(); val locked = CountDownLatch(1); val finish = CountDownLatch(1)
        doAnswer { locked.countDown(); check(finish.await(5, TimeUnit.SECONDS)); it.callSpiedMethod() }
            .whenever(state).document(1)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val result = pool.submit<IndexingJobStatus> { publish(job) }
            check(locked.await(5, TimeUnit.SECONDS))
            val deletion = pool.submit { jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1") }
            waitForLock(); assertThat(deletion.isDone).isFalse(); finish.countDown()
            assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(IndexingJobStatus.COMPLETED)
            deletion.get(5, TimeUnit.SECONDS)
            assertThat(row(job)["outcome"]).isEqualTo("INDEXED")
        } finally { finish.countDown(); pool.shutdownNow() }
    }

    @ParameterizedTest
    @ValueSource(strings = ["worker", "request-epoch", "document-epoch", "no-lease", "PENDING", "RETRY_WAIT", "FAILED", "COMPLETED"])
    fun `실패 서비스와 실제 finalize 가드 거절은 문서 버전 Job 결과 데이터를 모두 보존한다`(condition: String) {
        val job = claim()
        var token = job.token
        when (condition) {
            "worker" -> token = token.copy(workerId = "other")
            "request-epoch" -> token = token.copy(epoch = token.epoch + 1)
            "document-epoch" -> jdbc.update("UPDATE document SET execution_epoch=execution_epoch+1 WHERE id=1")
            "no-lease" -> jdbc.update("UPDATE indexing_job SET lease_until=NULL WHERE id=?", token.jobId)
            else -> jdbc.update("UPDATE indexing_job SET status=? WHERE id=?", condition, token.jobId)
        }
        val before = databaseState()
        assertThatThrownBy { failures.recordFailure(token, "ERROR", "test", false, 5, Duration.ofSeconds(30)) }
            .isInstanceOf(ExecutionLostException::class.java)
        assertThatThrownBy { publication.publish(context(job.copy(token = token)), listOf(chunk(job))) }
            .isInstanceOf(ExecutionLostException::class.java)
        assertThat(databaseState()).isEqualTo(before)
        verify(writer, never()).upsertAll(any())
    }

    @ParameterizedTest @ValueSource(strings = ["RETRY_WAIT", "FAILED"])
    fun `커밋된 외부 회수 fixture는 재claim 전 이전 Worker의 모든 변경을 차단한다`(released: String) {
        val job = claim()
        val status = IndexingJobStatus.valueOf(released)
        seedReleasedExecution(jdbc, job.token, status,
            if (status == IndexingJobStatus.RETRY_WAIT) queue.dbNow().plusSeconds(60) else null)
        val before = databaseState()
        assertThat(queue.renew(job.token)).isZero()
        assertThat(jobs.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)).isZero()
        assertThat(jobs.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isZero()
        assertThatThrownBy { failures.recordFailure(job.token, "ERROR", "test", false, 5, Duration.ofSeconds(30)) }
            .isInstanceOf(ExecutionLostException::class.java)
        assertThatThrownBy { publish(job) }.isInstanceOf(ExecutionLostException::class.java)
        assertThat(databaseState()).isEqualTo(before)
    }

    @Test fun `다른 Worker 재claim 뒤 이전 실패와 finalize는 새 실행 lease와 결과를 덮어쓰지 못한다`() {
        val old = claim()
        seedReleasedExecution(jdbc, old.token, IndexingJobStatus.RETRY_WAIT, queue.dbNow().minusSeconds(1))
        val next = checkNotNull(queue.claim(5, "instance-2"))
        assertThat(next.token.jobId).isEqualTo(old.token.jobId)
        assertThat(next.token.epoch).isEqualTo(old.token.epoch + 1)
        TransactionTemplate(transactionManager).execute { writer.upsertAll(listOf(chunk(next).copy(content = "new-result"))) }
        val before = databaseState()
        assertThatThrownBy { failures.recordFailure(old.token, "LATE", "old failure", true, 5, Duration.ofSeconds(30)) }
            .isInstanceOf(ExecutionLostException::class.java)
        assertThatThrownBy { publish(old) }.isInstanceOf(ExecutionLostException::class.java)
        assertThat(databaseState()).isEqualTo(before)
        assertThat(row(next)["status"]).isEqualTo("PROCESSING")
        assertThat(row(next)["worker_id"]).isEqualTo("instance-2")
    }

    @ParameterizedTest @ValueSource(booleans = [false, true])
    fun `실패 UPDATE 직후 중단은 오류 상태 deadline owner lease 해제를 함께 롤백한다`(permanent: Boolean) {
        val job = claim()
        val before = databaseState()
        doAnswer { it.callSpiedMethod(); error("stop after failure UPDATE before service commit") }
            .whenever(jobs).fail(any(), any(), any(), any(), anyOrNull(), any(), any())
        assertThatThrownBy { failures.recordFailure(job.token, "ERROR", "test", permanent, 5, Duration.ofSeconds(30)) }
            .isInstanceOf(IllegalStateException::class.java)
        assertThat(databaseState()).isEqualTo(before)
    }

    @ParameterizedTest @ValueSource(strings = ["phase", "failure", "finalize"])
    fun `일반 변경의 상위 흐름은 만료 동일 실행을 갱신한 뒤 같은 attempt와 epoch로 진행한다`(operation: String) {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=?", job.token.jobId)
        val before = row(job)
        val execution = execution()
        when (operation) {
            "phase" -> execution.database(job.token) {
                if (jobs.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch) == 0) throw LeaseExpiredException()
            }
            "failure" -> execution.failure(job.token) {
                failures.recordFailure(job.token, "TEMP", "test", false, 5, Duration.ofSeconds(30))
            }
            else -> execution.completion(job.token, ExecutionIdentity(job), { IndexingJobStatus.COMPLETED }) { publish(job) }
        }
        verify(store, times(1)).renew(job.token)
        val after = row(job)
        for (column in listOf("source_event_id", "enqueue_seq", "attempt_count", "execution_epoch")) {
            assertThat(after[column]).isEqualTo(before[column])
        }
        assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isEqualTo(job.token.epoch)
        when (operation) {
            "phase" -> {
                assertThat(after["phase"]).isEqualTo("PARSING")
                assertThat(queue.snapshot(job.token)!!.leaseUntil).isAfter(queue.dbNow())
            }
            "failure" -> assertThat(after["status"]).isEqualTo("RETRY_WAIT")
            else -> assertThat(after["outcome"]).isEqualTo("INDEXED")
        }
    }

    @ParameterizedTest @ValueSource(strings = ["failure", "finalize"])
    fun `서비스도 문서 잠금 대기 중 Job을 먼저 잠그지 않고 대기 중 만료 뒤 무변경으로 거절한다`(operation: String) {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()+INTERVAL '2 seconds' WHERE id=?", job.token.jobId)
        val before = databaseState()
        val held = CountDownLatch(1); val release = CountDownLatch(1); val entered = CountDownLatch(1)
        val pid = AtomicInteger()
        val pool = Executors.newFixedThreadPool(2)
        try {
            val holder = pool.submit {
                TransactionTemplate(transactionManager).execute {
                    jdbc.queryForList("SELECT id FROM document WHERE id=1 FOR UPDATE")
                    held.countDown(); check(release.await(10, TimeUnit.SECONDS))
                }
            }
            check(held.await(5, TimeUnit.SECONDS))
            val pending = pool.submit {
                assertThatThrownBy {
                    TransactionTemplate(transactionManager).execute {
                        pid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Int::class.java)!!)
                        assertThat(queue.snapshot(job.token)!!.canWrite(job.token)).isTrue()
                        entered.countDown()
                        if (operation == "failure") failures.recordFailure(job.token, "ERROR", "test", false, 5, Duration.ofSeconds(30))
                        else publish(job)
                    }
                }.isInstanceOf(LeaseExpiredException::class.java)
            }
            check(entered.await(5, TimeUnit.SECONDS))
            waitUntil { jdbc.queryForObject("SELECT cardinality(pg_blocking_pids(?)) > 0", Boolean::class.java, pid.get()) == true }
            TransactionTemplate(transactionManager).execute {
                jdbc.queryForList("SELECT id FROM indexing_job WHERE id=${job.token.jobId} FOR UPDATE NOWAIT")
            }
            waitUntil { jdbc.queryForObject("SELECT lease_until <= clock_timestamp() FROM indexing_job WHERE id=?", Boolean::class.java, job.token.jobId) == true }
            release.countDown(); holder.get(5, TimeUnit.SECONDS); pending.get(5, TimeUnit.SECONDS)
            assertThat(databaseState()).isEqualTo(before)
        } finally { release.countDown(); pool.shutdownNow() }
    }

    @ParameterizedTest @ValueSource(strings = ["PENDING", "PROCESSING", "RETRY_WAIT", "COMPLETED", "FAILED"])
    fun `OPEN에서 제외된 활성 INDEX도 모든 유형 후속 Job을 미종결 동안 막는다`(status: String) {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val id = checkNotNull(jdbc.queryForObject("SELECT id FROM indexing_job WHERE document_id=1", Long::class.java))
        when (status) {
            "PROCESSING" -> checkNotNull(queue.claim(5, "previous-1"))
            "RETRY_WAIT" -> jdbc.update("UPDATE indexing_job SET status='RETRY_WAIT', " +
                "next_retry_at=clock_timestamp()+INTERVAL '1 day' WHERE id=?", id)
            "COMPLETED", "FAILED" -> {
                val previous = checkNotNull(queue.claim(5, "previous-1"))
                if (status == "COMPLETED") check(jobs.complete(id, previous.token.workerId, previous.token.epoch) == 1)
                else failures.recordFailure(previous.token, "PERMANENT", "test", true, 5, Duration.ofSeconds(30))
            }
        }
        queue.enqueue(UUID.randomUUID(), 1, null, JobType.DELETE, null)
        queue.enqueue(UUID.randomUUID(), 1, 2, JobType.INDEX, null)
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=2")
        queue.enqueue(UUID.randomUUID(), 2, null, JobType.DELETE, null)
        doReturn(ClaimAdmission.BLOCKED).whenever(breaker).admission()
        val before = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id=?", id)
        val epoch = jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)
        val first = checkNotNull(queue.claim(5, "open-1"))
        assertThat(first.type).isEqualTo(JobType.DELETE)
        if (status in listOf("COMPLETED", "FAILED")) {
            assertThat(first.documentId).isEqualTo(1)
            assertThat(queue.claim(5, "open-2")!!.documentId).isEqualTo(2)
        } else {
            assertThat(first.documentId).isEqualTo(2)
            assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isEqualTo(epoch)
        }
        assertThat(queue.claim(5, "open-3")).isNull()
        assertThat(jdbc.queryForMap("SELECT * FROM indexing_job WHERE id=?", id)).isEqualTo(before)
        verify(breaker, never()).acquire(any(), any())
        verify(breaker, never()).record(any(), any())
    }

    @Test fun `claim 뒤 OPEN 호출 차단은 같은 실행에서 기다리며 실패 기록이나 attempt 증가 없이 완료한다`() {
        val job = claim()
        doReturn(ClaimAdmission.BLOCKED).whenever(breaker).admission()
        doReturn(CallAdmission.Blocked(Duration.ofSeconds(30)), CallAdmission.Allowed(CircuitPermit("embedding-api", 0, "test")))
            .whenever(breaker).acquire(any(), any())
        val pipeline = PipelineFixture()
        pipeline.runner.run(job)
        assertThat(row(job)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job)["attempt_count"]).isEqualTo(job.attemptCount)
        assertThat(row(job)["execution_epoch"]).isEqualTo(job.token.epoch)
        assertThat(row(job)["last_error_code"]).isNull()
        verify(pipeline.embedding, times(1)).embed(any())
        verify(breaker, never()).record(any(), argThat { outcome != CircuitOutcome.SUCCESS })
        verify(breaker, times(1)).record(any(), argThat { outcome == CircuitOutcome.SUCCESS })
    }

    @ParameterizedTest @ValueSource(booleans = [false, true])
    fun `서킷 상태와 무관하게 삭제된 RETRY_WAIT의 시각과 후속 순서를 지킨다`(open: Boolean) {
        val job = claim()
        failures.recordFailure(job.token, "TEMP", "test", false, 5, Duration.ofDays(1))
        queue.enqueue(UUID.randomUUID(), 1, null, JobType.DELETE, null)
        queue.enqueue(UUID.randomUUID(), 1, 2, JobType.INDEX, null)
        queue.enqueue(UUID.randomUUID(), 2, null, JobType.DELETE, null)
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id IN (1,2)")
        if (open) doReturn(ClaimAdmission.BLOCKED).whenever(breaker).admission()
        val before = row(job)
        assertThat(queue.claim(5, "other-1")!!.documentId).isEqualTo(2)
        assertThat(queue.claim(5, "other-2")).isNull()
        assertThat(row(job)).isEqualTo(before)
        assertThat(jdbc.queryForList("SELECT status FROM indexing_job WHERE document_id=1 AND id<>?", String::class.java,
            job.token.jobId)).containsExactly("PENDING", "PENDING")
        jdbc.update("UPDATE indexing_job SET next_retry_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", job.token.jobId)
        val retry = checkNotNull(queue.claim(5, "other-3"))
        assertThat(retry.token.jobId).isEqualTo(job.token.jobId)
        assertThat(retry.sourceEventId).isEqualTo(job.sourceEventId)
        assertThat(retry.attemptCount).isEqualTo(job.attemptCount + 1)
        assertThat(retry.token.epoch).isEqualTo(job.token.epoch + 1)
        assertThat(row(retry)["enqueue_seq"]).isEqualTo(before["enqueue_seq"])
        assertThat(publication.preflight(retry)).isTrue()
        assertThat(row(retry)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
        val delete = checkNotNull(queue.claim(5, "other-4"))
        assertThat(delete.type).isEqualTo(JobType.DELETE)
        assertThat(queue.claim(5, "other-5")).isNull()
        assertThat(jobs.complete(delete.token.jobId, delete.token.workerId, delete.token.epoch, JobOutcome.DELETED)).isEqualTo(1)
        assertThat(queue.claim(5, "other-6")!!.documentVersionId).isEqualTo(2)
        verify(breaker, never()).acquire(any(), any())
        verify(breaker, never()).record(any(), any())
    }

    @ParameterizedTest @ValueSource(booleans = [false, true])
    fun `OPEN에서도 삭제 INDEX를 claim하고 실제 runner가 외부 호출 없이 skip 완료한다`(open: Boolean) {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
        if (open) doReturn(ClaimAdmission.BLOCKED).whenever(breaker).admission()
        val job = checkNotNull(queue.claim(5, "deleted-index-1"))
        val pipeline = PipelineFixture()
        pipeline.runner.run(job)
        assertThat(row(job)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
        assertThat(row(job)["worker_id"]).isNull()
        assertThat(row(job)["lease_until"]).isNull()
        verifyNoInteractions(pipeline.download, pipeline.parsing, pipeline.embedding)
        verify(breaker, never()).acquire(any(), any())
        verify(breaker, never()).record(any(), any())
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
    }

    @ParameterizedTest @ValueSource(booleans = [false, true])
    fun `OPEN에서도 비활성 문서 DELETE runner는 처리기 성공을 DELETED 완료로 연결한다`(open: Boolean) {
        if (open) doReturn(ClaimAdmission.BLOCKED).whenever(breaker).admission()
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
        queue.enqueue(UUID.randomUUID(), 1, null, JobType.DELETE, null)
        val job = checkNotNull(queue.claim(5, "delete-1"))
        val handler: DocumentDeletionService = mock()
        // handler의 정상 완료 계약만 흉내 내고 내부 정리는 실행하지 않는다.
        doAnswer { check(jobs.complete(job.token.jobId, job.token.workerId, job.token.epoch) == 1); null }
            .whenever(handler).delete(job)
        val pipeline = PipelineFixture(deletion = handler)
        pipeline.runner.run(job)
        verify(handler, times(1)).delete(job)
        verifyNoInteractions(pipeline.download, pipeline.parsing, pipeline.embedding)
        verify(breaker, never()).acquire(any(), any())
        verify(breaker, never()).record(any(), any())
        verify(versions, never()).findById(any())
        assertThat(row(job)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job)["outcome"]).isEqualTo("DELETED")
        assertThat(row(job)["worker_id"]).isNull()
        assertThat(row(job)["lease_until"]).isNull()
    }

    @Test fun `임베딩 응답을 보류한 동안 커밋된 삭제는 응답 재개 뒤 결과 없이 skip 완료한다`() {
        val job = claim()
        val pipeline = PipelineFixture()
        val entered = CountDownLatch(1); val resume = CountDownLatch(1)
        whenever(pipeline.embedding.embed(any())).thenAnswer {
            entered.countDown(); check(resume.await(10, TimeUnit.SECONDS)); listOf(FloatArray(1536) { 0.1f })
        }
        val pool = Executors.newSingleThreadExecutor()
        try {
            val running = pool.submit { pipeline.runner.run(job) }
            check(entered.await(5, TimeUnit.SECONDS))
            TransactionTemplate(transactionManager).execute { jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1") }
            resume.countDown(); running.get(5, TimeUnit.SECONDS)
            assertThat(row(job)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
            assertThat(row(job)["worker_id"]).isNull()
            assertThat(row(job)["lease_until"]).isNull()
            assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
            verify(versions, never()).complete(any(), any(), any(), anyOrNull())
            verify(pipeline.embedding, times(1)).embed(any())
            assertThat(Files.exists(pipeline.path.get())).isFalse()
        } finally { resume.countDown(); pool.shutdownNow() }
    }

    @ParameterizedTest @ValueSource(strings = ["download", "parsing", "embedding"])
    fun `실제 pipeline의 지연 단계는 DB 자원과 행 잠금을 놓으며 별도 heartbeat가 갱신한다`(stage: String) {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val pipeline = PipelineFixture()
        val entered = CountDownLatch(1); val resume = CountDownLatch(1)
        val token = AtomicReference<ExecutionToken>()
        doAnswer { token.set(it.getArgument(0)); it.callSpiedMethod() }.whenever(store).snapshot(any())
        fun paused() {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse()
            entered.countDown(); check(resume.await(10, TimeUnit.SECONDS))
        }
        when (stage) {
            "download" -> whenever(pipeline.download.download(any())).thenAnswer { paused(); pipeline.temporaryFile() }
            "parsing" -> whenever(pipeline.parsing.parse(any(), any(), any())).thenAnswer { paused(); emptyList<com.osscontest.worker.indexing.parsing.domain.ParsedBlock>() }
            else -> whenever(pipeline.embedding.embed(any())).thenAnswer { paused(); listOf(FloatArray(1536) { 0.1f }) }
        }
        val worker = DbJobWorker(QueueProperties(concurrency = 1, pollInterval = Duration.ofDays(1)),
            pipeline.database, queue, pipeline.runner, ThreadSleepQueueLoopWaiter(), 5, ExecutionResults())
        worker.start()
        try {
            check(entered.await(5, TimeUnit.SECONDS))
            assertThat(source.openConnections.get()).isZero()
            val owned = token.get()
            val before = checkNotNull(queue.snapshot(owned))
            TransactionTemplate(transactionManager).execute {
                jdbc.queryForList("SELECT id FROM document WHERE id=1 FOR UPDATE NOWAIT")
                jdbc.queryForList("SELECT id FROM indexing_job WHERE id=${owned.jobId} FOR UPDATE NOWAIT")
            }
            worker.heartbeat().forEach { it.get(5, TimeUnit.SECONDS) }
            val renewed = checkNotNull(queue.snapshot(owned))
            assertThat(renewed.leaseUntil).isAfter(before.leaseUntil)
            assertThat(renewed.epoch).isEqualTo(before.epoch)
            assertThat(renewed.documentEpoch).isEqualTo(before.documentEpoch)
            assertThat(jdbc.queryForObject("SELECT attempt_count FROM indexing_job WHERE id=?", Int::class.java, owned.jobId)).isEqualTo(1)
            resume.countDown()
            waitUntil { queue.snapshot(owned)?.completed(owned) == true && pipeline.path.get()?.let { !Files.exists(it) } == true }
        } finally { worker.stop(); resume.countDown() }
    }

    @ParameterizedTest @ValueSource(booleans = [false, true])
    fun `finalize DB 장애 대기는 커넥션을 반환하고 복구 후 기존 결과 재사용 또는 회수 종료를 선택한다`(released: Boolean) {
        val job = claim()
        val waiting = CountDownLatch(1); val recover = CountDownLatch(1)
        val calls = AtomicInteger()
        doAnswer {
            it.callSpiedMethod()
            // 실제 연결 장애처럼 runtime 예외로 감싼다. checked SQLException을 그대로 던지면 @Transactional이 롤백하지 않고 커밋한다.
            if (calls.incrementAndGet() == 1) throw org.springframework.dao.DataAccessResourceFailureException(
                "injected DB fault before commit", java.sql.SQLException("injected DB fault before commit", "08006"))
            1
        }.whenever(versions).complete(any(), any(), any(), anyOrNull())
        val database = DatabaseOperations(jdbc, RetryWaiter {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse()
            assertThat(source.openConnections.get()).isZero()
            waiting.countDown(); check(recover.await(10, TimeUnit.SECONDS))
        }, DatabaseWaitProperties(checkInterval = Duration.ofMillis(1)), SimpleMeterRegistry())
        val pipeline = PipelineFixture(database)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val running = pool.submit { pipeline.runner.run(job) }
            check(waiting.await(5, TimeUnit.SECONDS))
            assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk", Int::class.java)).isZero()
            if (released) seedReleasedExecution(jdbc, job.token, IndexingJobStatus.RETRY_WAIT, queue.dbNow().plusSeconds(60))
            else jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=?", job.token.jobId)
            val recoveryState = databaseState()
            recover.countDown(); running.get(5, TimeUnit.SECONDS)
            verify(pipeline.download, times(1)).download(any())
            verify(pipeline.embedding, times(1)).embed(any())
            assertThat(Files.exists(pipeline.path.get())).isFalse()
            if (released) {
                assertThat(databaseState()).isEqualTo(recoveryState)
                verify(writer, times(1)).upsertAll(any())
                verify(store, never()).renew(job.token)
            } else {
                assertThat(row(job)["outcome"]).isEqualTo("INDEXED")
                val retained = argumentCaptor<List<DocumentChunk>>()
                verify(writer, times(2)).upsertAll(retained.capture())
                assertThat(retained.secondValue).isSameAs(retained.firstValue)
                verify(store, times(1)).renew(job.token)
                assertThat(row(job)["attempt_count"]).isEqualTo(job.attemptCount)
                assertThat(row(job)["execution_epoch"]).isEqualTo(job.token.epoch)
            }
        } finally { recover.countDown(); pool.shutdownNow() }
    }

    @Test fun `실제 완료 커밋 응답 유실과 후속 claim 뒤에도 pipeline은 외부 호출과 결과 쓰기를 반복하지 않는다`() {
        val job = claim()
        queue.enqueue(UUID.randomUUID(), 1, 2, JobType.INDEX, null)
        val completed = CountDownLatch(1); val advance = CountDownLatch(1)
        doAnswer { it.callSpiedMethod(); source.loseNextCommit.set(true); 1 }.whenever(jobs).complete(any(), any(), any(), any())
        val database = DatabaseOperations(jdbc, RetryWaiter {
            completed.countDown(); check(advance.await(10, TimeUnit.SECONDS))
        }, DatabaseWaitProperties(checkInterval = Duration.ofMillis(1)), SimpleMeterRegistry())
        val controlled = PipelineFixture(database)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val running = pool.submit { controlled.runner.run(job) }
            check(completed.await(5, TimeUnit.SECONDS))
            assertThat(row(job)["status"]).isEqualTo("COMPLETED")
            assertThat(row(job)["worker_id"]).isNull()
            assertThat(row(job)["lease_until"]).isNull()
            val next = checkNotNull(queue.claim(5, "next-2"))
            assertThat(next.token.epoch).isEqualTo(job.token.epoch + 1)
            val before = databaseState()
            advance.countDown(); running.get(5, TimeUnit.SECONDS)
            assertThat(databaseState()).isEqualTo(before)
            verify(controlled.download, times(1)).download(any())
            verify(controlled.embedding, times(1)).embed(any())
            verify(writer, times(1)).upsertAll(any())
            verify(jobs, times(1)).complete(any(), any(), any(), any())
            assertThat(Files.exists(controlled.path.get())).isFalse()
        } finally { advance.countDown(); pool.shutdownNow() }
    }

    @Test fun `서킷 대기 중 커밋된 삭제는 현재 INDEX를 skip 완료하고 후속 DELETE가 정리한다`() {
        val job=claim()
        doReturn(CallAdmission.Blocked(Duration.ofSeconds(30))).whenever(breaker).acquire(any(),any())
        queue.enqueue(UUID.randomUUID(),1,null,JobType.DELETE,null)
        val waiting=CountDownLatch(1); val deleted=CountDownLatch(1)
        val pipeline=PipelineFixture(circuitWaiter=RetryWaiter {
            waiting.countDown(); check(deleted.await(5,TimeUnit.SECONDS))
        })
        val pool=Executors.newSingleThreadExecutor()
        try {
            val running=pool.submit { pipeline.runner.run(job) }
            check(waiting.await(5,TimeUnit.SECONDS))
            assertThat(row(job)["status"]).isEqualTo("PROCESSING")
            assertThat(queue.claim(5,"early-delete")).isNull()
            jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
            deleted.countDown(); running.get(5,TimeUnit.SECONDS)
            assertThat(row(job)["outcome"]).isEqualTo("SKIPPED_DOCUMENT_DELETED")
            assertThat(row(job)["worker_id"]).isNull(); assertThat(row(job)["lease_until"]).isNull()
            assertThat(row(job)["attempt_count"]).isEqualTo(job.attemptCount)
            assertThat(row(job)["execution_epoch"]).isEqualTo(job.token.epoch)
            assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1",Long::class.java)).isEqualTo(job.token.epoch)
            verifyNoInteractions(pipeline.embedding)
            verify(breaker,never()).record(any(),any())
            val next=queue.claim(5,"delete")!!
            assertThat(next.type).isEqualTo(JobType.DELETE)
            assertThat(deletion.delete(next)).isEqualTo(IndexingJobStatus.COMPLETED)
            assertThat(row(next)["outcome"]).isEqualTo("DELETED")
        } finally { deleted.countDown(); pool.shutdownNow() }
    }

    @ParameterizedTest @ValueSource(strings=["expired","RETRY_WAIT","FAILED","epoch"])
    fun `서킷 획득 DB 복구 뒤 실제 SQL 실행권 갱신 또는 회수 종료를 거쳐 호출한다`(condition:String) {
        val job=claim(); var waits=0
        val database=DatabaseOperations(jdbc,RetryWaiter {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse()
            assertThat(source.openConnections.get()).isZero()
            waits++
            when(condition) {
                "expired" -> jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=?",job.token.jobId)
                "FAILED" -> com.osscontest.worker.support.seedReleasedExecution(jdbc,job.token,IndexingJobStatus.FAILED)
                else -> {
                    com.osscontest.worker.support.seedReleasedExecution(jdbc,job.token,IndexingJobStatus.RETRY_WAIT,
                        queue.dbNow().minusSeconds(1))
                    if(condition=="epoch") assertThat(queue.claim(5,"new-owner")!!.token.epoch).isGreaterThan(job.token.epoch)
                }
            }
        },DatabaseWaitProperties(),SimpleMeterRegistry())
        doThrow(org.springframework.dao.DataAccessResourceFailureException("acquire DB down"))
            .doReturn(CallAdmission.Allowed(CircuitPermit("embedding-api",0,"recovered"))).whenever(breaker).acquire(any(),any())
        val pipeline=PipelineFixture(database=database)
        pipeline.runner.run(job)
        assertThat(waits).isEqualTo(1)
        assertThat(row(job)["attempt_count"]).isEqualTo(if(condition=="epoch") job.attemptCount+1 else job.attemptCount)
        if(condition=="expired") {
            assertThat(row(job)["outcome"]).isEqualTo("INDEXED")
            assertThat(row(job)["execution_epoch"]).isEqualTo(job.token.epoch)
            verify(pipeline.embedding,times(1)).embed(any())
        } else {
            verifyNoInteractions(pipeline.embedding)
            verify(writer,never()).upsertAll(any())
            assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk",Int::class.java)).isZero()
            assertThat(row(job)["status"]).isEqualTo(if(condition=="epoch") "PROCESSING" else condition)
        }
    }

    @ParameterizedTest @ValueSource(ints=[1,5])
    fun `서킷을 연 실제 API 오류도 공통 실패 서비스가 예약 또는 한도 종결한다`(attempt:Int) {
        queue.enqueue(UUID.randomUUID(),1,1,JobType.INDEX,null)
        jdbc.update("UPDATE indexing_job SET attempt_count=?",attempt-1)
        val job=queue.claim(5,"failing")!!
        val pipeline=PipelineFixture()
        whenever(pipeline.embedding.embed(any())).thenThrow(RuntimeException("503"))
        doAnswer { doReturn(ClaimAdmission.BLOCKED).whenever(breaker).admission(); null }
            .whenever(breaker).record(any(),any())
        pipeline.runner.run(job)
        assertThat(row(job)["status"]).isEqualTo(if(attempt==5) "FAILED" else "RETRY_WAIT")
        assertThat(row(job)["next_retry_at"]).let { if(attempt==5) it.isNull() else it.isNotNull() }
        assertThat(row(job)["attempt_count"]).isEqualTo(attempt)
        assertThat(row(job)["execution_epoch"]).isEqualTo(job.token.epoch)
        assertThat(row(job)["worker_id"]).isNull(); assertThat(row(job)["lease_until"]).isNull()
        verify(pipeline.embedding,times(1)).embed(any())
    }

    @Test fun `동시 처리 하나에서 API 실패의 retry 기한을 기다리지 않고 다른 문서 DELETE를 처리한다`() {
        val job=claim()
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=2")
        queue.enqueue(UUID.randomUUID(),2,null,JobType.DELETE,null)
        val pipeline=PipelineFixture(deletion=deletion)
        whenever(pipeline.embedding.embed(any())).thenThrow(RuntimeException("503"))
        val finished=CountDownLatch(1)
        doAnswer {
            val result=it.callSpiedMethod()
            if(it.getArgument<JobOutcome>(3)==JobOutcome.DELETED) {
                TransactionSynchronizationManager.registerSynchronization(object : org.springframework.transaction.support.TransactionSynchronization {
                    override fun afterCommit() { finished.countDown() }
                })
            }
            result
        }.whenever(jobs).complete(any(),any(),any(),any())
        val worker=DbJobWorker(QueueProperties(concurrency=1,pollInterval=Duration.ofDays(1)),pipeline.database,
            queue,pipeline.runner,ThreadSleepQueueLoopWaiter(),5,ExecutionResults())
        // 이미 커밋된 첫 claim을 넘긴 뒤 실제 큐 진입점으로 이어 간다.
        // spy에 whenever(spy.claim(...))를 쓰면 stub을 거는 순간 실제 claim이 null 인자로 호출된다.
        doAnswer {
            if(row(job)["status"]=="PROCESSING") job else it.callSpiedMethod()
        }.whenever(store).claim(eq(5),any(),any())
        worker.start()
        try {
            check(finished.await(5,TimeUnit.SECONDS))
            assertThat(row(job)["status"]).isEqualTo("RETRY_WAIT")
            assertThat(jdbc.queryForObject("SELECT status FROM indexing_job WHERE job_type='DELETE'",String::class.java)).isEqualTo("COMPLETED")
            verify(pipeline.embedding,times(1)).embed(any())
        } finally { worker.stop() }
    }

    private fun deleteFixture():ClaimedJob {
        val first=DocumentChunk(1,1,0,"stored","stored","hash",1,null,null,null,null,FloatArray(1536){0.1f},LocalDateTime.now())
        TransactionTemplate(transactionManager).execute {
            writer.upsertAll(listOf(first,first.copy(documentVersionId=2),first.copy(documentId=2,documentVersionId=3)))
        }
        jdbc.update("UPDATE document SET deleted_at=clock_timestamp() WHERE id=1")
        queue.enqueue(UUID.randomUUID(),1,null,JobType.DELETE,null)
        return queue.claim(5,"delete-owner")!!
    }
    @Test fun `실제 DELETE 서비스는 모든 버전 청크를 삭제하고 다른 문서와 버전 행을 보존한다`() {
        val job=deleteFixture(); deletion.delete(job)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk WHERE document_id=1",Int::class.java)).isZero()
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_chunk WHERE document_id=2",Int::class.java)).isEqualTo(1)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM document_version",Int::class.java)).isEqualTo(3)
        assertThat(row(job)["outcome"]).isEqualTo("DELETED")
        assertThat(row(job)["worker_id"]).isNull(); assertThat(row(job)["lease_until"]).isNull()
    }
    @ParameterizedTest @ValueSource(strings=["chunks","completion"])
    fun `DELETE 삭제 후와 완료 UPDATE 후 중단은 실제 서비스 TX 전체를 rollback한다`(stage:String) {
        val job=deleteFixture(); val before=databaseState()
        if(stage=="chunks") doAnswer { it.callSpiedMethod(); error("after deletion") }.whenever(chunks).deleteByDocumentId(1)
        else doAnswer { it.callSpiedMethod(); error("after completion") }.whenever(jobs).complete(any(),any(),any(),any())
        assertThatThrownBy { deletion.delete(job) }.isInstanceOf(IllegalStateException::class.java)
        assertThat(databaseState()).isEqualTo(before)
    }
    @Test fun `DELETE 완료 응답 유실 뒤 소유권 해제와 후속 epoch 증가에도 처리기를 반복하지 않는다`() {
        val job=deleteFixture()
        source.loseNextCommit.set(true)
        assertThatThrownBy { deletion.delete(job) }.isInstanceOf(Exception::class.java)
        queue.enqueue(UUID.randomUUID(),1,2,JobType.INDEX,null)
        val next=queue.claim(5,"successor")!!
        assertThat(next.token.epoch).isGreaterThan(job.token.epoch)
        val before=databaseState()
        var handlers=0
        execution().completion(job.token,ExecutionIdentity(job),completed={}) { handlers++; deletion.delete(job); Unit }
        assertThat(handlers).isZero()
        assertThat(databaseState()).isEqualTo(before)
    }
    @ParameterizedTest @ValueSource(strings=["expired","worker","epoch","active"])
    fun `DELETE의 잘못된 실행권과 활성 문서에서는 실제 서비스가 변경하지 않는다`(condition:String) {
        val job=deleteFixture()
        when(condition) {
            "expired" -> jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()-INTERVAL '1 second'")
            "worker" -> jdbc.update("UPDATE indexing_job SET worker_id='different'")
            "epoch" -> jdbc.update("UPDATE document SET execution_epoch=execution_epoch+1 WHERE id=1")
            else -> jdbc.update("UPDATE document SET deleted_at=NULL WHERE id=1")
        }
        val before=databaseState()
        assertThatThrownBy { deletion.delete(job) }.isInstanceOf(RuntimeException::class.java)
        assertThat(databaseState()).isEqualTo(before)
    }

    /** 실제 Worker·runner·임베딩 흐름과 Spring 서비스를 쓰고 외부 작업만 테스트가 제어한다. */
    private inner class PipelineFixture(
        val database: DatabaseOperations = DatabaseOperations(jdbc, RetryWaiter {}, DatabaseWaitProperties(), SimpleMeterRegistry()),
        deletion: DocumentDeletionService = mock(),
        circuitWaiter: RetryWaiter = RetryWaiter {},
    ) {
        val download: DocumentDownloadClient = mock()
        val parsing: ParsingTimeoutGuard = mock()
        val embedding: EmbeddingUseCase = mock()
        val path = AtomicReference<Path>()
        private val operations = ExecutionOperations(database, queue)
        private val registry: DocumentParserRegistry = mock()
        private val chunker: ChunkingService = mock()
        private val processor = EmbeddingIndexingProcessor(embedding, publication, NoriTokenizer(), Clock.systemUTC(),
            250, ExternalApiOperations(operations, database, breaker, circuitWaiter, DatabaseWaitProperties(), publication), operations)
        val runner = IndexingPipelineRunner(jobs, documents, versions, download, registry, parsing, chunker,
            mock<ChunkGuard>(), processor, failures, mock(), operations, deletion, 5, Duration.ofSeconds(30), 10000,
            ChunkingStrategy.FIXED_TOKEN, SimpleMeterRegistry(), publication = publication)

        init {
            jdbc.update("UPDATE document_version SET file_size=11, content_hash=? WHERE id=1",
                "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
            whenever(download.download(any())).thenAnswer { temporaryFile() }
            whenever(registry.findParser(any())).thenReturn(mock())
            whenever(parsing.parse(any(), any(), any())).thenReturn(emptyList())
            whenever(chunker.chunk(any(), any())).thenReturn(listOf(Chunk(0, "hello world", "hash", 1, null, null, null, null)))
            whenever(embedding.embed(any())).thenReturn(listOf(FloatArray(1536) { 0.1f }))
        }
        fun temporaryFile(): Path = Files.createTempFile("worker-contract-", ".txt").also {
            Files.writeString(it, "hello world"); path.set(it)
        }
    }

    private fun databaseState(): Map<String, List<Map<String, Any?>>> =
        listOf("document", "document_version", "indexing_job", "document_chunk").associateWith {
            jdbc.queryForList("SELECT * FROM $it ORDER BY id")
        }

    private fun waitUntil(condition: () -> Boolean) {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition()) { check(System.nanoTime() < until); Thread.sleep(10) }
    }

    private fun waitForLock() {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'",
                Int::class.java)!! == 0) {
            check(System.nanoTime() < until); Thread.sleep(10)
        }
    }
}
