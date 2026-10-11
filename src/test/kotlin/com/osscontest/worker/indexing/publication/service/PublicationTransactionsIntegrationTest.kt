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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.persistence.EntityManagerFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.*
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
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/** Worker-owned private schema. Uses the real JDBC writer and JPA repositories on one transaction manager. */
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
) {
    @MockitoSpyBean private lateinit var writer: DocumentChunkWriter
    @MockitoSpyBean private lateinit var chunks: DocumentChunkRepository
    @MockitoSpyBean private lateinit var versions: DocumentVersionRepository
    @MockitoSpyBean private lateinit var documents: DocumentRepository
    @MockitoSpyBean private lateinit var jobs: IndexingJobRepository
    @MockitoSpyBean private lateinit var state: PublicationStateReader

    @Configuration
    @EnableTransactionManagement(proxyTargetClass = true)
    @EnableJpaRepositories(basePackageClasses = [IndexingJobRepository::class])
    @Import(JobExecutionLocks::class, JobOwnershipGuard::class, JobClaimTransactions::class, JobQueueStore::class,
        IndexingJobWritesImpl::class, PublicationStateReader::class, DocumentChunkWriter::class,
        IndexingPublicationService::class, IndexingFailureService::class, ExecutionResults::class,
        RetryPolicy::class, RandomRetryJitter::class)
    class Config {
        @Bean fun dataSource(): CommitResponseLossDataSource {
            val url = requireNotNull(System.getenv("INTEGRATION_DB_URL"))
            val user = requireNotNull(System.getenv("INTEGRATION_DB_USERNAME"))
            val password = System.getenv("INTEGRATION_DB_PASSWORD").orEmpty()
            val base = DriverManagerDataSource(url, user, password)
            assertDedicatedIntegrationDatabase(JdbcTemplate(base))
            JdbcTemplate(base).execute("CREATE SCHEMA worker_c3_publication")
            // pgvector is a prerequisite of the dedicated DB, not created by this fixture.
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
        return checkNotNull(queue.claimForWorker("instance-1", 60_000, 5))
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
        val interrupt = { error("stop after $stage") }
        when (stage) {
            "upsert" -> doAnswer { it.callRealMethod(); interrupt() }.whenever(writer).upsertAll(any())
            "trailing" -> doAnswer { it.callRealMethod(); interrupt() }.whenever(chunks).deleteTrailingChunks(any(), any())
            "version" -> doAnswer { it.callRealMethod(); interrupt() }.whenever(versions).complete(any(), any(), any(), anyOrNull())
            "promotion" -> doAnswer { it.callRealMethod(); interrupt() }.whenever(documents).promoteSearchableVersion(any(), any())
            else -> doAnswer { it.callRealMethod(); interrupt() }.whenever(jobs).complete(any(), any(), any(), any())
        }
        assertThatThrownBy { publish(job) }.isInstanceOf(IllegalStateException::class.java)
        assertRolledBack(job, before)
    }

    @Test fun `완료 직전 lease 만료는 이미 쓴 결과 전체를 롤백한다`() {
        val job = claim(); val before = row(job)
        doAnswer {
            jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=?", job.token.jobId)
            it.callRealMethod()
        }.whenever(jobs).complete(any(), any(), any(), any())
        assertThatThrownBy { publish(job) }.isInstanceOf(LeaseExpiredException::class.java)
        assertRolledBack(job, before)
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
        publish(job) // receipt acknowledges the same result, with no writes
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
            it.callRealMethod()
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
        doAnswer { locked.countDown(); check(finish.await(5, TimeUnit.SECONDS)); it.callRealMethod() }
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

    private fun waitForLock() {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'",
                Int::class.java)!! == 0) {
            check(System.nanoTime() < until); Thread.sleep(10)
        }
    }
}
