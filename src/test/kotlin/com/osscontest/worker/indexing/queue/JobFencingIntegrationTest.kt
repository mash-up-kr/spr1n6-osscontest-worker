package com.osscontest.worker.indexing.queue

import com.osscontest.worker.support.claimFixture
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import com.osscontest.worker.indexing.pipeline.service.ThreadSleepRetryWaiter
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobWritesImpl
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import com.osscontest.worker.support.CommitResponseLossDataSource
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.*
import org.springframework.boot.convert.ApplicationConversionService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.dao.InvalidDataAccessApiUsageException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter
import jakarta.persistence.EntityManagerFactory
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/** 전용 스키마에서 Worker SQL만 검증한다. Core 마이그레이션, 회수 함수, 외부 API는 쓰지 않는다. */
@Tag("integration")
@SpringJUnitConfig(JobFencingIntegrationTest.Config::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobFencingIntegrationTest(
    private val queue: JobQueueStore,
    private val writes: IndexingJobRepository,
    private val jdbc: JdbcTemplate,
    private val transactions: PlatformTransactionManager,
    private val source: CommitResponseLossDataSource,
) {
    @MockitoSpyBean private lateinit var locks: JobExecutionLocks
    @MockitoSpyBean private lateinit var ownership: JobOwnershipGuard
    @MockitoSpyBean private lateinit var claims: JobClaimTransactions

    @Configuration
    @EnableTransactionManagement(proxyTargetClass = true)
    @EnableJpaRepositories(basePackageClasses = [IndexingJobRepository::class])
    @Import(JobExecutionLocks::class, JobOwnershipGuard::class, JobClaimTransactions::class,
        JobQueueStore::class, IndexingJobWritesImpl::class)
    class Config {
        @Bean fun circuitBreaker() = com.osscontest.worker.support.closedCircuitBreaker()
        @Bean fun dataSource(): CommitResponseLossDataSource {
            val url = requireNotNull(System.getenv("INTEGRATION_DB_URL"))
            val user = requireNotNull(System.getenv("INTEGRATION_DB_USERNAME"))
            val password = System.getenv("INTEGRATION_DB_PASSWORD").orEmpty()
            val base = DriverManagerDataSource(url, user, password)
            assertDedicatedIntegrationDatabase(JdbcTemplate(base))
            // 이전 실행이 컨텍스트 생성 중 실패하면 스키마가 남으므로 먼저 지운다.
            JdbcTemplate(base).execute("DROP SCHEMA IF EXISTS worker_c1_fencing CASCADE")
            JdbcTemplate(base).execute("CREATE SCHEMA worker_c1_fencing")
            return CommitResponseLossDataSource(DriverManagerDataSource(url + if ('?' in url) "&currentSchema=worker_c1_fencing"
                else "?currentSchema=worker_c1_fencing", user, password))
        }
        @Bean fun jdbc(dataSource: DataSource) = JdbcTemplate(dataSource)
        // Boot 없이 띄운 컨텍스트라 @Value의 "PT30S" 같은 Duration 문자열을 바꿀 변환기를 직접 등록한다.
        @Bean fun conversionService() = ApplicationConversionService.getSharedInstance()
        @Bean fun namedJdbc(jdbc: JdbcTemplate) = NamedParameterJdbcTemplate(jdbc)
        @Bean fun entityManagerFactory(dataSource: DataSource) = LocalContainerEntityManagerFactoryBean().apply {
            setDataSource(dataSource)
            setPackagesToScan("com.osscontest.worker.indexing.publication.entity")
            jpaVendorAdapter = HibernateJpaVendorAdapter()
            setJpaPropertyMap(mapOf("hibernate.hbm2ddl.auto" to "none", "hibernate.jdbc.time_zone" to "UTC"))
        }
        @Bean fun transactionManager(entityManagerFactory: EntityManagerFactory) = JpaTransactionManager(entityManagerFactory)
        @Bean fun properties() = QueueProperties(workerId = "c1-worker")
    }

    @BeforeAll
    fun schema() {
        jdbc.execute("CREATE TABLE document (id BIGINT PRIMARY KEY, execution_epoch BIGINT NOT NULL DEFAULT 0, deleted_at TIMESTAMP)")
        jdbc.execute("""
            CREATE TABLE indexing_job (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                source_event_id UUID NOT NULL UNIQUE, document_id BIGINT NOT NULL REFERENCES document(id),
                document_version_id BIGINT, job_type VARCHAR(20) NOT NULL DEFAULT 'INDEX',
                enqueue_seq BIGINT GENERATED BY DEFAULT AS IDENTITY UNIQUE, status VARCHAR(20) NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0, execution_epoch BIGINT NOT NULL DEFAULT 0,
                worker_id VARCHAR(100), lease_until TIMESTAMPTZ, next_retry_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ, trace_id TEXT, phase TEXT, outcome TEXT,
                last_error_code TEXT, last_error_message TEXT
            )
        """.trimIndent())
        jdbc.execute("CREATE TABLE saved_result (job_id BIGINT, result TEXT)")
    }

    @AfterAll fun dropSchema() { jdbc.execute("DROP SCHEMA worker_c1_fencing CASCADE") }

    @BeforeEach fun fixture() {
        source.loseNextCommit.set(false)
        jdbc.update("DELETE FROM saved_result")
        jdbc.update("DELETE FROM indexing_job")
        jdbc.update("DELETE FROM document")
        for (id in 1L..3L) jdbc.update("INSERT INTO document(id) VALUES (?)", id)
    }

    private fun enqueue(documentId: Long = 1): Long {
        val event = UUID.randomUUID()
        queue.enqueue(event, documentId, documentId, JobType.INDEX, null)
        return checkNotNull(jdbc.queryForObject("SELECT id FROM indexing_job WHERE source_event_id = ?", Long::class.java, event))
    }
    private fun claim(): ClaimedJob { enqueue(); return checkNotNull(queue.claimFixture(5) {}) }
    private fun row(id: Long) = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", id)
    private fun expire(id: Long) { jdbc.update("UPDATE indexing_job SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", id) }
    private fun waitUntil(condition: () -> Boolean) {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition()) {
            check(System.nanoTime() < until) { "Timed out waiting for database condition" }
            Thread.sleep(10)
        }
    }
    private fun waitForLock() = waitUntil {
        jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", Int::class.java)!! > 0
    }

    private fun <T> withLock(sql: String, action: () -> T): T {
        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val pool = Executors.newSingleThreadExecutor()
        val holder = pool.submit {
            TransactionTemplate(transactions).execute {
                jdbc.queryForList(sql)
                held.countDown()
                check(release.await(10, TimeUnit.SECONDS))
            }
        }
        try {
            check(held.await(5, TimeUnit.SECONDS))
            return action()
        } finally { release.countDown(); holder.get(5, TimeUnit.SECONDS); pool.shutdownNow() }
    }

    @Test fun `다중 Worker가 동시에 같은 문서의 Job을 중복 claim하지 않는다`() {
        val first = enqueue(); enqueue()
        val other = enqueue(2)
        val start = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(4)
        try {
            val results = (1..4).map { i -> pool.submit<ClaimedJob?> { start.await(); queue.claim(5, "worker-$i") } }
            start.countDown()
            val acquired = results.mapNotNull { it.get(5, TimeUnit.SECONDS) }
            assertThat(acquired.map { it.token.jobId }).containsExactlyInAnyOrder(first, other)
            assertThat(row(first)["attempt_count"]).isEqualTo(1)
            assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isEqualTo(1)
        } finally { pool.shutdownNow() }
    }

    @Test fun `A claim 롤백 뒤 B가 같은 epoch를 얻어도 A 준비 토큰으로 B를 resolve하지 않는다`() {
        enqueue()
        var prepared: ExecutionToken? = null
        TransactionTemplate(transactions).execute { tx ->
            queue.claim(5, "instance-1") { prepared = it }
            tx.setRollbackOnly()
        }
        val next = checkNotNull(queue.claim(5, "instance-2"))
        assertThat(next.token.epoch).isEqualTo(checkNotNull(prepared).epoch)
        assertThat(queue.resolveClaim(checkNotNull(prepared))).isNull()
        assertThat(queue.resolveClaim(next.token)).isEqualTo(next)
    }

    @Test fun `동적 Worker는 프로시저 없는 DB에서 N건만 claim하고 처리 반환 후 남은 Job을 획득한다`() {
        val ids = listOf(enqueue(), enqueue(2), enqueue(3))
        val started = CountDownLatch(2)
        val finish = CountDownLatch(1)
        val completed = CountDownLatch(3)
        val claimed = java.util.concurrent.ConcurrentLinkedQueue<ClaimedJob>()
        val threads = java.util.concurrent.ConcurrentHashMap.newKeySet<Thread>()
        val runner: IndexingPipelineRunner = mock()
        doAnswer {
            val job = it.getArgument<ClaimedJob>(0)
            // 트랜잭션 밖의 이 JDBC 조회는 다른 연결을 써서 커밋된 claim을 본다.
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse()
            val committed = row(job.token.jobId)
            assertThat(committed["status"]).isEqualTo("PROCESSING")
            assertThat(committed["worker_id"]).isEqualTo(job.token.workerId)
            assertThat(committed["execution_epoch"]).isEqualTo(job.token.epoch)
            assertThat(committed["lease_until"]).isNotNull()
            assertThat(committed["attempt_count"]).isEqualTo(1)
            assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=?", Long::class.java,
                job.documentId)).isEqualTo(job.token.epoch)
            threads.add(Thread.currentThread())
            claimed.add(job)
            started.countDown()
            check(finish.await(5, TimeUnit.SECONDS))
            check(writes.complete(job.token.jobId, job.token.workerId, job.token.epoch) == 1)
            completed.countDown()
            null
        }.whenever(runner).run(any<ClaimedJob>())
        val database = DatabaseOperations(jdbc, ThreadSleepRetryWaiter(), DatabaseWaitProperties(), SimpleMeterRegistry())
        val worker = DbJobWorker(QueueProperties(workerId = "c1-worker", concurrency = 2,
            pollInterval = Duration.ofDays(1)), database, queue, runner, ThreadSleepQueueLoopWaiter(), 5, ExecutionResults())
        worker.start()
        try {
            check(started.await(5, TimeUnit.SECONDS))
            assertThat(worker.probe()).isFalse()
            assertThat(claimed).hasSize(2)
            assertThat(jdbc.queryForObject("SELECT count(*) FROM indexing_job WHERE status='PROCESSING'", Int::class.java)).isEqualTo(2)
            assertThat(jdbc.queryForObject("SELECT count(*) FROM indexing_job WHERE status='PENDING'", Int::class.java)).isEqualTo(1)
            finish.countDown()
            check(completed.await(5, TimeUnit.SECONDS))
            assertThat(claimed.map { it.token.jobId }).containsExactlyInAnyOrderElementsOf(ids)
            assertThat(jdbc.queryForObject("SELECT count(*) FROM indexing_job WHERE status='COMPLETED'", Int::class.java)).isEqualTo(3)
        } finally {
            finish.countDown()
            worker.stop()
            threads.forEach { it.join(5000) }
        }
    }

    @Test fun `미래 선행 재시도는 후속을 막고 다른 문서는 처리하며 due 뒤 같은 Job을 재claim한다`() {
        val job = claim(); val original = row(job.token.jobId)
        val successor = enqueue(); val other = enqueue(2)
        assertThat(writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, "RETRY_WAIT", queue.dbNow().plusSeconds(60), "TEMP", "test")).isEqualTo(1)
        assertThat(queue.claimFixture(5) {}!!.token.jobId).isEqualTo(other)
        assertThat(queue.claimFixture(5) {}).isNull()
        assertThat(row(successor)["status"]).isEqualTo("PENDING")
        jdbc.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id=?", job.token.jobId)
        val retry = checkNotNull(queue.claimFixture(5) {})
        assertThat(retry.token.jobId).isEqualTo(job.token.jobId)
        assertThat(retry.token.epoch).isEqualTo(job.token.epoch + 1)
        assertThat(retry.attemptCount).isEqualTo(2)
        assertThat(row(retry.token.jobId)["enqueue_seq"]).isEqualTo(original["enqueue_seq"])
        assertThat(retry.sourceEventId).isEqualTo(job.sourceEventId)
    }

    @Test fun `한 문서 heartbeat의 실제 잠금 대기가 다른 문서 갱신을 막지 않는다`() {
        enqueue(); enqueue(2)
        val active = java.util.concurrent.ConcurrentLinkedQueue<ClaimedJob>()
        val started = CountDownLatch(2); val finish = CountDownLatch(1)
        val threads = java.util.concurrent.ConcurrentHashMap.newKeySet<Thread>()
        val runner: IndexingPipelineRunner = mock()
        doAnswer {
            active.add(it.getArgument(0)); threads.add(Thread.currentThread()); started.countDown()
            finish.await(); null
        }.whenever(runner).run(any<ClaimedJob>())
        val database = DatabaseOperations(jdbc, ThreadSleepRetryWaiter(), DatabaseWaitProperties(), SimpleMeterRegistry())
        val worker = DbJobWorker(QueueProperties(concurrency = 2, pollInterval = Duration.ofDays(1)),
            database, queue, runner, ThreadSleepQueueLoopWaiter(), 5, ExecutionResults())
        worker.start()
        try {
            check(started.await(5, TimeUnit.SECONDS))
            val other = active.single { it.documentId == 2L }
            val before = checkNotNull(queue.snapshot(other.token)?.leaseUntil)
            val tasks = withLock("SELECT id FROM document WHERE id=1 FOR UPDATE") {
                val pending = worker.heartbeat()
                waitForLock()
                waitUntil { checkNotNull(queue.snapshot(other.token)?.leaseUntil) > before }
                assertThat(pending.any { !it.isDone }).isTrue()
                pending
            }
            tasks.forEach { it.get(5, TimeUnit.SECONDS) }
            assertThat(queue.snapshot(other.token)!!.ownsProcessing(other.token)).isTrue()
        } finally { finish.countDown(); worker.stop(); threads.forEach { it.join(5000) } }
    }

    @Test fun `활성 실패 API는 잘못된 상태 deadline 조합에서 행을 변경하지 않는다`() {
        val job = claim(); val before = row(job.token.jobId)
        for ((status, deadline) in listOf("COMPLETED" to null, "RETRY_WAIT" to null,
                "FAILED" to queue.dbNow().plusSeconds(30))) {
            assertThatThrownBy {
                writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, status, deadline, "ERROR", "test")
            }.isInstanceOf(InvalidDataAccessApiUsageException::class.java)
            // @Repository 예외 변환이 require 실패를 감싼다.
            .hasCauseInstanceOf(IllegalArgumentException::class.java)
            assertThat(row(job.token.jobId)).isEqualTo(before)
        }
    }

    @Test fun `잠긴 선행 Job만 건너뛰고 같은 문서 후속 대신 다른 문서를 claim한다`() {
        val first = enqueue(); val successor = enqueue(); val other = enqueue(2)
        withLock("SELECT id FROM indexing_job WHERE id=$first FOR UPDATE") {
            assertThat(queue.claimFixture(5) {}!!.token.jobId).isEqualTo(other)
            assertThat(row(first)["status"]).isEqualTo("PENDING")
            assertThat(row(successor)["status"]).isEqualTo("PENDING")
        }
    }

    @Test fun `문서만 잠긴 후보를 기다리지 않고 다른 문서를 claim한다`() {
        enqueue(); val other = enqueue(2)
        withLock("SELECT id FROM document WHERE id=1 FOR UPDATE") {
            assertThat(queue.claimFixture(5) {}!!.token.jobId).isEqualTo(other)
        }
    }

    @Test fun `claim 롤백은 문서 Job epoch와 attempt 증가를 함께 취소한다`() {
        val id = enqueue()
        TransactionTemplate(transactions).execute { tx ->
            assertThat(queue.claimFixture(5) {}!!.token.jobId).isEqualTo(id)
            tx.setRollbackOnly()
        }
        assertThat(row(id)["status"]).isEqualTo("PENDING")
        assertThat(row(id)["attempt_count"]).isEqualTo(0)
        assertThat(row(id)["execution_epoch"]).isEqualTo(0L)
        assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isZero()
    }

    @Test fun `문서 잠금 뒤 후보 상태가 바뀌면 조건을 재검사하고 epoch를 발급하지 않는다`() {
        val id = enqueue()
        val selected = CountDownLatch(1)
        val resume = CountDownLatch(1)
        doAnswer {
            selected.countDown()
            check(resume.await(5, TimeUnit.SECONDS))
            it.callRealMethod()
        }.whenever(locks).lockJob(id, true)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val result = pool.submit<ClaimedJob?> { queue.claimFixture(5) { error("Ineligible claim issued a token") } }
            check(selected.await(5, TimeUnit.SECONDS))
            // 회수 절차 없이 Job 행만 잠그는 과거 Writer의 동시 실행을 흉내 낸다.
            jdbc.update("UPDATE indexing_job SET status='COMPLETED' WHERE id=?", id)
            resume.countDown()
            assertThat(result.get(5, TimeUnit.SECONDS)).isNull()
            assertThat(row(id)["attempt_count"]).isEqualTo(0)
            assertThat(row(id)["execution_epoch"]).isEqualTo(0L)
            assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isZero()
        } finally { resume.countDown(); pool.shutdownNow() }
    }

    @Test fun `Worker Job epoch 문서 epoch 불일치는 모든 쓰기를 거절한다`() {
        val job = claim()
        val tokens = listOf(job.token.copy(workerId = "other"), job.token.copy(epoch = job.token.epoch + 1), job.token)
        tokens.forEach { token ->
            if (token == job.token) jdbc.update("UPDATE document SET execution_epoch=execution_epoch+1 WHERE id=1")
            val before = row(job.token.jobId)
            assertThat(queue.renew(token)).isZero()
            assertThat(writes.updatePhase(token.jobId, "PARSING", token.workerId, token.epoch)).isZero()
            assertThat(writes.complete(token.jobId, token.workerId, token.epoch)).isZero()
            assertThat(writes.fail(token.jobId, token.workerId, token.epoch, "FAILED", null, "ERROR", "test")).isZero()
            assertThat(row(job.token.jobId)).isEqualTo(before)
        }
        assertThat(queue.resolveClaim(job.token)).isNull()
    }

    @Test fun `일반 쓰기는 만료를 거절하고 미회수 heartbeat만 갱신하며 횟수와 epoch를 유지한다`() {
        val job = claim(); expire(job.token.jobId)
        val before = row(job.token.jobId)
        assertThat(writes.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)).isZero()
        assertThat(writes.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isZero()
        assertThat(writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, "FAILED", null, "ERROR", "test")).isZero()
        assertThat(queue.resolveClaim(job.token)).isNotNull()
        assertThat(queue.renew(job.token)).isEqualTo(1)
        val after = row(job.token.jobId)
        for (column in listOf("attempt_count", "execution_epoch", "source_event_id", "enqueue_seq")) assertThat(after[column]).isEqualTo(before[column])
        assertThat(queue.snapshot(job.token)!!.leaseUntil).isAfter(queue.dbNow())
    }

    @Test fun `회수 후 재claim 전에도 이전 Worker가 실행을 되살리지 못한다`() {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET status='RETRY_WAIT', worker_id=NULL, lease_until=NULL, next_retry_at=clock_timestamp()+INTERVAL '1 minute' WHERE id=?", job.token.jobId)
        val before = row(job.token.jobId)
        assertThat(queue.renew(job.token)).isZero()
        assertThat(writes.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)).isZero()
        assertThat(writes.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isZero()
        assertThat(writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, "FAILED", null, "ERROR", "test")).isZero()
        assertThat(row(job.token.jobId)).isEqualTo(before)
    }

    @Test fun `phase는 문서 잠금 대기 중 Job을 먼저 잠그지 않고 만료 뒤 쓰기도 거절한다`() {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()+INTERVAL '500 milliseconds' WHERE id=?", job.token.jobId)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val future = withLock("SELECT id FROM document WHERE id=1 FOR UPDATE") {
                val pending = pool.submit<Int> { writes.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch) }
                waitForLock()
                TransactionTemplate(transactions).execute {
                    // 대기 중인 Writer가 Job을 먼저 잠그지 않았을 때만 이 NOWAIT가 성공한다.
                    jdbc.queryForList("SELECT id FROM indexing_job WHERE id=${job.token.jobId} FOR UPDATE NOWAIT")
                }
                waitUntil { jdbc.queryForObject("SELECT lease_until <= clock_timestamp() FROM indexing_job WHERE id=?", Boolean::class.java, job.token.jobId) == true }
                pending
            }
            assertThat(future.get(5, TimeUnit.SECONDS)).isZero()
            assertThat(row(job.token.jobId)["phase"]).isNull()
        } finally { pool.shutdownNow() }
    }

    @Test fun `완료 직전 lease 만료로 거절되면 같은 트랜잭션의 결과 쓰기도 롤백한다`() {
        val job = claim()
        assertThatThrownBy {
            TransactionTemplate(transactions).execute {
                jdbc.update("INSERT INTO saved_result VALUES (?, 'retained')", job.token.jobId)
                expire(job.token.jobId)
                if (writes.complete(job.token.jobId, job.token.workerId, job.token.epoch) == 0) throw LeaseExpiredException()
            }
        }.isInstanceOf(LeaseExpiredException::class.java)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM saved_result", Int::class.java)).isZero()
        assertThat(row(job.token.jobId)["status"]).isEqualTo("PROCESSING")
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["phase", "failure", "default-completion", "explicit-completion"])
    fun `잠금 후 가드 통과 직후 lease가 만료돼도 최종 일반 UPDATE는 거절한다`(operation: String) {
        val job = claim()
        doAnswer {
            val permitted = it.callRealMethod() as Boolean
            assertThat(permitted).isTrue()
            // 메모리 확인 뒤 lease를 만료 시각 이후로 확실히 옮긴다.
            // 소유자·epoch·상태는 바꾸지 않으며 회수 절차도 부르지 않는다.
            expire(job.token.jobId)
            permitted
        }.whenever(ownership).canWrite(any(), eq(job.token))
        val written = when (operation) {
            "phase" -> writes.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)
            "failure" -> writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, "FAILED", null, "ERROR", "test")
            "default-completion" -> writes.complete(job.token.jobId, job.token.workerId, job.token.epoch)
            else -> writes.complete(job.token.jobId, job.token.workerId, job.token.epoch, JobOutcome.INDEXED)
        }
        assertThat(written).isZero()
        val retained = row(job.token.jobId)
        assertThat(retained["status"]).isEqualTo("PROCESSING")
        assertThat(retained["phase"]).isNull()
        assertThat(retained["outcome"]).isNull()
        assertThat(retained["last_error_code"]).isNull()
        assertThat(retained["worker_id"]).isEqualTo(job.token.workerId)
        assertThat(retained["execution_epoch"]).isEqualTo(job.token.epoch)
        assertThat(retained["attempt_count"]).isEqualTo(job.attemptCount)
    }

    @Test fun `잠금 없는 실행 가능 후보는 enqueue_seq 순서대로 claim한다`() {
        val ids = listOf(enqueue(2), enqueue(1), enqueue(3))
        val acquired = List(3) { checkNotNull(queue.claimFixture()) }
        assertThat(acquired.map { it.token.jobId }).containsExactlyElementsOf(ids)
        assertThat(acquired.map { row(it.token.jobId)["enqueue_seq"] as Long }).isSorted()
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["COMPLETED", "FAILED"])
    fun `선행 종결 후 후속을 획득하며 종결 Job은 다시 대기 상태로 되돌리지 않는다`(terminal: String) {
        val first = claim()
        val successor = enqueue()
        val other = enqueue(2)
        if (terminal == "COMPLETED") {
            check(writes.complete(first.token.jobId, first.token.workerId, first.token.epoch) == 1)
        } else {
            check(writes.fail(first.token.jobId, first.token.workerId, first.token.epoch, "FAILED", null, "PERMANENT", "test") == 1)
        }
        val completed = row(first.token.jobId)
        assertThat(queue.claimFixture()!!.token.jobId).isEqualTo(successor)
        assertThat(queue.claimFixture()!!.token.jobId).isEqualTo(other)
        repeat(3) { assertThat(queue.claimFixture()).isNull() }
        assertThat(row(first.token.jobId)).isEqualTo(completed)
    }

    @Test fun `claim UPDATE 이후 반환 실패도 문서와 Job 변경 전체를 롤백한다`() {
        val id = enqueue()
        val before = row(id)
        val reads = java.util.concurrent.atomic.AtomicInteger()
        doAnswer {
            if (reads.incrementAndGet() == 1) error("fail after claim UPDATE before commit")
            it.callRealMethod()
        }.whenever(locks).lockJob(id, false)
        assertThatThrownBy { queue.claimFixture() }.isInstanceOf(IllegalStateException::class.java)
        assertThat(reads.get()).isEqualTo(1)
        assertThat(row(id)).isEqualTo(before)
        assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isZero()
    }

    @Test fun `실제 claim 커밋 응답 유실은 준비 토큰을 resolve하고 추가 선점이나 attempt 증가를 하지 않는다`() {
        val id = enqueue()
        val other = enqueue(2)
        val started = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val thread = java.util.concurrent.atomic.AtomicReference<Thread>()
        val runner: IndexingPipelineRunner = mock()
        doAnswer {
            thread.set(Thread.currentThread())
            val job = it.getArgument<ClaimedJob>(0)
            assertThat(job.token.jobId).isEqualTo(id)
            assertThat(row(id)["status"]).isEqualTo("PROCESSING")
            started.countDown()
            finish.await()
            null
        }.whenever(runner).run(any<ClaimedJob>())
        val waits = java.util.concurrent.atomic.AtomicInteger()
        val database = DatabaseOperations(jdbc, com.osscontest.worker.indexing.pipeline.service.RetryWaiter {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse()
            assertThat(source.openConnections.get()).isZero()
            waits.incrementAndGet()
        }, DatabaseWaitProperties(), SimpleMeterRegistry())
        val worker = DbJobWorker(QueueProperties(concurrency = 1, pollInterval = Duration.ofDays(1)),
            database, queue, runner, ThreadSleepQueueLoopWaiter(), 5, ExecutionResults())
        source.loseNextCommit.set(true)
        worker.start()
        try {
            check(started.await(5, TimeUnit.SECONDS))
            assertThat(waits.get()).isEqualTo(1)
            assertThat(row(id)["attempt_count"]).isEqualTo(1)
            assertThat(row(id)["execution_epoch"]).isEqualTo(1L)
            assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isEqualTo(1)
            assertThat(row(other)["status"]).isEqualTo("PENDING")
            org.mockito.kotlin.verify(claims, org.mockito.kotlin.times(1)).claim(any(), any(), any(), any(), any(), any())
            org.mockito.kotlin.verify(runner, org.mockito.kotlin.times(1)).run(any<ClaimedJob>())
        } finally { worker.stop(); finish.countDown(); thread.get()?.join(5000) }
    }

    @Test fun `동시 처리 수 1에서도 RETRY_WAIT의 기한을 기다리지 않고 다른 문서를 처리한다`() {
        val first = enqueue()
        val other = enqueue(2)
        val processed = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val thread = java.util.concurrent.atomic.AtomicReference<Thread>()
        val runner: IndexingPipelineRunner = mock()
        doAnswer {
            thread.set(Thread.currentThread())
            val job = it.getArgument<ClaimedJob>(0)
            if (job.token.jobId == first) {
                check(writes.fail(first, job.token.workerId, job.token.epoch, "RETRY_WAIT",
                    queue.dbNow().plusSeconds(86400), "TEMP", "test") == 1)
            } else {
                assertThat(job.token.jobId).isEqualTo(other)
                assertThat(queue.snapshot(ExecutionToken(first, "receipt", 1))!!.retryAt).isAfter(queue.dbNow())
                processed.countDown()
                finish.await()
            }
            null
        }.whenever(runner).run(any<ClaimedJob>())
        val database = DatabaseOperations(jdbc, ThreadSleepRetryWaiter(), DatabaseWaitProperties(), SimpleMeterRegistry())
        val worker = DbJobWorker(QueueProperties(concurrency = 1, pollInterval = Duration.ofDays(1)),
            database, queue, runner, ThreadSleepQueueLoopWaiter(), 5, ExecutionResults())
        worker.start()
        try {
            check(processed.await(5, TimeUnit.SECONDS))
            assertThat(row(first)["status"]).isEqualTo("RETRY_WAIT")
            assertThat(row(first)["worker_id"]).isNull()
            assertThat(row(first)["lease_until"]).isNull()
            assertThat(row(first)["attempt_count"]).isEqualTo(1)
        } finally { worker.stop(); finish.countDown(); thread.get()?.join(5000) }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["PENDING", "RETRY_WAIT", "FAILED", "COMPLETED", "NO_LEASE"])
    fun `상태와 lease 존재 가드의 누락 케이스도 모든 일반 SQL과 heartbeat에서 무변경으로 거절한다`(condition: String) {
        val job = claim()
        if (condition == "NO_LEASE") jdbc.update("UPDATE indexing_job SET lease_until=NULL WHERE id=?", job.token.jobId)
        else jdbc.update("UPDATE indexing_job SET status=? WHERE id=?", condition, job.token.jobId)
        val before = row(job.token.jobId)
        val epoch = jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)
        assertThat(queue.renew(job.token)).isZero()
        assertThat(writes.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)).isZero()
        assertThat(writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, "FAILED", null, "ERROR", "test")).isZero()
        assertThat(writes.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isZero()
        assertThat(row(job.token.jobId)).isEqualTo(before)
        assertThat(jdbc.queryForObject("SELECT execution_epoch FROM document WHERE id=1", Long::class.java)).isEqualTo(epoch)
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["heartbeat", "failure", "completion"])
    fun `추가 Worker 경로도 문서부터 기다리고 잠금 뒤 만료 lease는 heartbeat만 갱신한다`(operation: String) {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET lease_until=clock_timestamp()+INTERVAL '2 seconds' WHERE id=?", job.token.jobId)
        val before = row(job.token.jobId)
        val pid = java.util.concurrent.atomic.AtomicInteger()
        val entered = CountDownLatch(1)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val future = withLock("SELECT id FROM document WHERE id=1 FOR UPDATE") {
                val pending = pool.submit<Int> {
                    TransactionTemplate(transactions).execute {
                        pid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Int::class.java)!!)
                        assertThat(queue.dbNow()).isBefore((before["lease_until"] as java.sql.Timestamp).toInstant())
                        entered.countDown()
                        when (operation) {
                            "heartbeat" -> queue.renew(job.token)
                            "failure" -> writes.fail(job.token.jobId, job.token.workerId, job.token.epoch, "FAILED", null, "ERROR", "test")
                            else -> writes.complete(job.token.jobId, job.token.workerId, job.token.epoch)
                        }
                    }
                }
                check(entered.await(5, TimeUnit.SECONDS))
                waitUntil { jdbc.queryForObject("SELECT cardinality(pg_blocking_pids(?)) > 0", Boolean::class.java, pid.get()) == true }
                TransactionTemplate(transactions).execute {
                    jdbc.queryForList("SELECT id FROM indexing_job WHERE id=${job.token.jobId} FOR UPDATE NOWAIT")
                }
                waitUntil { jdbc.queryForObject("SELECT lease_until <= clock_timestamp() FROM indexing_job WHERE id=?", Boolean::class.java, job.token.jobId) == true }
                pending
            }
            val written = future.get(5, TimeUnit.SECONDS)
            if (operation == "heartbeat") {
                assertThat(written).isEqualTo(1)
                assertThat(queue.snapshot(job.token)!!.leaseUntil).isAfter(queue.dbNow())
                for (column in listOf("status", "worker_id", "execution_epoch", "attempt_count")) assertThat(row(job.token.jobId)[column]).isEqualTo(before[column])
            } else {
                assertThat(written).isZero()
                assertThat(row(job.token.jobId)).isEqualTo(before)
            }
        } finally { pool.shutdownNow() }
    }
}
