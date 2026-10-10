package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Tag("integration")
@SpringBootTest(properties = ["indexing.queue.enabled=false"])
@ActiveProfiles("integration")
class DbJobQueueIntegrationTest(
    private val queue: JobQueueStore,
    private val jdbc: JdbcTemplate,
    private val transactions: PlatformTransactionManager,
    private val publication: IndexingPublicationService,
    private val deletion: DocumentDeletionService,
    private val failures: IndexingFailureService,
) {
    @BeforeEach
    fun fixture() {
        assertDedicatedIntegrationDatabase(jdbc)
        jdbc.update("INSERT INTO tenant (id, name) VALUES (800001, 'queue-integration')")
        for (id in 800001L..800002L) {
            jdbc.update("INSERT INTO document (id, tenant_id, owner_principal_id, title) VALUES (?, 800001, 'test', 'queue')", id)
            jdbc.update("""
                INSERT INTO document_version (id, document_id, version_no, embedding_version_no,
                    source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
                VALUES (?, ?, 1, 1, 'test', ?, 'text/plain', 1, 'hash', 'test')
            """.trimIndent(), id, id, "test.txt".toByteArray())
        }
    }

    @AfterEach
    fun cleanup() {
        jdbc.update("DELETE FROM indexing_job WHERE document_id IN (800001,800002)")
        jdbc.update("DELETE FROM document_chunk WHERE document_id IN (800001,800002)")
        jdbc.update("DELETE FROM outbox_event WHERE document_id IN (800001,800002)")
        jdbc.update("UPDATE document SET searchable_version_id = NULL WHERE id IN (800001,800002)")
        jdbc.update("DELETE FROM document_version WHERE document_id IN (800001,800002)")
        jdbc.update("DELETE FROM document WHERE id IN (800001,800002)")
        jdbc.update("DELETE FROM tenant WHERE id = 800001")
    }

    private fun claim(): ClaimedJob {
        queue.enqueue(UUID.randomUUID(), 800001, 800001, JobType.INDEX, null)
        return checkNotNull(queue.claim(5) {})
    }

    private fun expire(job: ClaimedJob) {
        jdbc.update("UPDATE indexing_job SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
    }

    @Test
    fun `연장이 행 잠금을 먼저 얻으면 대기 중 회수의 만료 조건이 다시 평가된다`() {
        race(renewFirst = true)
    }

    @Test
    fun `회수가 행 잠금을 먼저 얻으면 대기 중 이전 토큰 연장은 거절된다`() {
        race(renewFirst = false)
    }

    private fun race(renewFirst: Boolean) {
        val job = claim()
        expire(job)
        val locked = CountDownLatch(1)
        val commit = CountDownLatch(1)
        val competing = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val winner = pool.submit {
                TransactionTemplate(transactions).execute {
                    if (renewFirst) queue.renew(job.token) else queue.recover(5, Duration.ofSeconds(30))
                    locked.countDown()
                    check(commit.await(5, TimeUnit.SECONDS))
                }
            }
            check(locked.await(5, TimeUnit.SECONDS))
            val loser = pool.submit<Long> {
                competing.countDown()
                if (renewFirst) queue.recover(5, Duration.ofSeconds(30)) else queue.renew(job.token).toLong()
            }
            check(competing.await(5, TimeUnit.SECONDS))
            assertThatThrownBy { loser.get(100, TimeUnit.MILLISECONDS) }.isInstanceOf(TimeoutException::class.java)
            commit.countDown()
            winner.get(5, TimeUnit.SECONDS)
            assertThat(loser.get(5, TimeUnit.SECONDS)).isZero()
            assertThat(queue.snapshot(job.token)!!.matches(job.token)).isEqualTo(renewFirst)
        } finally {
            commit.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `청크 UPSERT 후 완료 실패는 청크 버전 Job을 모두 롤백한다`() {
        val job = claim()
        // Duplicate chunk numbers pass UPSERT but fail stored-count validation after writing.
        assertThatThrownBy { publication.publish(context(job), listOf(chunk(0), chunk(0))) }
            .isInstanceOf(IllegalStateException::class.java)
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM document_chunk WHERE document_id = 800001", Long::class.java)).isZero()
        assertThat(queue.snapshot(job.token)!!.status).isEqualTo(IndexingJobStatus.PROCESSING)
        assertThat(jdbc.queryForObject("SELECT indexed_at FROM document_version WHERE id = 800001", LocalDateTime::class.java)).isNull()
    }

    @Test
    fun `실제 커밋 후 결과 저장 재호출은 완료 시각과 청크를 다시 쓰지 않는다`() {
        val job = claim()
        publication.publish(context(job), listOf(chunk(0)))
        val completed = jdbc.queryForObject("SELECT completed_at FROM indexing_job WHERE id = ?", java.sql.Timestamp::class.java, job.token.jobId)
        // The caller did not observe the previous return value; replay with changed inputs.
        publication.publish(context(job), listOf(chunk(0).copy(content = "must not overwrite")))
        assertThat(jdbc.queryForObject("SELECT content FROM document_chunk WHERE document_id = 800001", String::class.java)).isEqualTo("saved")
        assertThat(jdbc.queryForObject("SELECT completed_at FROM indexing_job WHERE id = ?", java.sql.Timestamp::class.java, job.token.jobId)).isEqualTo(completed)
    }

    @Test
    fun `재시도 대기 중 다른 문서는 claim되고 같은 Job은 due 이후 새 실행을 받는다`() {
        val job = claim()
        queue.enqueue(UUID.randomUUID(), 800002, 800002, JobType.INDEX, null)
        jdbc.update("UPDATE indexing_job SET status = 'RETRY_WAIT', worker_id = NULL, lease_until = NULL, " +
            "next_retry_at = clock_timestamp() + INTERVAL '1 minute' WHERE id = ?", job.token.jobId)
        val other = checkNotNull(queue.claim(5) {})
        assertThat(other.documentId).isEqualTo(800002)
        assertThat(queue.claim(5) {}).isNull()
        jdbc.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val retry = checkNotNull(queue.claim(5) {})
        assertThat(retry.token.jobId).isEqualTo(job.token.jobId)
        assertThat(retry.sourceEventId).isEqualTo(job.sourceEventId)
        assertThat(retry.token.epoch).isEqualTo(job.token.epoch + 1)
        assertThat(queue.snapshot(retry.token)!!.retryAt).isNull()
    }

    @Test
    fun `delete Job 완료 후 이전 인덱싱 토큰이 청크를 다시 만들지 못한다`() {
        val old = claim()
        publication.publish(context(old), listOf(chunk(0)))
        jdbc.update("UPDATE document SET deleted_at = clock_timestamp() WHERE id = 800001")
        queue.enqueue(UUID.randomUUID(), 800001, null, JobType.DELETE, null)
        val job = checkNotNull(queue.claim(5) {})
        deletion.delete(job)
        deletion.delete(job)
        publication.publish(context(old), listOf(chunk(0))) // old completion is acknowledged without writes
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM document_chunk WHERE document_id = 800001", Long::class.java)).isZero()
        assertThat(queue.snapshot(job.token)!!.status).isEqualTo(IndexingJobStatus.COMPLETED)
    }

    @Test
    fun `실제 실패 커밋 재호출은 해제 상태와 같은 식별자 순서 횟수 deadline을 보존한다`() {
        val job = claim()
        val original = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)
        assertThat(failures.recordFailure(job.token, "TEMP", "temporary", false, 5, Duration.ofSeconds(30)))
            .isEqualTo(IndexingJobStatus.RETRY_WAIT)
        val committed = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)
        assertThat(committed["worker_id"]).isNull()
        assertThat(committed["lease_until"]).isNull()
        for (column in listOf("source_event_id", "enqueue_seq", "attempt_count", "execution_epoch")) {
            assertThat(committed[column]).isEqualTo(original[column])
        }
        failures.recordFailure(job.token, "OTHER", "replayed", false, 5, Duration.ofSeconds(60))
        val replayed = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)
        assertThat(replayed).isEqualTo(committed)
        assertThat(queue.claim(5) {}).isNull()
        jdbc.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val next = checkNotNull(queue.claim(5) {})
        assertThat(next.sourceEventId).isEqualTo(job.sourceEventId)
        assertThat(next.attemptCount).isEqualTo(job.attemptCount + 1)
        assertThat(next.token.epoch).isEqualTo(job.token.epoch + 1)
        assertThat(jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", next.token.jobId)["enqueue_seq"])
            .isEqualTo(original["enqueue_seq"])
    }

    @Test
    fun `다른 Worker 오래된 epoch 만료 lease는 실패 상태를 반영하지 못한다`() {
        val job = claim()
        for (token in listOf(job.token.copy(workerId = "other"), job.token.copy(epoch = job.token.epoch - 1))) {
            assertThatThrownBy { failures.recordFailure(token, "TEMP", "error", false, 5, Duration.ofSeconds(30)) }
                .isInstanceOf(ExecutionLostException::class.java)
        }
        expire(job)
        assertThatThrownBy { failures.recordFailure(job.token, "TEMP", "error", false, 5, Duration.ofSeconds(30)) }
            .isInstanceOf(LeaseExpiredException::class.java)
        assertThat(queue.snapshot(job.token)!!.status).isEqualTo(IndexingJobStatus.PROCESSING)
    }

    @Test
    fun `만료 복구도 선형 지터를 적용하고 실패 반영에서 횟수와 epoch를 소비하지 않는다`() {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET attempt_count = 3 WHERE id = ?", job.token.jobId)
        expire(job)
        val before = queue.dbNow()
        assertThat(queue.recover(5, Duration.ofSeconds(30))).isEqualTo(1)
        val after = queue.dbNow()
        val recovered = checkNotNull(queue.snapshot(job.token))
        assertThat(recovered.status).isEqualTo(IndexingJobStatus.RETRY_WAIT)
        assertThat(recovered.retryAt).isBetween(before.plusSeconds(72), after.plusSeconds(108))
        assertThat(recovered.epoch).isEqualTo(job.token.epoch)
        assertThat(recovered.workerId).isNull()
        assertThat(recovered.leaseUntil).isNull()
        assertThat(queue.claim(5) {}).isNull()
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM indexing_job WHERE id = ?", Int::class.java, job.token.jobId))
            .isEqualTo(3)
    }

    @Test
    fun `마지막 실행 lease 만료는 FAILED로 종결하고 재시도를 예약하지 않는다`() {
        val job = claim()
        jdbc.update("UPDATE indexing_job SET attempt_count = 5 WHERE id = ?", job.token.jobId)
        expire(job)
        queue.recover(5, Duration.ofSeconds(30))
        val recovered = checkNotNull(queue.snapshot(job.token))
        assertThat(recovered.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(recovered.retryAt).isNull()
        assertThat(recovered.epoch).isEqualTo(job.token.epoch)
        assertThat(recovered.workerId).isNull()
        assertThat(recovered.leaseUntil).isNull()
        assertThat(queue.claim(5) {}).isNull()
    }

    private fun context(job: ClaimedJob) = IndexingContext(job.token.jobId, 800001, 800001, 1, null, job.token)
    private fun chunk(number: Int) = DocumentChunk(800001, 800001, number, "saved", "saved", "hash", 1,
        null, null, null, null, FloatArray(1536) { 0.1f }, LocalDateTime.now())
}
