package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
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
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Tag("integration")
@SpringBootTest(properties = ["indexing.queue.enabled=false", "spring.kafka.listener.auto-startup=false"])
@ActiveProfiles("integration")
class DbJobQueueIntegrationTest(
    private val queue: JobQueueStore,
    private val jdbc: JdbcTemplate,
    private val transactions: PlatformTransactionManager,
    private val publication: IndexingPublicationService,
    private val deletion: DocumentDeletionService,
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
        return checkNotNull(queue.claim(false, 5) {})
    }

    private fun expire(job: ClaimedJob) {
        jdbc.update("UPDATE indexing_job SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
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
                    if (renewFirst) queue.renew(job.token) else queue.recover(5)
                    locked.countDown()
                    check(commit.await(5, TimeUnit.SECONDS))
                }
            }
            check(locked.await(5, TimeUnit.SECONDS))
            val loser = pool.submit<Long> {
                competing.countDown()
                if (renewFirst) queue.recover(5) else queue.renew(job.token).toLong()
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
    fun `retry PROCESSING이 완료될 때까지 새 Job은 claim되지 않는다`() {
        val job = claim()
        queue.enqueue(UUID.randomUUID(), 800002, 800002, JobType.INDEX, null)
        jdbc.update("UPDATE indexing_job SET status = 'RETRY_WAIT', next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        assertThat(queue.claim(false, 5) {}).isNull()
        assertThat(queue.resumeRetry(job.token, 5)).isEqualTo(1)
        assertThat(queue.claim(false, 5) {}).isNull()
        publication.publish(context(job), listOf(chunk(0)))
        assertThat(queue.claim(false, 5) {}!!.documentId).isEqualTo(800002)
    }

    @Test
    fun `delete Job 완료 후 이전 인덱싱 토큰이 청크를 다시 만들지 못한다`() {
        val old = claim()
        publication.publish(context(old), listOf(chunk(0)))
        jdbc.update("UPDATE document SET deleted_at = clock_timestamp() WHERE id = 800001")
        queue.enqueue(UUID.randomUUID(), 800001, null, JobType.DELETE, null)
        val job = checkNotNull(queue.claim(false, 5) {})
        deletion.delete(job)
        deletion.delete(job)
        publication.publish(context(old), listOf(chunk(0))) // old completion is acknowledged without writes
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM document_chunk WHERE document_id = 800001", Long::class.java)).isZero()
        assertThat(queue.snapshot(job.token)!!.status).isEqualTo(IndexingJobStatus.COMPLETED)
    }

    private fun context(job: ClaimedJob) = IndexingContext(job.token.jobId, 800001, 800001, 1, null, job.token)
    private fun chunk(number: Int) = DocumentChunk(800001, 800001, number, "saved", "saved", "hash", 1,
        null, null, null, null, FloatArray(1536) { 0.1f }, LocalDateTime.now())
}
