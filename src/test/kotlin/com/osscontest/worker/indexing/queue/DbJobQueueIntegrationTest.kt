package com.osscontest.worker.indexing.queue

import com.osscontest.worker.support.claimFixture
import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import com.osscontest.worker.support.seedReleasedExecution
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.mockito.kotlin.whenever
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.ActiveProfiles
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID

@Tag("integration")
@SpringBootTest(properties = ["indexing.queue.enabled=false"])
@ActiveProfiles("integration")
class DbJobQueueIntegrationTest(
    private val queue: JobQueueStore,
    private val jdbc: JdbcTemplate,
    private val publication: IndexingPublicationService,
    private val deletion: DocumentDeletionService,
    private val failures: IndexingFailureService,
    private val execution: ExecutionOperations,
) {
    @MockitoBean private lateinit var breaker: CircuitBreaker

    @BeforeEach
    fun fixture() {
        whenever(breaker.admission()).thenReturn(ClaimAdmission.NORMAL)
        whenever(breaker.acquire()).thenReturn(java.time.Duration.ZERO)
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
        return checkNotNull(queue.claimFixture(5) {})
    }

    private fun expire(job: ClaimedJob) {
        jdbc.update("UPDATE indexing_job SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
    }

    // Recovery procedure scheduling, jitter and heartbeat/recovery lock races belong to the DB owner.
    // Worker tests prepare released state and verify active Worker APIs, without calling that procedure.
    @Test
    fun `외부 회수 뒤 재claim 전 이전 실행은 heartbeat와 publish 결과를 쓰지 못한다`() {
        val job = claim()
        seedReleasedExecution(jdbc, job.token, IndexingJobStatus.RETRY_WAIT, queue.dbNow().plusSeconds(60))
        val before = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)
        assertThat(queue.renew(job.token)).isZero()
        assertThatThrownBy { publication.publish(context(job), listOf(chunk(0))) }
            .isInstanceOf(ExecutionLostException::class.java)
        assertThat(jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)).isEqualTo(before)
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM document_chunk WHERE document_id = 800001", Long::class.java)).isZero()
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
        val other = checkNotNull(queue.claimFixture(5) {})
        assertThat(other.documentId).isEqualTo(800002)
        assertThat(queue.claimFixture(5) {}).isNull()
        jdbc.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val retry = checkNotNull(queue.claimFixture(5) {})
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
        val job = checkNotNull(queue.claimFixture(5) {})
        execution.completion(job.token, ExecutionIdentity(job), completed = {}) { deletion.delete(job); Unit }
        execution.completion(job.token, ExecutionIdentity(job), completed = {}) { error("Completed DELETE must not replay") }
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
        assertThat(queue.claimFixture(5) {}).isNull()
        jdbc.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val next = checkNotNull(queue.claimFixture(5) {})
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
    fun `외부 회수로 종결된 FAILED는 claim되지 않고 이전 heartbeat도 거절된다`() {
        val job = claim()
        seedReleasedExecution(jdbc, job.token, IndexingJobStatus.FAILED)
        val before = jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)
        assertThat(queue.claimFixture(5) {}).isNull()
        assertThat(queue.renew(job.token)).isZero()
        assertThat(jdbc.queryForMap("SELECT * FROM indexing_job WHERE id = ?", job.token.jobId)).isEqualTo(before)
    }

    private fun context(job: ClaimedJob) = IndexingContext(job.token.jobId, 800001, 800001, 1, null, job.token)
    private fun chunk(number: Int) = DocumentChunk(800001, 800001, number, "saved", "saved", "hash", 1,
        null, null, null, null, FloatArray(1536) { 0.1f }, LocalDateTime.now())
}
