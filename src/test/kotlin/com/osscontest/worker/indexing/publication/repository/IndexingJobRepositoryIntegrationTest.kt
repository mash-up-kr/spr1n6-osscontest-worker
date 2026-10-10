package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import jakarta.persistence.EntityManager
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import com.osscontest.worker.indexing.queue.JobQueueStore
import com.osscontest.worker.indexing.queue.JobType
import com.osscontest.worker.support.QueueIntegrationConfiguration
import org.springframework.context.annotation.Import
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.UUID

@Tag("integration")
@DataJpaTest
@Import(QueueIntegrationConfiguration::class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("integration")
class IndexingJobRepositoryIntegrationTest(
    private val indexingJobRepository: IndexingJobRepository,
    private val jdbcTemplate: JdbcTemplate,
    private val entityManager: EntityManager,
    private val queue: JobQueueStore,
) {
    // indexing_job.document_version_id/document_id는 document_version(id, document_id)를 참조하는
    // 복합 FK다(운영 스키마 fk_indexing_job_document_version). 테스트가 하드코딩하는
    // documentId=1L/documentVersionId=1L을 만족시키기 위해 선행 tenant/document/document_version
    // 행을 명시적 id로 시딩한다. 테스트 메서드마다 트랜잭션을 롤백하므로
    // 매 테스트 시작 시 id=1로 다시 시딩해도 충돌하지 않는다.
    @BeforeEach
    fun seedDocumentVersionFixture() {
        assertDedicatedIntegrationDatabase(jdbcTemplate)
        jdbcTemplate.update(
            "INSERT INTO tenant (id, name, created_at) VALUES (1, 'test-tenant', CURRENT_TIMESTAMP)",
        )
        jdbcTemplate.update(
            "INSERT INTO document (id, tenant_id, owner_principal_id, title) VALUES (1, 1, 'owner', 'test title')",
        )
        jdbcTemplate.update(
            """
            INSERT INTO document_version
                (id, document_id, version_no, embedding_version_no, source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
            VALUES
                (1, 1, 1, 1, 'test-object-key', ?, 'application/pdf', 100, 'test-content-hash', 'owner')
            """,
            "test.pdf".toByteArray(),
        )
    }

    @Test
    fun `큐 등록은 source event id에 대해 멱등이다`() {
        val event = UUID.randomUUID()
        queue.enqueue(event, 1, 1, JobType.INDEX, null)
        queue.enqueue(event, 1, 1, JobType.INDEX, null)
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM indexing_job WHERE source_event_id = ?", Long::class.java, event))
            .isEqualTo(1)
    }

    @Test
    fun `PROCESSING 소유자는 다른 claim이 덮어쓰지 못하고 이전 토큰은 완료할 수 없다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claim(false, 5) {})
        assertThat(queue.claim(false, 5) {}).isNull()
        assertThat(indexingJobRepository.complete(job.token.jobId, "other", job.token.epoch, "INDEXED")).isZero()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch - 1, "INDEXED")).isZero()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch, "INDEXED")).isEqualTo(1)
        entityManager.clear()
        val saved = indexingJobRepository.findById(job.token.jobId).orElseThrow()
        assertThat(saved.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(saved.workerId).isEqualTo(job.token.workerId)
        assertThat(saved.executionEpoch).isEqualTo(job.token.epoch)
    }

    @Test
    fun `회수 전에 만료 lease를 연장하면 복구 함수가 회수하지 않는다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claim(false, 5) {})
        jdbcTemplate.update("UPDATE indexing_job SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        assertThat(queue.renew(job.token)).isEqualTo(1)
        assertThat(queue.recover(5)).isZero()
        assertThat(queue.snapshot(job.token)!!.matches(job.token)).isTrue()
    }

    @Test
    fun `회수와 새 claim 이후 이전 epoch로 연장 phase 완료할 수 없다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claim(false, 5) {})
        jdbcTemplate.update("UPDATE indexing_job SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        assertThat(queue.recover(5)).isEqualTo(1)
        val next = checkNotNull(queue.claim(false, 5) {})
        assertThat(next.token.epoch).isGreaterThan(job.token.epoch)
        assertThat(queue.renew(job.token)).isZero()
        assertThat(indexingJobRepository.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)).isZero()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch, "INDEXED")).isZero()
    }

    @Test
    fun `재시도 대기 회수는 deadline을 보존하고 due 전 claim하지 않는다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claim(false, 5) {})
        jdbcTemplate.update("UPDATE indexing_job SET status = 'RETRY_WAIT', next_retry_at = clock_timestamp() + INTERVAL '1 minute', " +
            "lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val deadline = queue.snapshot(job.token)!!.retryAt
        queue.recover(5)
        assertThat(queue.snapshot(job.token)!!.retryAt).isEqualTo(deadline)
        assertThat(queue.claim(false, 5) {}).isNull()
        jdbcTemplate.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val next = checkNotNull(queue.claim(true, 5) {})
        assertThat(queue.snapshot(next.token)!!.retryAt).isNotNull()
        assertThat(queue.hasRetries()).isTrue()
    }
}
