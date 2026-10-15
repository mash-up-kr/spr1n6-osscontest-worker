/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.support.claimFixture
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import com.osscontest.worker.support.seedReleasedExecution
import jakarta.persistence.EntityManager
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.dao.InvalidDataAccessApiUsageException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import com.osscontest.worker.indexing.queue.JobQueueStore
import com.osscontest.worker.indexing.queue.JobType
import com.osscontest.worker.support.QueueIntegrationConfiguration
import org.springframework.context.annotation.Import
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
        // 이 클래스는 enqueue와 완료 연산을 검증하므로 Job을 테스트에서 직접 등록한다.
        jdbcTemplate.update("DELETE FROM indexing_job WHERE document_id = 1")
    }

    @Test
    fun `DELETE 공통 완료는 DELETED와 owner lease 해제를 함께 저장한다`() {
        queue.enqueue(UUID.randomUUID(), 1, null, JobType.DELETE, null)
        val job = checkNotNull(queue.claimFixture(5) {})
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isEqualTo(1)
        val snapshot = checkNotNull(queue.snapshot(job.token))
        assertThat(snapshot.completed(job.token)).isTrue()
        assertThat(snapshot.outcome).isEqualTo("DELETED")
        assertThat(snapshot.workerId).isNull()
        assertThat(snapshot.leaseUntil).isNull()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isZero()
    }

    @Test
    fun `INDEX skip만 허용하며 DELETE를 skip outcome으로 기록하지 않는다`() {
        queue.enqueue(UUID.randomUUID(), 1, null, JobType.DELETE, null)
        val job = checkNotNull(queue.claimFixture(5) {})
        org.assertj.core.api.Assertions.assertThatThrownBy {
            indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch,
                com.osscontest.worker.indexing.queue.JobOutcome.SKIPPED_DOCUMENT_DELETED)
        }.isInstanceOf(InvalidDataAccessApiUsageException::class.java)
            // @Repository 예외 변환이 require 실패를 감싼다.
            .hasCauseInstanceOf(IllegalArgumentException::class.java)
        assertThat(queue.snapshot(job.token)!!.status).isEqualTo(IndexingJobStatus.PROCESSING)
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
        val job = checkNotNull(queue.claimFixture(5) {})
        assertThat(queue.claimFixture(5) {}).isNull()
        assertThat(indexingJobRepository.complete(job.token.jobId, "other", job.token.epoch)).isZero()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch - 1)).isZero()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isEqualTo(1)
        entityManager.clear()
        val saved = indexingJobRepository.findById(job.token.jobId).orElseThrow()
        assertThat(saved.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(saved.workerId).isNull()
        assertThat(saved.leaseUntil).isNull()
        assertThat(saved.outcome).isEqualTo("INDEXED")
        assertThat(saved.executionEpoch).isEqualTo(job.token.epoch)
    }

    @Test
    fun `미회수 동일 실행의 만료 lease는 횟수와 epoch 변경 없이 갱신한다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claimFixture(5) {})
        jdbcTemplate.update("UPDATE indexing_job SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        assertThat(queue.renew(job.token)).isEqualTo(1)
        val snapshot = checkNotNull(queue.snapshot(job.token))
        assertThat(snapshot.ownsProcessing(job.token)).isTrue()
        assertThat(snapshot.leaseUntil).isAfter(snapshot.now)
        assertThat(snapshot.epoch).isEqualTo(job.token.epoch)
        assertThat(jdbcTemplate.queryForObject("SELECT attempt_count FROM indexing_job WHERE id = ?",
            Int::class.java, job.token.jobId)).isEqualTo(job.attemptCount)
    }

    @Test
    fun `회수와 새 claim 이후 이전 epoch로 연장 phase 완료할 수 없다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claimFixture(5) {})
        seedReleasedExecution(jdbcTemplate, job.token, IndexingJobStatus.RETRY_WAIT, queue.dbNow().plusSeconds(60))
        assertThat(queue.claimFixture(5) {}).isNull()
        jdbcTemplate.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val next = checkNotNull(queue.claimFixture(5) {})
        assertThat(next.token.epoch).isGreaterThan(job.token.epoch)
        assertThat(queue.renew(job.token)).isZero()
        assertThat(indexingJobRepository.updatePhase(job.token.jobId, "PARSING", job.token.workerId, job.token.epoch)).isZero()
        assertThat(indexingJobRepository.complete(job.token.jobId, job.token.workerId, job.token.epoch)).isZero()
    }

    @Test
    fun `재시도 대기는 due 전 claim하지 않고 deadline을 보존한다`() {
        queue.enqueue(UUID.randomUUID(), 1, 1, JobType.INDEX, null)
        val job = checkNotNull(queue.claimFixture(5) {})
        seedReleasedExecution(jdbcTemplate, job.token, IndexingJobStatus.RETRY_WAIT, queue.dbNow().plusSeconds(60))
        val deadline = queue.snapshot(job.token)!!.retryAt
        assertThat(queue.claimFixture(5) {}).isNull()
        assertThat(queue.snapshot(job.token)!!.retryAt).isEqualTo(deadline)
        jdbcTemplate.update("UPDATE indexing_job SET next_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", job.token.jobId)
        val next = checkNotNull(queue.claimFixture(5) {})
        assertThat(queue.snapshot(next.token)!!.retryAt).isNull()
    }
}
