package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import jakarta.persistence.EntityManager
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime
import java.util.UUID

@Tag("integration")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("local")
class IndexingJobRepositoryIntegrationTest(
    private val indexingJobRepository: IndexingJobRepository,
    private val jdbcTemplate: JdbcTemplate,
    private val entityManager: EntityManager,
) {
    // indexing_job.document_version_id/document_id는 document_version(id, document_id)를 참조하는
    // 복합 FK다(운영 스키마 fk_indexing_job_document_version). 테스트가 하드코딩하는
    // documentId=1L/documentVersionId=1L을 만족시키기 위해 선행 tenant/document/document_version
    // 행을 명시적 id로 시딩한다. @DataJpaTest는 테스트 메서드마다 트랜잭션을 롤백하므로
    // 매 테스트 시작 시 id=1로 다시 시딩해도 충돌하지 않는다.
    @BeforeEach
    fun seedDocumentVersionFixture() {
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
    fun `insertIfAbsent는 같은 sourceEventId로 두 번 호출해도 행이 하나만 생긴다`() {
        val eventId = UUID.randomUUID()
        val firstInsert = indexingJobRepository.insertIfAbsent(eventId, 1L, 1L, null)
        val secondInsert = indexingJobRepository.insertIfAbsent(eventId, 1L, 1L, null)

        // 두 번째 호출은 uk_indexing_job_source_event에 걸려 ON CONFLICT DO NOTHING으로
        // 조용히 스킵되므로 영향받은 행 수가 0이어야 한다 — 새 행이 추가되지 않았다는 직접 증거다.
        assertThat(firstInsert).isEqualTo(1)
        assertThat(secondInsert).isZero()

        // findBySourceEventId는 Spring Data의 단일 결과 파생 쿼리라, 동일 sourceEventId로
        // 행이 두 개 이상 존재하면 예외(IncorrectResultSizeDataAccessException)를 던진다.
        // 즉 여기서 예외 없이 non-null 값을 돌려받는 것 자체가 "이 sourceEventId에 대해
        // 정확히 한 행만 존재한다"는 증거이며, 공유 DB의 다른 테스트가 만든 행에 영향받지
        // 않도록 테이블 전체가 아닌 이 eventId 하나로 범위를 좁힌 검증이다.
        val job = indexingJobRepository.findBySourceEventId(eventId)
        assertThat(job).isNotNull
        assertThat(job!!.sourceEventId).isEqualTo(eventId)
    }

    @Test
    fun `start는 attempt_count가 상한 이상이면 재획득하지 않는다`() {
        val job =
            indexingJobRepository.save(
                IndexingJobEntity(
                    sourceEventId = UUID.randomUUID(),
                    documentId = 1L,
                    documentVersionId = 1L,
                    status = IndexingJobStatus.PROCESSING,
                    attemptCount = 5,
                    nextRetryAt = null,
                    workerId = null,
                    lastErrorCode = null,
                    lastErrorMessage = null,
                    traceId = null,
                    startedAt = null,
                    completedAt = null,
                    updatedAt = LocalDateTime.now(),
                ),
            )

        val updated = indexingJobRepository.start(job.id!!, "worker-2", maxAttempts = 5)

        assertThat(updated).isZero()
    }

    @Test
    fun `start는 PROCESSING 상태도 재획득 가능하다`() {
        val job =
            indexingJobRepository.save(
                IndexingJobEntity(
                    sourceEventId = UUID.randomUUID(),
                    documentId = 1L,
                    documentVersionId = 1L,
                    status = IndexingJobStatus.PROCESSING,
                    attemptCount = 1,
                    nextRetryAt = null,
                    workerId = "worker-1",
                    lastErrorCode = null,
                    lastErrorMessage = null,
                    traceId = null,
                    startedAt = LocalDateTime.now(),
                    completedAt = null,
                    updatedAt = LocalDateTime.now(),
                ),
            )

        val updated = indexingJobRepository.start(job.id!!, "worker-2", maxAttempts = 5)

        assertThat(updated).isEqualTo(1)
    }

    @Test
    fun `complete는 PROCESSING 작업을 COMPLETED로 전이시킨다`() {
        val job =
            indexingJobRepository.save(
                IndexingJobEntity(
                    sourceEventId = UUID.randomUUID(),
                    documentId = 1L,
                    documentVersionId = 1L,
                    status = IndexingJobStatus.PROCESSING,
                    attemptCount = 1,
                    nextRetryAt = null,
                    workerId = "worker-1",
                    lastErrorCode = null,
                    lastErrorMessage = null,
                    traceId = null,
                    startedAt = LocalDateTime.now(),
                    completedAt = null,
                    updatedAt = LocalDateTime.now(),
                ),
            )

        val updated = indexingJobRepository.complete(job.id!!)

        assertThat(updated).isEqualTo(1)
        // complete()는 native @Modifying UPDATE라 영속성 컨텍스트를 거치지 않는다.
        // clear() 없이 findById를 호출하면 1차 캐시에 남은 위 save() 결과(수정 전 상태)가
        // 그대로 반환되어 DB의 실제 변경 사항을 반영하지 못한다.
        entityManager.clear()
        val reloaded = indexingJobRepository.findById(job.id!!).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(reloaded.completedAt).isNotNull
        assertThat(reloaded.nextRetryAt).isNull()
    }

    @Test
    fun `failIfAttemptsExceeded는 상한 도달 시 FAILED로 종결한다`() {
        val job =
            indexingJobRepository.save(
                IndexingJobEntity(
                    sourceEventId = UUID.randomUUID(),
                    documentId = 1L,
                    documentVersionId = 1L,
                    status = IndexingJobStatus.PROCESSING,
                    attemptCount = 5,
                    nextRetryAt = null,
                    workerId = "worker-1",
                    lastErrorCode = null,
                    lastErrorMessage = null,
                    traceId = null,
                    startedAt = LocalDateTime.now(),
                    completedAt = null,
                    updatedAt = LocalDateTime.now(),
                ),
            )

        val updated = indexingJobRepository.failIfAttemptsExceeded(job.id!!, maxAttempts = 5)

        assertThat(updated).isEqualTo(1)
        entityManager.clear()
        val reloaded = indexingJobRepository.findById(job.id!!).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(reloaded.lastErrorCode).isEqualTo("MAX_ATTEMPTS_EXCEEDED")
    }

    @Test
    fun `failIfAttemptsExceeded는 상한 미도달이면 아무 것도 하지 않는다`() {
        val job =
            indexingJobRepository.save(
                IndexingJobEntity(
                    sourceEventId = UUID.randomUUID(),
                    documentId = 1L,
                    documentVersionId = 1L,
                    status = IndexingJobStatus.PROCESSING,
                    attemptCount = 2,
                    nextRetryAt = null,
                    workerId = "worker-1",
                    lastErrorCode = null,
                    lastErrorMessage = null,
                    traceId = null,
                    startedAt = LocalDateTime.now(),
                    completedAt = null,
                    updatedAt = LocalDateTime.now(),
                ),
            )

        val updated = indexingJobRepository.failIfAttemptsExceeded(job.id!!, maxAttempts = 5)

        assertThat(updated).isZero()
        val reloaded = indexingJobRepository.findById(job.id!!).orElseThrow()
        assertThat(reloaded.status).isEqualTo(IndexingJobStatus.PROCESSING)
    }

    @Test
    fun `failActiveJobsForDocument는 활성 Job만 FAILED로 종결하고 완료된 Job은 건드리지 않는다`() {
        // 활성 Job(PENDING/PROCESSING/RETRY_WAIT)마다 서로 다른 document_version_id가 필요하다 —
        // uq_indexing_job_active_version이 동일 document_version_id에 활성 Job을 하나만 허용한다.
        // @BeforeEach가 이미 document_version(id=1, document_id=1, version_no=1)을 시딩해뒀으므로
        // 나머지 2~6은 여기서 추가로 시딩한다.
        for (versionNo in 2..6) {
            jdbcTemplate.update(
                """
                INSERT INTO document_version
                    (id, document_id, version_no, embedding_version_no, source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
                VALUES
                    (?, 1, ?, ?, ?, ?, 'application/pdf', 100, ?, 'owner')
                """.trimIndent(),
                versionNo.toLong(),
                versionNo,
                versionNo,
                "test-object-key-$versionNo",
                "test.pdf".toByteArray(),
                "test-content-hash-$versionNo",
            )
        }

        fun newJob(
            documentVersionId: Long,
            status: IndexingJobStatus,
            lastErrorCode: String? = null,
        ) = indexingJobRepository.save(
            IndexingJobEntity(
                sourceEventId = UUID.randomUUID(),
                documentId = 1L,
                documentVersionId = documentVersionId,
                status = status,
                attemptCount = 1,
                nextRetryAt = if (status == IndexingJobStatus.RETRY_WAIT) LocalDateTime.now().plusMinutes(5) else null,
                workerId = "worker-1",
                lastErrorCode = lastErrorCode,
                lastErrorMessage = if (lastErrorCode != null) "pre-existing failure" else null,
                traceId = null,
                startedAt = LocalDateTime.now(),
                completedAt =
                    if (status == IndexingJobStatus.COMPLETED || status == IndexingJobStatus.FAILED) {
                        LocalDateTime.now()
                    } else {
                        null
                    },
                updatedAt = LocalDateTime.now(),
            ),
        )

        val pendingJob = newJob(2L, IndexingJobStatus.PENDING)
        val processingJob = newJob(3L, IndexingJobStatus.PROCESSING)
        val retryWaitJob = newJob(4L, IndexingJobStatus.RETRY_WAIT)
        val completedJob = newJob(5L, IndexingJobStatus.COMPLETED)
        val failedJob = newJob(6L, IndexingJobStatus.FAILED, lastErrorCode = "SOME_OTHER_ERROR")

        val updated = indexingJobRepository.failActiveJobsForDocument(1L)

        assertThat(updated).isEqualTo(3)
        entityManager.clear()

        listOf(pendingJob, processingJob, retryWaitJob).forEach { job ->
            val reloaded = indexingJobRepository.findById(job.id!!).orElseThrow()
            assertThat(reloaded.status).isEqualTo(IndexingJobStatus.FAILED)
            assertThat(reloaded.lastErrorCode).isEqualTo("DOCUMENT_DELETED")
            assertThat(reloaded.nextRetryAt).isNull()
            assertThat(reloaded.completedAt).isNotNull
        }

        val reloadedCompleted = indexingJobRepository.findById(completedJob.id!!).orElseThrow()
        assertThat(reloadedCompleted.status).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(reloadedCompleted.lastErrorCode).isNull()

        val reloadedFailed = indexingJobRepository.findById(failedJob.id!!).orElseThrow()
        assertThat(reloadedFailed.status).isEqualTo(IndexingJobStatus.FAILED)
        assertThat(reloadedFailed.lastErrorCode).isEqualTo("SOME_OTHER_ERROR")
    }
}
