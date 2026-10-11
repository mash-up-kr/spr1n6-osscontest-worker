package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.JobOutcome
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.service.IndexingPublicationService
import com.osscontest.worker.support.claimFixture
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.mockito.kotlin.whenever
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDateTime
import java.util.UUID

@Tag("integration")
@SpringBootTest(properties = ["indexing.queue.enabled=false"])
@ActiveProfiles("integration")
class DeletedDocumentIntegrationTest(
    private val queue: JobQueueStore,
    private val jdbc: JdbcTemplate,
    private val publication: IndexingPublicationService,
    private val deletion: DocumentDeletionService,
) {
    private val tenantId = 900001L
    private val documentId = 900001L
    private val oldVersionId = 900001L
    private val newVersionId = 900002L

    @MockitoBean private lateinit var breaker: CircuitBreaker

    @BeforeEach
    fun fixture() {
        whenever(breaker.admission()).thenReturn(ClaimAdmission.NORMAL)
        whenever(breaker.acquire()).thenReturn(java.time.Duration.ZERO)
        assertDedicatedIntegrationDatabase(jdbc)
        cleanup()
        jdbc.update("INSERT INTO tenant (id, name) VALUES (?, 'deleted-document')", tenantId)
        // latest_upload_version_no를 2로 두어 검색 버전 승격 시 ck_document_embedding_not_ahead_of_upload를 지킨다.
        jdbc.update(
            "INSERT INTO document (id, tenant_id, owner_principal_id, title, latest_upload_version_no) VALUES (?, ?, 'test', 'deleted', 2)",
            documentId, tenantId,
        )
        for ((versionId, versionNo) in listOf(oldVersionId to 1, newVersionId to 2)) {
            jdbc.update(
                """
                INSERT INTO document_version (id, document_id, version_no, embedding_version_no,
                    source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
                VALUES (?, ?, ?, ?, 'test', ?, 'text/plain', 1, 'hash', 'test')
                """.trimIndent(),
                versionId, documentId, versionNo, versionNo, "test.txt".toByteArray(),
            )
        }
    }

    @AfterEach
    fun cleanup() {
        jdbc.update("DELETE FROM indexing_job WHERE document_id = ?", documentId)
        jdbc.update("DELETE FROM document_chunk WHERE document_id = ?", documentId)
        jdbc.update("DELETE FROM outbox_event WHERE document_id = ?", documentId)
        jdbc.update("UPDATE document SET searchable_version_id = NULL WHERE id = ?", documentId)
        jdbc.update("DELETE FROM document_version WHERE document_id = ?", documentId)
        jdbc.update("DELETE FROM document WHERE id = ?", documentId)
        jdbc.update("DELETE FROM tenant WHERE id = ?", tenantId)
    }

    private fun claimIndex(versionId: Long = oldVersionId): ClaimedJob {
        queue.enqueue(UUID.randomUUID(), documentId, versionId, JobType.INDEX, null)
        return checkNotNull(queue.claimFixture(5) {})
    }

    private fun enqueueIndexWithoutClaim(versionId: Long = oldVersionId): Long {
        queue.enqueue(UUID.randomUUID(), documentId, versionId, JobType.INDEX, null)
        return jdbc.queryForObject("SELECT MAX(id) FROM indexing_job WHERE document_id = ?", Long::class.java, documentId)!!
    }

    private fun deleteDocument() {
        jdbc.update("UPDATE document SET deleted_at = clock_timestamp() WHERE id = ?", documentId)
    }

    private fun row(jobId: Long): Map<String, Any?> =
        jdbc.queryForMap("SELECT status, outcome, last_error_code FROM indexing_job WHERE id = ?", jobId)

    private fun completedAt(jobId: Long) =
        jdbc.queryForObject("SELECT completed_at FROM indexing_job WHERE id = ?", java.sql.Timestamp::class.java, jobId)

    private fun chunkCount() =
        jdbc.queryForObject("SELECT COUNT(*) FROM document_chunk WHERE document_id = ?", Long::class.java, documentId)

    private fun context(job: ClaimedJob) = IndexingContext(job.token.jobId, documentId, oldVersionId, 1, null, job.token)

    private fun chunk(number: Int) = DocumentChunk(oldVersionId, documentId, number, "saved", "saved", "hash", 1,
        null, null, null, null, FloatArray(1536) { 0.1f }, LocalDateTime.now())

    @Test
    fun `삭제된 문서의 INDEX Job은 SKIPPED로 완료되고 재호출해도 다시 쓰지 않는다`() {
        val job = claimIndex()
        deleteDocument()

        assertThat(publication.preflight(job)).isTrue()
        assertThat(row(job.token.jobId)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job.token.jobId)["outcome"]).isEqualTo(JobOutcome.SKIPPED_DOCUMENT_DELETED.name)
        val completed = completedAt(job.token.jobId)

        assertThat(publication.preflight(job)).isTrue()
        assertThat(completedAt(job.token.jobId)).isEqualTo(completed)
    }

    @Test
    fun `삭제되지 않은 문서는 false를 돌려주고 Job을 건드리지 않는다`() {
        val job = claimIndex()

        assertThat(publication.preflight(job)).isFalse()
        assertThat(row(job.token.jobId)["status"]).isEqualTo("PROCESSING")
        assertThat(row(job.token.jobId)["outcome"]).isNull()
    }

    @Test
    fun `저장 시점에 삭제된 문서는 청크를 쓰지 않고 SKIPPED로 완료한다`() {
        val job = claimIndex()
        deleteDocument()

        assertThat(publication.publish(context(job), listOf(chunk(0)))).isEqualTo(IndexingJobStatus.COMPLETED)
        assertThat(chunkCount()).isZero()
        assertThat(row(job.token.jobId)["outcome"]).isEqualTo(JobOutcome.SKIPPED_DOCUMENT_DELETED.name)
        assertThat(jdbc.queryForObject("SELECT searchable_version_id FROM document WHERE id = ?", Long::class.java, documentId)).isNull()
    }

    @Test
    fun `정상 인덱싱 완료는 outcome INDEXED를 기록한다`() {
        val job = claimIndex()

        publication.publish(context(job), listOf(chunk(0)))

        assertThat(chunkCount()).isEqualTo(1)
        assertThat(row(job.token.jobId)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job.token.jobId)["outcome"]).isEqualTo(JobOutcome.INDEXED.name)
    }

    @Test
    fun `DELETE Job 완료는 outcome DELETED를 기록한다`() {
        deleteDocument()
        queue.enqueue(UUID.randomUUID(), documentId, null, JobType.DELETE, null)
        val job = checkNotNull(queue.claimFixture(5) {})

        deletion.delete(job)

        assertThat(row(job.token.jobId)["outcome"]).isEqualTo(JobOutcome.DELETED.name)
    }

    @Test
    fun `소유한 삭제 INDEX를 preflight로 종결한 뒤 후속 DELETE가 진행된다`() {
        val job = claimIndex()
        deleteDocument()
        queue.enqueue(UUID.randomUUID(), documentId, null, JobType.DELETE, null)
        // 선행 INDEX가 미종결이라 DELETE는 아직 claim되지 않는다.
        assertThat(queue.claimFixture(5) {}).isNull()

        assertThat(publication.preflight(job)).isTrue()

        assertThat(row(job.token.jobId)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job.token.jobId)["outcome"]).isEqualTo(JobOutcome.SKIPPED_DOCUMENT_DELETED.name)
        assertThat(checkNotNull(queue.claimFixture(5) {}).type).isEqualTo(JobType.DELETE)
    }

    @Test
    fun `시도 한도를 소진한 삭제 INDEX는 Worker가 선점하거나 강제 종결하지 않는다`() {
        val jobId = enqueueIndexWithoutClaim()
        jdbc.update("UPDATE indexing_job SET attempt_count = 5 WHERE id = ?", jobId)
        deleteDocument()

        val before = row(jobId)
        assertThat(queue.claimFixture(5) {}).isNull()
        assertThat(row(jobId)).isEqualTo(before)
    }

    @Test
    fun `유효한 구버전 결과는 저장 완료하되 검색 버전을 역행시키지 않는다`() {
        jdbc.update("UPDATE document SET searchable_version_id = ? WHERE id = ?", newVersionId, documentId)
        val job = claimIndex(oldVersionId)
        publication.publish(context(job), listOf(chunk(0)))

        assertThat(row(job.token.jobId)["status"]).isEqualTo("COMPLETED")
        assertThat(row(job.token.jobId)["outcome"]).isEqualTo(JobOutcome.INDEXED.name)
        assertThat(chunkCount()).isEqualTo(1)
        assertThat(jdbc.queryForObject("SELECT searchable_version_id FROM document WHERE id = ?", Long::class.java, documentId))
            .isEqualTo(newVersionId)
    }
}
