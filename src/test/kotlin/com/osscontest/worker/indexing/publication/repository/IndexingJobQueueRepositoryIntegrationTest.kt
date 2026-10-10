package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.pipeline.domain.ClaimedJob
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobType
import com.osscontest.worker.indexing.pipeline.domain.JobContext
import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionTemplate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// 각 SQL이 즉시 커밋돼야 동시 claim 경합을 다른 트랜잭션에서 관찰할 수 있다.
// 그래서 테스트 트랜잭션 래핑을 끄고(NOT_SUPPORTED) @AfterEach에서 직접 정리한다.
@Tag("integration")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("integration")
@Import(IndexingJobQueueRepository::class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class IndexingJobQueueRepositoryIntegrationTest(
    private val queueRepository: IndexingJobQueueRepository,
    private val jdbcTemplate: JdbcTemplate,
    private val transactionManager: PlatformTransactionManager,
) {
    @BeforeEach
    fun seedTenant() {
        cleanUp()
        jdbcTemplate.update(
            "INSERT INTO tenant (id, name, created_at) VALUES (1, 'test-tenant', CURRENT_TIMESTAMP)",
        )
    }

    @AfterEach
    fun cleanUp() {
        // @BeforeEach의 가드가 실패해도 @AfterEach는 실행되므로, 삭제 전에 여기서 먼저 확인한다.
        assertDedicatedIntegrationDatabase(jdbcTemplate)
        // document_version INSERT 트리거가 outbox_event에도 행을 만들어, 먼저 지워야 한다.
        jdbcTemplate.update("DELETE FROM indexing_job")
        jdbcTemplate.update("DELETE FROM outbox_event")
        jdbcTemplate.update("DELETE FROM document_version")
        jdbcTemplate.update("DELETE FROM document")
        jdbcTemplate.update("DELETE FROM tenant")
    }

    private fun seedDocument(documentId: Long) {
        jdbcTemplate.update(
            "INSERT INTO document (id, tenant_id, owner_principal_id, title) VALUES (?, 1, 'owner', 'title')",
            documentId,
        )
    }

    private fun seedVersion(
        versionId: Long,
        documentId: Long,
        versionNo: Long,
    ) {
        jdbcTemplate.update(
            """
            INSERT INTO document_version
                (id, document_id, version_no, embedding_version_no, source_object_key, original_filename,
                 mime_type, file_size, content_hash, created_by_principal_id)
            VALUES (?, ?, ?, ?, ?, ?, 'application/pdf', 100, ?, 'owner')
            """,
            versionId,
            documentId,
            versionNo,
            versionNo,
            "key-$versionId",
            "file.pdf".toByteArray(),
            "hash-$versionId",
        )
    }

    /** 등록 순서(enqueue_seq)는 DB 시퀀스가 INSERT 순서대로 부여한다. */
    private fun seedJob(
        documentId: Long,
        versionId: Long,
        status: String = "PENDING",
    ): Long =
        jdbcTemplate.queryForObject(
            """
            INSERT INTO indexing_job
                (source_event_id, document_id, document_version_id, status, kafka_topic, kafka_partition, kafka_offset)
            VALUES (gen_random_uuid(), ?, ?, ?, 'test', 0, 0)
            RETURNING id
            """,
            Long::class.java,
            documentId,
            versionId,
            status,
        )!!

    private fun jobRow(jobId: Long): Map<String, Any?> =
        jdbcTemplate.queryForMap("SELECT * FROM indexing_job WHERE id = ?", jobId)

    private fun documentEpoch(documentId: Long): Long =
        jdbcTemplate.queryForObject(
            "SELECT execution_epoch FROM document WHERE id = ?",
            Long::class.java,
            documentId,
        )!!

    private fun dbNow(): LocalDateTime =
        jdbcTemplate.queryForObject("SELECT LOCALTIMESTAMP", LocalDateTime::class.java)!!

    private fun claimedContext(): JobContext {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedJob(1, 1)
        return requireNotNull(queueRepository.claim("worker-a", 120)).context
    }

    @Test
    fun `claim은 대기 중인 Job을 잡아 소유권과 epoch와 lease를 기록한다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        val jobId = seedJob(1, 1)

        val claimed = requireNotNull(queueRepository.claim("worker-a", 120))

        assertThat(claimed.context).isEqualTo(JobContext(jobId, 1L, "worker-a", 1L))
        assertThat(claimed.documentVersionId).isEqualTo(1L)
        assertThat(claimed.jobType).isEqualTo(IndexingJobType.INDEX)
        assertThat(claimed.attemptCount).isEqualTo(1)

        val row = jobRow(jobId)
        assertThat(row["status"]).isEqualTo("PROCESSING")
        assertThat(row["worker_id"]).isEqualTo("worker-a")
        assertThat(row["execution_epoch"]).isEqualTo(1L)
        assertThat(row["lease_until"]).isNotNull
        assertThat(row["started_at"]).isNotNull
        assertThat(documentEpoch(1)).isEqualTo(1L)
    }

    @Test
    fun `claim은 대기 중인 Job이 없으면 null을 돌려주고 epoch를 올리지 않는다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedJob(1, 1, status = "COMPLETED")

        assertThat(queueRepository.claim("worker-a", 120)).isNull()
        assertThat(documentEpoch(1)).isZero()
    }

    @Test
    fun `complete는 실행권이 있는 Worker의 완료를 기록하고 lease를 해제한다`() {
        val ctx = claimedContext()

        assertThat(queueRepository.complete(ctx, "INDEXED")).isEqualTo(1)

        val row = jobRow(ctx.jobId)
        assertThat(row["status"]).isEqualTo("COMPLETED")
        assertThat(row["outcome"]).isEqualTo("INDEXED")
        assertThat(row["completed_at"]).isNotNull
        assertThat(row["lease_until"]).isNull()
    }

    @Test
    fun `complete는 일치하지 않는 epoch를 거절한다`() {
        val ctx = claimedContext()

        assertThat(queueRepository.complete(ctx.copy(epoch = ctx.epoch + 1), "INDEXED")).isZero()
        assertThat(jobRow(ctx.jobId)["status"]).isEqualTo("PROCESSING")
    }

    @Test
    fun `complete는 다른 Worker를 거절한다`() {
        val ctx = claimedContext()

        assertThat(queueRepository.complete(ctx.copy(workerId = "worker-b"), "INDEXED")).isZero()
        assertThat(jobRow(ctx.jobId)["status"]).isEqualTo("PROCESSING")
    }

    @Test
    fun `complete는 PROCESSING이 아닌 Job을 거절한다`() {
        val ctx = claimedContext()
        queueRepository.complete(ctx, "INDEXED")

        assertThat(queueRepository.complete(ctx, "SKIPPED_DOCUMENT_DELETED")).isZero()
        assertThat(jobRow(ctx.jobId)["outcome"]).isEqualTo("INDEXED")
    }

    @Test
    fun `complete는 lease가 만료됐어도 소유자와 epoch가 맞으면 반영한다`() {
        val ctx = claimedContext()
        jdbcTemplate.update(
            "UPDATE indexing_job SET lease_until = CURRENT_TIMESTAMP - interval '1 minute' WHERE id = ?",
            ctx.jobId,
        )

        assertThat(queueRepository.complete(ctx, "INDEXED")).isEqualTo(1)
        assertThat(jobRow(ctx.jobId)["status"]).isEqualTo("COMPLETED")
    }

    @Test
    fun `fail은 RETRY_WAIT과 재시도 시각을 기록하고 lease를 해제한다`() {
        val ctx = claimedContext()
        val retryAt = jdbcTemplate.queryForObject("SELECT LOCALTIMESTAMP", LocalDateTime::class.java)!!.plusSeconds(30)

        val updated =
            queueRepository.fail(
                ctx,
                IndexingJobStatus.RETRY_WAIT,
                retryAt,
                "EMBEDDING_FAILED",
                "temporary error",
            )

        assertThat(updated).isEqualTo(1)
        val row = jobRow(ctx.jobId)
        val stored =
            jdbcTemplate.queryForObject(
                "SELECT next_retry_at::timestamp FROM indexing_job WHERE id = ?",
                LocalDateTime::class.java,
                ctx.jobId,
            )
        assertThat(row["status"]).isEqualTo("RETRY_WAIT")
        assertThat(stored).isEqualTo(retryAt)
        assertThat(row["last_error_code"]).isEqualTo("EMBEDDING_FAILED")
        assertThat(row["last_error_message"]).isEqualTo("temporary error")
        assertThat(row["lease_until"]).isNull()
        assertThat(row["completed_at"]).isNull()
    }

    @Test
    fun `fail은 FAILED를 기록하고 종결 시각을 남긴다`() {
        val ctx = claimedContext()

        val updated = queueRepository.fail(ctx, IndexingJobStatus.FAILED, null, "UNSUPPORTED_MIME_TYPE", "bad")

        assertThat(updated).isEqualTo(1)
        val row = jobRow(ctx.jobId)
        assertThat(row["status"]).isEqualTo("FAILED")
        assertThat(row["completed_at"]).isNotNull
        assertThat(row["last_error_code"]).isEqualTo("UNSUPPORTED_MIME_TYPE")
        assertThat(row["last_error_message"]).isEqualTo("bad")
        assertThat(row["next_retry_at"]).isNull()
        assertThat(row["lease_until"]).isNull()
    }

    @Test
    fun `fail은 일치하지 않는 epoch를 거절한다`() {
        val ctx = claimedContext()

        val updated =
            queueRepository.fail(ctx.copy(epoch = ctx.epoch + 1), IndexingJobStatus.FAILED, null, "X", "x")

        assertThat(updated).isZero()
        assertThat(jobRow(ctx.jobId)["status"]).isEqualTo("PROCESSING")
    }

    @Test
    fun `fail은 RETRY_WAIT과 FAILED 이외의 상태를 거절한다`() {
        val ctx = claimedContext()

        assertThrows<IllegalArgumentException> {
            queueRepository.fail(ctx, IndexingJobStatus.COMPLETED, null, "X", "x")
        }
    }

    @Test
    fun `fail은 RETRY_WAIT에 재시도 시각이 없으면 거절한다`() {
        val ctx = claimedContext()

        assertThrows<IllegalArgumentException> {
            queueRepository.fail(ctx, IndexingJobStatus.RETRY_WAIT, null, "X", "x")
        }
    }

    @Test
    fun `fail은 FAILED에 재시도 시각이 있으면 거절한다`() {
        val ctx = claimedContext()

        assertThrows<IllegalArgumentException> {
            queueRepository.fail(ctx, IndexingJobStatus.FAILED, LocalDateTime.now(), "X", "x")
        }
    }

    @Test
    fun `claim은 같은 문서의 Job을 등록 순서대로 하나씩만 잡는다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedVersion(2, 1, 2)
        val first = seedJob(1, 1)
        val second = seedJob(1, 2)

        assertThat(requireNotNull(queueRepository.claim("worker-a", 120)).context.jobId).isEqualTo(first)
        // 앞선 Job이 PROCESSING이라 같은 문서의 후속 Job은 잡히지 않는다.
        assertThat(queueRepository.claim("worker-b", 120)).isNull()

        jdbcTemplate.update("UPDATE indexing_job SET status = 'COMPLETED' WHERE id = ?", first)

        val next = requireNotNull(queueRepository.claim("worker-b", 120))
        assertThat(next.context.jobId).isEqualTo(second)
        assertThat(next.context.epoch).isEqualTo(2L)
    }

    @Test
    fun `claim은 서로 다른 문서의 Job을 동시에 잡는다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedDocument(2)
        seedVersion(2, 2, 1)
        val job1 = seedJob(1, 1)
        val job2 = seedJob(2, 2)

        val first = requireNotNull(queueRepository.claim("worker-a", 120))
        val second = requireNotNull(queueRepository.claim("worker-b", 120))

        assertThat(setOf(first.context.jobId, second.context.jobId)).containsExactlyInAnyOrder(job1, job2)
        assertThat(first.context.epoch).isEqualTo(1L)
        assertThat(second.context.epoch).isEqualTo(1L)
    }

    @Test
    fun `한 Worker는 서로 다른 문서의 Job을 여러 개 동시에 claim할 수 있다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedDocument(2)
        seedVersion(2, 2, 1)
        seedDocument(3)
        seedVersion(3, 3, 1)
        val jobIds = listOf(seedJob(1, 1), seedJob(2, 2), seedJob(3, 3))

        // 앞선 claim의 처리가 끝나지 않은 상태에서 같은 Worker가 이어서 claim한다.
        val claimed = jobIds.map { requireNotNull(queueRepository.claim("worker-a", 120)) }

        assertThat(claimed.map { it.context.jobId }).containsExactlyInAnyOrderElementsOf(jobIds)
        assertThat(claimed.map { it.context.workerId }).containsOnly("worker-a")
        assertThat(jobIds.map { jobRow(it)["status"] }).containsOnly("PROCESSING")
        assertThat(queueRepository.claim("worker-a", 120)).isNull()
    }

    @Test
    fun `claim은 앞선 Job이 RETRY_WAIT이면 같은 문서의 후속 Job을 잡지 않는다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedVersion(2, 1, 2)
        seedJob(1, 1, status = "RETRY_WAIT")
        seedJob(1, 2)
        jdbcTemplate.update(
            "UPDATE indexing_job SET next_retry_at = CURRENT_TIMESTAMP + interval '1 hour' WHERE status = 'RETRY_WAIT'",
        )

        assertThat(queueRepository.claim("worker-a", 120)).isNull()
    }

    @Test
    fun `claim은 재시도 시각이 지난 RETRY_WAIT만 잡고 재시도는 새 epoch를 받는다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        val jobId = seedJob(1, 1)
        val firstClaim = requireNotNull(queueRepository.claim("worker-a", 120))
        queueRepository.fail(
            firstClaim.context,
            IndexingJobStatus.RETRY_WAIT,
            dbNow().plusHours(1),
            "TEMP",
            "temporary",
        )

        assertThat(queueRepository.claim("worker-b", 120)).isNull()

        jdbcTemplate.update(
            "UPDATE indexing_job SET next_retry_at = CURRENT_TIMESTAMP - interval '1 second' WHERE id = ?",
            jobId,
        )

        val retried = requireNotNull(queueRepository.claim("worker-b", 120))
        assertThat(retried.context.jobId).isEqualTo(jobId)
        assertThat(retried.context.epoch).isEqualTo(2L)
        assertThat(retried.attemptCount).isEqualTo(2)
        assertThat(jobRow(jobId)["next_retry_at"]).isNull()
    }

    @Test
    fun `이전 claim의 실행권은 재claim 뒤에 거절된다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        val jobId = seedJob(1, 1)
        val oldCtx = requireNotNull(queueRepository.claim("worker-a", 120)).context
        queueRepository.fail(oldCtx, IndexingJobStatus.RETRY_WAIT, dbNow().minusSeconds(1), "TEMP", "t")
        val newCtx = requireNotNull(queueRepository.claim("worker-a", 120)).context

        // 같은 Worker가 다시 잡아도 epoch가 달라 이전 실행은 구분된다.
        assertThat(newCtx.epoch).isEqualTo(oldCtx.epoch + 1)
        assertThat(queueRepository.complete(oldCtx, "INDEXED")).isZero()
        assertThat(jobRow(jobId)["status"]).isEqualTo("PROCESSING")
        assertThat(queueRepository.complete(newCtx, "INDEXED")).isEqualTo(1)
    }

    private fun leaseUntil(jobId: Long): OffsetDateTime =
        jdbcTemplate.queryForObject(
            "SELECT lease_until FROM indexing_job WHERE id = ?",
            OffsetDateTime::class.java,
            jobId,
        )!!

    @Test
    fun `renewLease는 유효한 lease를 연장한다`() {
        val ctx = claimedContext()
        val before = leaseUntil(ctx.jobId)

        assertThat(queueRepository.renewLease(ctx, 600)).isEqualTo(1)
        assertThat(leaseUntil(ctx.jobId)).isAfter(before)
    }

    @Test
    fun `renewLease는 이미 만료된 lease를 되살리지 않는다`() {
        val ctx = claimedContext()
        jdbcTemplate.update(
            "UPDATE indexing_job SET lease_until = CURRENT_TIMESTAMP - interval '1 minute' WHERE id = ?",
            ctx.jobId,
        )
        val expired = leaseUntil(ctx.jobId)

        assertThat(queueRepository.renewLease(ctx, 600)).isZero()
        assertThat(leaseUntil(ctx.jobId)).isEqualTo(expired)
    }

    @Test
    fun `renewLease는 일치하지 않는 epoch와 다른 Worker를 거절한다`() {
        val ctx = claimedContext()

        assertThat(queueRepository.renewLease(ctx.copy(epoch = ctx.epoch + 1), 600)).isZero()
        assertThat(queueRepository.renewLease(ctx.copy(workerId = "worker-b"), 600)).isZero()
    }

    @Test
    fun `updatePhase는 실행권이 있을 때만 단계를 기록한다`() {
        val ctx = claimedContext()

        assertThat(queueRepository.updatePhase(ctx.copy(epoch = ctx.epoch + 1), "EMBEDDING")).isZero()
        assertThat(jobRow(ctx.jobId)["phase"]).isNull()

        assertThat(queueRepository.updatePhase(ctx, "DOWNLOADING")).isEqualTo(1)
        assertThat(jobRow(ctx.jobId)["phase"]).isEqualTo("DOWNLOADING")
    }

    /**
     * 다른 트랜잭션이 claim한 채 커밋하지 않은 동안 [whileHeld]를 실행한다.
     * 그 트랜잭션이 잠근 Job과 문서가 아직 커밋되지 않은 상태에서의 두 번째 claim을 재현한다.
     */
    private fun <T> withUncommittedClaim(
        workerId: String,
        whileHeld: () -> T,
    ): Pair<ClaimedJob?, T> {
        val locked = CountDownLatch(1)
        val release = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()
        val holder =
            executor.submit<ClaimedJob?> {
                TransactionTemplate(transactionManager).execute {
                    val claimed = queueRepository.claim(workerId, 120)
                    locked.countDown()
                    check(release.await(10, TimeUnit.SECONDS)) { "release 신호를 받지 못했다" }
                    claimed
                }
            }
        try {
            check(locked.await(10, TimeUnit.SECONDS)) { "첫 claim이 시작되지 않았다" }
            val result = whileHeld()
            release.countDown()
            return holder.get(10, TimeUnit.SECONDS) to result
        } finally {
            release.countDown()
            executor.shutdown()
        }
    }

    @Test
    fun `claim은 다른 트랜잭션이 잠근 Job을 기다리지 않고 다음 후보를 잡는다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        seedDocument(2)
        seedVersion(2, 2, 1)
        val job1 = seedJob(1, 1)
        val job2 = seedJob(2, 2)

        val (held, second) = withUncommittedClaim("worker-a") { queueRepository.claim("worker-b", 120) }

        assertThat(requireNotNull(held).context.jobId).isEqualTo(job1)
        assertThat(requireNotNull(second).context.jobId).isEqualTo(job2)
    }

    @Test
    fun `claim은 후보가 하나뿐이고 다른 트랜잭션이 잠갔다면 기다리지 않고 null을 돌려준다`() {
        seedDocument(1)
        seedVersion(1, 1, 1)
        val job1 = seedJob(1, 1)

        val (held, second) = withUncommittedClaim("worker-a") { queueRepository.claim("worker-b", 120) }

        assertThat(requireNotNull(held).context.jobId).isEqualTo(job1)
        assertThat(second).isNull()
    }
}
