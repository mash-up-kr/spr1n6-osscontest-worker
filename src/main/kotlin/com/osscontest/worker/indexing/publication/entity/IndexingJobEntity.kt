package com.osscontest.worker.indexing.publication.entity

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobType
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "indexing_job")
class IndexingJobEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "source_event_id", nullable = false)
    var sourceEventId: UUID,
    @Column(name = "document_id", nullable = false)
    var documentId: Long,
    @Column(name = "document_version_id")
    var documentVersionId: Long?,
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    var status: IndexingJobStatus,
    @Column(name = "attempt_count", nullable = false)
    var attemptCount: Int,
    @Column(name = "next_retry_at")
    var nextRetryAt: Instant?,
    @Column(name = "worker_id")
    var workerId: String?,
    @Column(name = "last_error_code")
    var lastErrorCode: String?,
    @Column(name = "last_error_message")
    var lastErrorMessage: String?,
    @Column(name = "trace_id")
    var traceId: String?,
    @Column(name = "phase")
    var phase: String? = null,
    @Column(name = "started_at")
    var startedAt: Instant?,
    @Column(name = "completed_at")
    var completedAt: Instant?,
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant,
    @Enumerated(EnumType.STRING)
    @Column(name = "job_type", nullable = false)
    var jobType: IndexingJobType = IndexingJobType.INDEX,
    // 등록 순서는 DB 시퀀스가 INSERT 시점에 부여하므로 애플리케이션에서 쓰지 않는다.
    @Column(name = "enqueue_seq", insertable = false, updatable = false)
    var enqueueSeq: Long? = null,
    @Column(name = "execution_epoch", nullable = false)
    var executionEpoch: Long = 0,
    @Column(name = "lease_until")
    var leaseUntil: LocalDateTime? = null,
    @Column(name = "outcome")
    var outcome: String? = null,
    @Column(name = "lease_expires_at")
    var leaseExpiresAt: Instant? = null,
)
