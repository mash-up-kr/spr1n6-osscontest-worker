package com.osscontest.worker.indexing.publication.entity

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
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
    @Column(name = "document_version_id", nullable = false)
    var documentVersionId: Long,
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    var status: IndexingJobStatus,
    @Column(name = "attempt_count", nullable = false)
    var attemptCount: Int,
    @Column(name = "next_retry_at")
    var nextRetryAt: LocalDateTime?,
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
    var startedAt: LocalDateTime?,
    @Column(name = "completed_at")
    var completedAt: LocalDateTime?,
    @Column(name = "updated_at", nullable = false)
    var updatedAt: LocalDateTime,
    @Column(name = "kafka_topic", nullable = false)
    var kafkaTopic: String,
    @Column(name = "kafka_partition", nullable = false)
    var kafkaPartition: Int,
    @Column(name = "kafka_offset", nullable = false)
    var kafkaOffset: Long,
)
