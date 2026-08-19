package com.osscontest.worker.indexing.publication.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "document")
class DocumentEntity(
    @Id
    var id: Long,
    @Column(name = "tenant_id", nullable = false)
    var tenantId: Long,
    @Column(name = "searchable_version_id")
    var searchableVersionId: Long?,
    @Column(name = "deleted_at")
    var deletedAt: LocalDateTime?,
)
