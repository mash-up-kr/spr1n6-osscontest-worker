/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

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
    // 엔티티로 갱신하지 않는다. 읽어 둔 값으로 UPDATE하면 SQL이 올린 실행 번호를 되돌릴 수 있다.
    @Column(name = "execution_epoch", nullable = false, updatable = false)
    var executionEpoch: Long = 0,
)
