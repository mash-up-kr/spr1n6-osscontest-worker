package com.osscontest.worker.indexing.publication.entity

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table

@Entity
@Table(name = "document_chunk")
class DocumentChunkEntity(
    @Id
    var id: Long,
)
