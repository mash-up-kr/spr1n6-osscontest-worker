package com.osscontest.worker.indexing.publication.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table

@Entity
@Table(name = "document_version")
class DocumentVersionEntity(
    @Id
    var id: Long,
    @Column(name = "document_id", nullable = false)
    var documentId: Long,
    @Column(name = "version_no", nullable = false)
    var versionNo: Long,
    @Column(name = "source_object_key", nullable = false)
    var sourceObjectKey: String,
    @Column(name = "mime_type", nullable = false)
    var mimeType: String,
    @Column(name = "content_hash", nullable = false)
    var contentHash: String,
    @Column(name = "embedding_version_no", nullable = false)
    var embeddingVersionNo: Long,
)
