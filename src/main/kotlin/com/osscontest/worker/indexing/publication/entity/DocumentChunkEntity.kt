package com.osscontest.worker.indexing.publication.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.time.LocalDateTime

@Entity
@Table(name = "document_chunk")
class DocumentChunkEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "tenant_id", nullable = false)
    var tenantId: Long,
    @Column(name = "document_version_id", nullable = false)
    var documentVersionId: Long,
    @Column(name = "document_id", nullable = false)
    var documentId: Long,
    @Column(name = "chunk_no", nullable = false)
    var chunkNo: Int,
    @Column(name = "content", nullable = false)
    var content: String,
    @Column(name = "content_hash", nullable = false)
    var contentHash: String,
    @Column(name = "token_count")
    var tokenCount: Int?,
    @Column(name = "page_from")
    var pageFrom: Int?,
    @Column(name = "page_to")
    var pageTo: Int?,
    @Column(name = "section_path")
    var sectionPath: String?,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata", columnDefinition = "jsonb")
    var metadata: Map<String, Any>?,
    @JdbcTypeCode(SqlTypes.VECTOR)
    @Column(name = "embedding", nullable = false, columnDefinition = "vector(1536)")
    var embedding: FloatArray,
    @Column(name = "embedded_at", nullable = false)
    var embeddedAt: LocalDateTime,
)
