package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class IndexingEventValidator
    @Autowired
    constructor(
        private val documentVersionRepository: DocumentVersionRepository,
        @Value("\${indexing.consumer.supported-schema-versions:1}")
        supportedSchemaVersions: List<String> = listOf("1"),
    ) {
    private val supportedVersions = supportedSchemaVersions.map { it.trim().toInt() }.toSet()

    // 테스트 편의용 보조 생성자
    constructor(
        documentVersionRepository: DocumentVersionRepository,
        supportedSchemaVersions: Set<Int>,
    ) : this(documentVersionRepository, supportedSchemaVersions.map(Int::toString))

    /**
     * INDEXING_REQUESTED 이벤트 전용이다 — DOCUMENT_DELETED는 Kafka 리스너(Task 11)가
     * 이 검증기를 타지 않고 별도 핸들러로 라우팅하므로 여기서는 documentVersionId가
     * 항상 채워져 있다고 가정한다.
     *
     * tenantId 일치 검증은 의도적으로 여기서 하지 않는다 — DocumentEntity를 함께
     * 조회해야 하므로 Task 10(IndexingPipelineRunner, DocumentRepository와
     * DocumentVersionRepository를 모두 가지고 있음)에서 마저 처리한다. 이 클래스는
     * document_version 단위 검증만 담당한다(단일 책임 유지).
     */
    fun validate(event: IndexingRequestedEvent): DocumentVersionEntity {
        if (event.eventSchemaVersion !in supportedVersions) {
            throw InvalidEventException(
                "UNSUPPORTED_SCHEMA_VERSION",
                "eventSchemaVersion=${event.eventSchemaVersion} is not supported",
            )
        }

        val documentVersionId =
            event.documentVersionId
                ?: throw InvalidEventException(
                    "MISSING_DOCUMENT_VERSION_ID",
                    "INDEXING_REQUESTED event ${event.eventId} has no documentVersionId",
                )

        val documentVersion =
            documentVersionRepository.findById(documentVersionId).orElseThrow {
                InvalidEventException(
                    "DOCUMENT_VERSION_NOT_FOUND",
                    "document_version $documentVersionId does not exist",
                )
            }

        if (documentVersion.documentId != event.documentId) {
            throw InvalidEventException(
                "DOCUMENT_MISMATCH",
                "document_version $documentVersionId belongs to document " +
                    "${documentVersion.documentId}, not ${event.documentId}",
            )
        }

        return documentVersion
    }
}
