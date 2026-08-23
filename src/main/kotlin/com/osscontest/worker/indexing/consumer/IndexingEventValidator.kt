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

    // 단위 테스트에서 Spring 문자열 바인딩 없이 지원 버전을 전달한다.
    constructor(
        documentVersionRepository: DocumentVersionRepository,
        supportedSchemaVersions: Set<Int>,
    ) : this(documentVersionRepository, supportedSchemaVersions.map(Int::toString))

    /**
     * INDEXING_REQUESTED 이벤트 전용이다. DOCUMENT_DELETED는 Kafka 리스너가
     * 이 검증기를 타지 않고 별도 핸들러로 라우팅하므로 여기서는 documentVersionId가
     * 항상 채워져 있다고 가정한다.
     *
     * Kafka 키와 이벤트의 documentId, document_version의 documentId는 Core가 보장하는
     * 계약을 신뢰한다. tenantId는 DocumentEntity가 필요한 파이프라인에서 검증한다.
     */
    fun validate(event: IndexingEvent): DocumentVersionEntity {
        if (event.schemaVersion !in supportedVersions) {
            throw InvalidEventException(
                "UNSUPPORTED_SCHEMA_VERSION",
                "schemaVersion=${event.schemaVersion} is not supported",
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

        return documentVersion
    }
}
