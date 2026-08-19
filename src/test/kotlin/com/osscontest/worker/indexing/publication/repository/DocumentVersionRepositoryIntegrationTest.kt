package com.osscontest.worker.indexing.publication.repository

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.test.context.ActiveProfiles

@Tag("integration")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("local")
class DocumentVersionRepositoryIntegrationTest(
    private val documentVersionRepository: DocumentVersionRepository,
    private val documentRepository: DocumentRepository,
) {
    @Test
    fun `검색 버전이 없는 문서는 findSearchableEmbeddingVersionNo가 null이다`() {
        val result = documentRepository.findSearchableEmbeddingVersionNo(999_999L)
        assertThat(result).isNull()
    }
}
