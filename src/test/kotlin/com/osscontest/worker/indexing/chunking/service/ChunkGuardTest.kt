package com.osscontest.worker.indexing.chunking.service

import com.osscontest.worker.indexing.chunking.config.ChunkingProperties
import com.osscontest.worker.indexing.chunking.domain.Chunk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test

class ChunkGuardTest {
    private val guard = ChunkGuard(ChunkingProperties(maxChunksPerDocument = 3, maxTotalTokens = 1000))

    @Test
    fun `빈 청크 리스트는 예외를 던진다`() {
        val thrown =
            try {
                guard.assertValid(emptyList())
                null
            } catch (e: EmptyExtractionException) {
                e
            }

        assertThat(thrown).isNotNull
        assertThat(thrown!!.code).isEqualTo("EMPTY_EXTRACTION")
    }

    @Test
    fun `청크 수 상한을 넘으면 예외를 던진다`() {
        val chunks = (0..5).map { fakeChunk(it) }

        val thrown =
            try {
                guard.assertValid(chunks)
                null
            } catch (e: ChunkLimitExceededException) {
                e
            }

        assertThat(thrown).isNotNull
        assertThat(thrown!!.code).isEqualTo("CHUNK_LIMIT_EXCEEDED")
    }

    @Test
    fun `청크 수가 상한과 정확히 같으면 예외를 던지지 않는다`() {
        val chunks = (0..2).map { fakeChunk(it) }

        assertThatCode { guard.assertValid(chunks) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `총 토큰 수가 상한 이하면 예외를 던지지 않는다`() {
        val tightGuard = ChunkGuard(ChunkingProperties(maxChunksPerDocument = 10, maxTotalTokens = 30))
        val chunks = (0..2).map { fakeChunk(it, tokenCount = 10) } // 합계 30 = 상한

        assertThatCode { tightGuard.assertValid(chunks) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `총 토큰 수가 상한을 넘으면 예외를 던진다`() {
        val tightGuard = ChunkGuard(ChunkingProperties(maxChunksPerDocument = 10, maxTotalTokens = 25))
        val chunks = (0..2).map { fakeChunk(it, tokenCount = 10) } // 합계 30 > 상한 25

        val thrown =
            try {
                tightGuard.assertValid(chunks)
                null
            } catch (e: TotalTokenLimitExceededException) {
                e
            }

        assertThat(thrown).isNotNull
        assertThat(thrown!!.actualTotalTokens).isEqualTo(30)
        assertThat(thrown.maxTotalTokens).isEqualTo(25)
        assertThat(thrown.code).isEqualTo("TOTAL_TOKEN_LIMIT_EXCEEDED")
    }

    private fun fakeChunk(
        no: Int,
        tokenCount: Int = 10,
    ) = Chunk(
        chunkNo = no,
        content = "content-$no",
        contentHash = "hash-$no",
        tokenCount = tokenCount,
        pageFrom = null,
        pageTo = null,
        sectionPath = null,
        metadata = null,
    )
}
