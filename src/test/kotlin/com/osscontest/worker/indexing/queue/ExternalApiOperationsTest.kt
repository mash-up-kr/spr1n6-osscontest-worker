package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.embedding.service.EmbeddingRateLimitedException
import com.osscontest.worker.support.*
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*

class ExternalApiOperationsTest {
    @Test
    fun `429는 같은 호출을 반복하지 않고 runner로 전파한다`() {
        val execution = directExecution()
        val database = directDatabase()
        val api = ExternalApiOperations(execution, database)
        val action: () -> String = mock()
        whenever(action()).thenThrow(EmbeddingRateLimitedException("10", RuntimeException("429")))
        assertThatThrownBy { api.execute(testToken, action) }.isInstanceOf(EmbeddingRateLimitedException::class.java)
        verify(action).invoke()
        verify(execution).checkpoint(testToken)
        verify(database).awaitAvailable()
    }

    @Test
    fun `실행권을 잃으면 외부 요청을 시작하지 않는다`() {
        val execution = directExecution()
        doThrow(ExecutionLostException(testToken)).whenever(execution).checkpoint(testToken)
        val action: () -> String = mock()
        assertThatThrownBy { ExternalApiOperations(execution, directDatabase()).execute(testToken, action) }
            .isInstanceOf(ExecutionLostException::class.java)
        verifyNoInteractions(action)
    }
}
