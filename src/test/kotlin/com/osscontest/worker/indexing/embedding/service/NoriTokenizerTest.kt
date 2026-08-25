package com.osscontest.worker.indexing.embedding.service

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class NoriTokenizerTest {
    private val tokenizer = NoriTokenizer()

    @Test
    fun `조사와 어미를 제거하고 체언·용언 어간만 남긴다`() {
        val tokens = tokenizer.tokenize("국회가 2021년 예산을 언제까지 합의해 처리해야 한다").split(" ")

        assertThat(tokens).contains("국회", "예산", "처리")
        assertThat(tokens).doesNotContain("가", "을", "까지", "해야")
    }

    @Test
    fun `빈 문자열은 빈 결과를 낸다`() {
        assertThat(tokenizer.tokenize("")).isEmpty()
    }
}
