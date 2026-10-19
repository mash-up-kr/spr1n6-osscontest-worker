/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.embedding.service

import org.apache.lucene.analysis.ko.KoreanAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.springframework.stereotype.Component

/**
 * document_chunk.content_tokens에 저장할 정규화 토큰을 만든다.
 *
 * BM25 랭킹은 Server(SearchChunkRepository)가 계산하고, 그 입력인 형태소 분석은 적재 시점에 여기서 한다.
 * Postgres는 JVM 라이브러리인 Nori를 호출할 수 없어 결과를 컬럼으로 남긴다.
 *
 * KoreanAnalyzer의 기본 stop tag 세트가 조사·어미 등 검색에 의미 없는 형태소를
 * 걸러내고 체언·용언 어간만 남긴다.
 */
@Component
class NoriTokenizer {
    fun tokenize(text: String): String {
        val tokens = mutableListOf<String>()
        KoreanAnalyzer().use { analyzer ->
            analyzer.tokenStream("content", text).use { stream ->
                val termAttr = stream.addAttribute(CharTermAttribute::class.java)
                stream.reset()
                while (stream.incrementToken()) {
                    tokens.add(termAttr.toString())
                }
                stream.end()
            }
        }
        return tokens.joinToString(" ")
    }
}
