package com.osscontest.worker.indexing.embedding.service

import org.apache.lucene.analysis.ko.KoreanAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.springframework.stereotype.Component

/**
 * document_chunk.content_tokens에 저장할 정규화 토큰을 만든다.
 *
 * Postgres의 ts_rank_cd는 IDF가 없어 흔한 단어를 못 눌러준다. IDF를 반영한 BM25
 * 랭킹은 Server(SearchChunkRepository)가 애플리케이션 레이어에서 계산하는데, 그
 * 입력이 되는 형태소 분석은 여기(Worker, 적재 시점)에서 미리 해 둔다 — Postgres가
 * JVM 안에서만 도는 Nori를 직접 호출할 수 없어서, 인덱싱 시점에 결과를 컬럼으로
 * 남겨야 검색 시점에 다시 형태소 분석을 하지 않아도 된다.
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
