package com.osscontest.worker.indexing.chunking.service

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.Encoding
import com.knuddels.jtokkit.api.EncodingType
import com.knuddels.jtokkit.api.IntArrayList
import java.security.MessageDigest

/**
 * 모든 청킹 전략(FixedTokenChunker, ParagraphChunker, ParagraphOverlapChunker)이
 * 공유하는 jtokkit 기반 토큰화 헬퍼.
 */
internal object ChunkerTokenizer {
    val encoding: Encoding = Encodings.newDefaultEncodingRegistry().getEncoding(EncodingType.CL100K_BASE)

    // jtokkit의 decode()는 일반 List<Int>가 아니라 자체 IntArrayList 타입을 요구하므로,
    // 분할된 각 청크를 다시 변환해서 넘겨준다.
    fun List<Int>.toIntArrayList(): IntArrayList {
        val result = IntArrayList(size)
        forEach { result.add(it) }
        return result
    }

    fun headingContextPrefix(headingPath: List<String>): String =
        if (headingPath.isEmpty()) "" else "[${headingPath.joinToString(" > ")}]\n"

    fun sha256Hex(text: String): String =
        MessageDigest.getInstance("SHA-256").digest(text.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
}
