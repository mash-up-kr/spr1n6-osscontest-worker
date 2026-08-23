package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import java.io.InputStream

interface DocumentParser {
    val supportedMimeTypes: Set<String>

    /**
     * 반환된 [Sequence]는 끝까지 소비해야 한다. 구현체가 시퀀스 소비를 마칠 때까지
     * [input] 스트림이나 리더를 열어둘 수 있으므로 일부만 소비하면 자원이 누수될 수 있다.
     */
    fun parse(input: InputStream): Sequence<ParsedBlock>
}
