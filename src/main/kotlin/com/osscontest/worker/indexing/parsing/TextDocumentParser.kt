package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.BlockType
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import org.springframework.stereotype.Component
import java.io.InputStream

@Component
class TextDocumentParser : DocumentParser {
    override val supportedMimeTypes = setOf("text/plain", "text/markdown")
    override fun parse(input: InputStream): Sequence<ParsedBlock> =
        sequence {
            val headingStack = mutableListOf<Pair<Int, String>>()
            var order = 0

            input.bufferedReader(Charsets.UTF_8).useLines { lines ->
                val paragraph = StringBuilder()

                // 중첩 local fun은 sequence 빌더의 SequenceScope 리시버를 이어받지 못해 yield를
                // 못 부른다 — 그래서 문단 방출은 항상 메인 루프에서 하고, 이 함수는 버퍼 비우기만 한다.
                fun flushParagraph() {
                    paragraph.clear()
                }

                for (rawLine in lines) {
                    val headingMatch = HEADING_PATTERN.matchEntire(rawLine)
                    if (headingMatch != null) {
                        if (paragraph.isNotBlank()) {
                            yield(
                                ParsedBlock(
                                    order = order++,
                                    type = BlockType.PARAGRAPH,
                                    text = paragraph.toString().trim(),
                                    pageNo = null,
                                    headingPath = headingStack.map { it.second },
                                ),
                            )
                        }
                        flushParagraph()

                        val level = headingMatch.groupValues[1].length
                        val headingText = headingMatch.groupValues[2].trim()
                        // 레벨을 건너뛰는 경우(예: H1 다음 바로 H3)는 가상의 중간 헤딩을 만들지 않고
                        // 더 깊은 헤딩을 가장 가까운 얕은 헤딩 아래에 그대로 중첩시킨다 — 의도된 동작.
                        while (headingStack.isNotEmpty() && headingStack.last().first >= level) {
                            headingStack.removeAt(headingStack.size - 1)
                        }
                        headingStack.add(level to headingText)

                        yield(
                            ParsedBlock(
                                order = order++,
                                type = BlockType.HEADING,
                                text = headingText,
                                pageNo = null,
                                headingPath = headingStack.map { it.second },
                            ),
                        )
                        continue
                    }

                    if (rawLine.isBlank()) {
                        if (paragraph.isNotBlank()) {
                            yield(
                                ParsedBlock(
                                    order = order++,
                                    type = BlockType.PARAGRAPH,
                                    text = paragraph.toString().trim(),
                                    pageNo = null,
                                    headingPath = headingStack.map { it.second },
                                ),
                            )
                        }
                        flushParagraph()
                    } else {
                        if (paragraph.isNotEmpty()) paragraph.append(' ')
                        paragraph.append(rawLine.trim())
                    }
                }

                if (paragraph.isNotBlank()) {
                    yield(
                        ParsedBlock(
                            order = order++,
                            type = BlockType.PARAGRAPH,
                            text = paragraph.toString().trim(),
                            pageNo = null,
                            headingPath = headingStack.map { it.second },
                        ),
                    )
                }
            }
        }

    private companion object {
        val HEADING_PATTERN = Regex("^(#{1,6})\\s+(.*)$")
    }
}
