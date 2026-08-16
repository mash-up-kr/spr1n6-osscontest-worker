package com.osscontest.worker.indexing.parsing.domain

data class ParsedBlock(
    val order: Int,
    val type: BlockType,
    val text: String,
    val pageNo: Int?,
    val headingPath: List<String>,
)
