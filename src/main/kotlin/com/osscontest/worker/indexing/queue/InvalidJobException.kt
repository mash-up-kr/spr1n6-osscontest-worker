package com.osscontest.worker.indexing.queue

class InvalidJobException(
    val code: String,
    message: String,
) : RuntimeException(message)
