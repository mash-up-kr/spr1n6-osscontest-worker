package com.osscontest.worker.indexing.consumer

class InvalidEventException(
    val code: String,
    message: String,
) : RuntimeException(message)
