package com.osscontest.worker.indexing.consumer

class DeserializationException(
    cause: Throwable,
) : RuntimeException("Failed to deserialize indexing requested event", cause)
