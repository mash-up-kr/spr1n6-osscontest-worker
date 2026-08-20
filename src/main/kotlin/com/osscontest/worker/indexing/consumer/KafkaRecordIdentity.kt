package com.osscontest.worker.indexing.consumer

/** Kafka 로그에서 하나의 record를 식별하는 불변 위치다. */
data class KafkaRecordIdentity(
    val topic: String,
    val partition: Int,
    val offset: Long,
) {
    init {
        require(topic.isNotBlank()) { "Kafka topic must not be blank" }
        require(partition >= 0) { "Kafka partition must be non-negative" }
        require(offset >= 0) { "Kafka offset must be non-negative" }
    }
}
