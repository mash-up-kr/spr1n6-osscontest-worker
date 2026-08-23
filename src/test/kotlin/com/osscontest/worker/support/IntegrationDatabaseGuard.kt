package com.osscontest.worker.support

import org.springframework.jdbc.core.JdbcTemplate

/** 통합 테스트가 일반 개발·운영 DB를 변경하지 않도록 전용 DB 이름을 확인한다. */
fun assertDedicatedIntegrationDatabase(jdbcTemplate: JdbcTemplate) {
    val databaseName = jdbcTemplate.queryForObject("SELECT current_database()", String::class.java).orEmpty()
    require(
        databaseName.contains("test", ignoreCase = true) ||
            databaseName.contains("integration", ignoreCase = true),
    ) {
        "Integration tests require a database whose name contains 'test' or 'integration': $databaseName"
    }
}
