/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.fault

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "fault-injection")
data class FaultInjectionProperties(
    val enabled: Boolean = false,
    val phase: String = "EMBEDDING",
)
