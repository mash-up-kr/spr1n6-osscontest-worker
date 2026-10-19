/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.queue

class InvalidJobException(
    val code: String,
    message: String,
) : RuntimeException(message)
