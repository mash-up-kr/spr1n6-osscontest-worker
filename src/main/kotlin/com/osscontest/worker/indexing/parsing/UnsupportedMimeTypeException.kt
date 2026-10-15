/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.parsing

class UnsupportedMimeTypeException(
    val mimeType: String,
) : RuntimeException("No parser registered for mimeType=$mimeType") {
    val code: String = "UNSUPPORTED_MIME_TYPE"
}
