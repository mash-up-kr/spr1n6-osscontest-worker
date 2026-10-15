package com.osscontest.worker.indexing.queue

import com.zaxxer.hikari.SQLExceptionOverride.Override
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.SQLException

class OpenProxyExceptionOverrideTest {
    private val override = OpenProxyExceptionOverride()

    @Test
    fun `읽기 전용 서버에 쓴 연결은 버린다`() {
        assertThat(override.adjudicate(SQLException("read-only", "25006"))).isEqualTo(Override.MUST_EVICT)
    }

    @Test
    fun `OpenProxy가 붙을 서버를 찾지 못한 연결은 버린다`() {
        assertThat(override.adjudicate(SQLException("AllServersDown", "58000"))).isEqualTo(Override.MUST_EVICT)
    }

    @Test
    fun `그 밖의 오류는 HikariCP 기본 판정에 맡긴다`() {
        // DO_NOT_EVICT 가 되면 연결 끊김 같은 Hikari 기본 폐기까지 막히므로 CONTINUE_EVICT 여야 한다.
        assertThat(override.adjudicate(SQLException("unique violation", "23505"))).isEqualTo(Override.CONTINUE_EVICT)
        assertThat(override.adjudicate(SQLException("no state"))).isEqualTo(Override.CONTINUE_EVICT)
    }
}
