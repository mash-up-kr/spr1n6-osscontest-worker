package com.osscontest.worker.indexing.queue

import com.zaxxer.hikari.SQLExceptionOverride
import java.sql.SQLException

/**
 * OpenProxy 뒤의 Primary가 바뀐 뒤 남은 연결을 풀에서 버리게 한다.
 *
 * 두 오류 모두 HikariCP 기본 판정으로는 연결을 버리지 않아, 같은 연결이 계속 실패한다.
 * - 25006: Replica로 내려간 옛 Primary에 쓰기를 보냈다
 * - 58000: OpenProxy가 붙을 서버를 찾지 못했다(AllServersDown)
 *
 * HikariCP가 인자 없는 생성자로 만들므로 `exception-override-class-name`으로 지정한다.
 * Server의 같은 이름 클래스와 규칙이 같아야 하며, 한쪽의 SQLSTATE 목록을 바꾸면 다른 쪽도 맞춘다.
 */
class OpenProxyExceptionOverride : SQLExceptionOverride {
    override fun adjudicate(sqlException: SQLException): SQLExceptionOverride.Override =
        if (sqlException.sqlState in EVICT_SQL_STATES) {
            SQLExceptionOverride.Override.MUST_EVICT
        } else {
            // CONTINUE_EVICT 는 버린다는 뜻이 아니라 Hikari 기본 판정을 이어서 적용한다는 뜻이다.
            // DO_NOT_EVICT 를 돌려주면 연결 끊김 같은 기본 폐기까지 막힌다.
            SQLExceptionOverride.Override.CONTINUE_EVICT
        }

    private companion object {
        val EVICT_SQL_STATES = setOf("25006", "58000")
    }
}
