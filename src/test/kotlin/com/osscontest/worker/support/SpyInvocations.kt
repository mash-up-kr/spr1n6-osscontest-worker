package com.osscontest.worker.support

import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import java.lang.reflect.Modifier

/**
 * spy의 원래 동작을 실행한다.
 *
 * Spring Data 리포지토리처럼 인터페이스 프록시를 spy하면 Mockito는 인터페이스 mock을 만들어
 * callRealMethod가 abstract 메서드라 실패한다. 이때 @MockitoSpyBean은 원래 빈에 호출을 넘기는
 * 기본 answer를 걸어 두므로 그 answer로 실행한다.
 */
fun InvocationOnMock.callSpiedMethod(): Any? {
    if (!Modifier.isAbstract(method.modifiers)) return callRealMethod()
    return Mockito.mockingDetails(mock).mockCreationSettings.defaultAnswer.answer(this)
}
