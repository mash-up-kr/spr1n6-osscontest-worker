# Track B 장애 대비 설계 — 수집 파이프라인 워커

> **담당**: 은지 (A — 수집 & 워커 확장성, `[임베딩 워커 - 그룹2]`)
> **작성일**: 2026-08-17
> **상태**: Track A 구현 완료 → Track B 착수 전 범위 확정용
>
> **개정 (2026-08-17)**: 위 "Kafka가 재전달할 수 없는 경로"가 왜 생기는지 따져보니, 원인이 하나였다 — **재시도를 ack 이후로 미룬 것.** `RETRY_WAIT` 기록 후 ack하면 그 순간 Kafka와의 연결이 끊기고, 이후 폴러가 만든 `PROCESSING`은 아무도 회수할 수 없다. 그래서 **재시도를 ack 이전으로 당긴다(인라인 재시도)**. 이 전환으로 §2.2 B-Gap-1과 §3 P0-1(고아 회수)이 **문제 자체로서 소멸**하고, 재시도 폴러(`IndexingRetryScheduler`)도 제거된다. 대신 재시도 예산이 `max.poll.interval.ms` 안에 들어가야 한다는 새 제약이 생기고, 흡수하지 못하는 장기 장애는 DLQ로 넘긴다. 이에 맞춰 §1.3·§2.2·§3(P0-1 교체, P0-3 재작성)·§4·§5·§6·부록을 정정한다.

---

## 0. 이 문서의 목적

Track A는 **"정합성이 깨지지 않는 파이프라인"**을 목표로 했고, 그 목표는 달성했다. Track B는 정합성을 *다시 만드는* 작업이 아니라, Track A가 의도적으로 미뤄둔 것과 **Track A를 실제로 구현하고 나서야 드러난 구멍**을 메우는 작업이다.

그래서 이 문서는 세 부분이다.

1. **§1 — Track A로 막아진 장애**: 이미 해결된 것. Track B에서 다시 건드리지 않는다.
2. **§2 — Track A가 남긴 구멍**: 미룬 것 + 구현하고 발견한 것 + 아직 결정 안 된 것.
3. **§3~§6 — Track B 항목별 대응 방법**: 각 장애를 무엇으로 어떻게 막는지, 우선순위와 검증 방법까지.

---

## 1. Track A 구현으로 막아진 장애

### 1.1 요약표

| # | 장애 | 막은 장치 | 근거 |
|---|---|---|---|
| A-1 | 워커 프로세스 크래시(SIGKILL, OOM 킬) | **ack이 처리 완료 후이므로** offset 미커밋 → Kafka가 파티션을 재할당해 자동 재전달 + `status IN ('PENDING','PROCESSING')` 재획득 | 스펙 §1.3 문제1, §1.4-(1) |
| A-2 | 리밸런스 중 같은 이벤트 중복 소비 | `source_event_id UNIQUE` + `uq_indexing_job_active_version` + `UNIQUE(document_version_id, chunk_no)` UPSERT | 스펙 §1.1 |
| A-3 | 두 워커가 동시에 같은 Job 소유 | 배제하지 않고 수렴 — 청킹 결정성(§3.6) + UPSERT | 스펙 §1.2, 부록 원칙 4 |
| A-4 | poison pill 크래시 루프(파서 네이티브 크래시 등) | 재획득 쿼리의 `attempt_count < :maxAttempts` 캡 → `FAILED('MAX_ATTEMPTS_EXCEEDED')` | 스펙 §1.4-(1), plan Task 3 |
| A-5 | 실패 시 ack 보류로 인한 hot loop(파티션 정지) | `finally { ack.acknowledge() }` — 성공/실패 무관 항상 ack | 스펙 §3.1, plan Task 11 |
| A-6 | 잘못된 이벤트의 무한 재시도(스키마 미지원, 필수 필드 누락, tenant 불일치, 없는 버전) | `IndexingEventValidator` → `InvalidEventException` 즉시 종결 + ack | 스펙 §0.3, plan Task 5 |
| A-7 | 역직렬화 실패 메시지가 파티션을 막는 것 | `DeserializationException` catch → 로그 + ack | plan Task 11 |
| A-8 | 오래된 업로드가 최신 업로드를 덮어씀(순서 역전) | `document_version.embedding_version_no` fencing 비교 | 스펙 §1.3 문제2, §1.4-(3) |
| A-9 | STALE 버전에 임베딩 비용 낭비 | 다운로드 **이전** 조기 fencing 판정 → `COMPLETED(chunk_count=null)` | 스펙 §3.1, plan Task 10 |
| A-10 | 실패 쓰기가 성공 결과를 덮어씀 | 실패 쓰기에만 `status = 'PROCESSING'` 가드(완료 쓰기는 가드 없음 — 비대칭) | 스펙 §1.4-(4) |
| A-11 | 재청킹으로 청크 수가 줄었을 때 잔존 청크 | publish 트랜잭션 말미 trailing DELETE(`chunk_no > :finalChunkCount`) | 스펙 §1.4-(2) |
| A-12 | 스캔 PDF가 "성공"으로 위장해 검색 버전 오염 | `ChunkGuard.assertValid()` — 빈 청크 → `EMPTY_EXTRACTION` | 스펙 §3.6, plan Task 9 |
| A-13 | 대용량 문서 청크 폭발로 임베딩 통째 실패 | `max-chunks-per-document: 5000` 상한 | 스펙 §3.6 |
| A-14 | 손상/변조된 원문을 인덱싱 | 다운로드 후 SHA-256 vs `content_hash` 비교 | 스펙 §3.3 |
| A-15 | Storage 무응답으로 워커 스레드 영구 잠김 | 다운로드 타임아웃 30초 | 스펙 §3.3 |
| A-16 | 비즈니스 예외 후 재시도 경로 없음 | `RETRY_WAIT` + `next_retry_at` + 재시도 폴러 | 스펙 §1.4-(4), §3.8 <br>**※ 개정 2에서 인라인 재시도로 대체(§3 P0-1)** |
| A-17 | 여러 폴러 인스턴스의 중복 재시도 | 조건부 UPDATE — 분산 락 불필요 | 스펙 §3.8 <br>**※ 폴러 제거로 무의미해짐** |
| A-18 | 삭제된 문서의 유령 청크가 검색에 노출 | `DOCUMENT_DELETED` 처리 + `document_chunk` 삭제 | 스펙 §3.9 |
| A-19 | 삭제 이벤트 유실 시 청크 잔존 | 정리 스윕 스케줄러 | 스펙 §3.9 |
| A-20 | 삭제된 문서를 재시도 폴러가 되살림 | `failActiveJobsForDocument()` | 스펙 §3.9, plan Task 19 |
| A-21 | 삭제 후 뒤늦게 끝난 Job이 검색 버전을 되살림 | 버전 전환 UPDATE에 `d.deleted_at IS NULL` 가드 | 스펙 §3.9, §6.1 |
| A-22 | 업로드↔삭제 순서 역전 | 같은 토픽·같은 파티션(`documentId` 키) | 스펙 §0.3 |

### 1.2 특히 강조할 만한 것

**① ack은 처리가 전부 끝난 뒤에만 한다 — 이게 워커 크래시 복구의 전부다.**

`ack-mode: manual`이고, `ack.acknowledge()`는 다운로드·파싱·청킹·`IndexingProcessor.process()`가 전부 끝난 뒤에 호출된다.

```
워커가 처리 도중 죽는다
  → 그 메시지의 offset은 커밋되지 않았다
  → heartbeat가 끊기고 Kafka가 워커 사망을 감지 (session.timeout.ms)
  → 파티션을 다른 컨슈머에게 재할당 (리밸런스)
  → 넘겨받은 워커가 브로커에 "이 파티션 어디부터 읽어요?"를 묻는다
  → 답은 committed offset. 죽은 워커가 처리 중이던 그 메시지부터 다시 읽는다
  → start()가 status IN ('PENDING','PROCESSING') 조건으로 재획득 → 처음부터 재처리
```

auto-commit이었다면 처리 전에 커밋이 나가서 그 메시지가 **유실**된다. manual ack이 "누락 없는 소비"를 성립시키는 장치다. 대가는 **중복**(at-least-once) — 89초까지 처리하고 죽으면 새 워커가 0초부터 다시 한다. 그래서 청킹 결정성 + UPSERT로 중복을 무해하게 만든다(A-3).

**② 재전달의 방아쇠는 ack이 아니라 파티션 재배정이다.**

Kafka 브로커가 컨슈머 그룹에 대해 저장하는 건 파티션당 숫자 하나(committed offset)뿐이다. "누구에게 뭘 줬는지", "ack이 왔는지"를 기억하지 않으므로 **ack 누락을 감지할 방법 자체가 없다.** 메시지가 다시 오는 순간은 컨슈머가 파티션을 새로 배정받아 "어디부터 읽죠?"를 묻는 때뿐이고, 그건 리밸런스에서만 일어난다.

```
재전달 = ① 방아쇠(리밸런스)  +  ② 되감을 거리(커밋 안 된 offset)
         └ 워커가 죽어야 당겨짐    └ manual ack이 만들어줌
```

**워커가 살아있는 채로 로직만 실패하면 ①이 없다.** heartbeat가 계속 가니 Kafka 입장에선 아무 문제 없는 상태고, 개입할 이유가 없다. 이게 §1.3의 출발점이다.

**③ 상호 배제를 포기하고 수렴을 택한 것.** 두 워커가 같은 Job을 동시에 만지는 것을 허용하고, 청킹 결정성 + UPSERT로 결과가 같은 값에 수렴하게 했다. 리밸런스로 두 워커가 같은 메시지를 잡아도, 되살아난 워커가 뒤늦게 마무리해도 최종 상태가 같다.

### 1.3 Track A가 택한 재시도 구조와, 그것이 만든 문제

§1.2-②에서 봤듯 **워커가 살아있는 실패는 Kafka가 모른다.** 그리고 Kafka에는 "30초 뒤에 다시 줘" 같은 지연 재전달 기능이 없다(스펙 §1.2 표: `지연 재시도(backoff) ❌`). 즉시 주거나, 컨슈머가 죽어서 통째로 다시 주거나 둘뿐이다.

Track A는 이걸 이렇게 풀었다.

```
파싱/임베딩 실패 (워커 생존)
  → RETRY_WAIT + next_retry_at 기록
  → ack                          ★ 여기서 Kafka와의 관계가 끝난다
  → 30초 뒤 IndexingRetryScheduler가 findRetryWaitDue()로 집어 재처리
```

ack하는 이유는 명확했다. 안 하면 hot loop가 나거나(즉시 재전달), 아무 일도 안 일어난다(재전달 없이 offset만 앞으로). 그래서 "실패를 기록했다면 반드시 ack"을 원칙으로 삼고, 재시도 트리거를 워커 자신이 갖게 했다.

**문제는 ★ 지점 이후로 그 Job에 대응하는 Kafka 메시지가 사라진다는 것이다.**

```
폴러가 RETRY_WAIT을 집음 → start() → PROCESSING
  → 이 시점에 워커 사망
  → 되감을 메시지가 없다 (ack은 아까 정당하게 끝났다)
  → 폴러는 WHERE status = 'RETRY_WAIT'만 조회한다. PROCESSING은 안 본다
  → 아무도, 영원히 안 집는다. 에러 로그조차 안 남는다
```

같은 일이 DB 장애 때도 생긴다. `recordFailure()`까지 실패하면 상태를 못 바꿨는데 `finally`에서 ack이 나가서, `PROCESSING`인 채로 메시지만 사라진다.

**즉 구멍의 원인은 "재시도를 ack 이후로 미룬 것" 하나다.** 개정 2는 여기를 고친다.

---

## 2. Track A가 남긴 구멍

### 2.1 의도적으로 미룬 것 (스펙 §4에 이미 명시)

| 구멍 | 현재 상태 | 왜 문제인가 |
|---|---|---|
| 에러 분류 없음 | 원인 불문 `attempt_count` 상한(5회)까지 재시도 | 0바이트 PDF에 5회를 낭비. 반대로 임베딩 429는 5회로 부족할 수 있다 |
| 선형 백오프만 | `next_retry_at = now() + 30s × attempt_count` | 임베딩 서버가 살아나면 실패한 Job들이 거의 동시에 재시도를 날려 다시 죽인다(thundering herd) |
| DLQ 없음 | 역직렬화 실패·검증 실패는 **로그만 남기고 사라진다** | 원인을 고쳐도 되살릴 방법이 없다. 이벤트 원문이 어디에도 없다 |
| Outbox 보정 없음 | `PUBLISHED`인데 Job이 없는 경우를 아무도 감지 못함 | 브로커 재시작만으로도 발생. 유실이 조용히 일어난다 |
| 진행률 미노출 | `phase`, `total_chunks`, `processed_chunks` 컬럼 자체가 없음 | 처리 중인지 멈춘 건지 구분 못 함 |
| 트레이싱 없음 | `trace_id`는 컬럼에만 있고 전파 안 됨 | 관측 공백 |
| 메트릭 없음 | 카운터/게이지 전무 | **감지할 수 없으면 대비한 게 아니다** |
| DB HA 대응 없음 | 페일오버 중 워커 동작 미정의 | 2차 과제 요구사항 |

### 2.2 구현하고 나서 드러난 것

---

**B-Gap-1. `PROCESSING` 고아 — 개정 2의 구조 변경으로 소멸.** ~~★최우선~~ → **해소 예정**

§1.3에서 분석한 그 문제다. 원인이 "재시도를 ack 이후로 미룬 것"이므로, 재시도를 ack 이전으로 당기면(§3 P0-1) **문제가 발생 자체를 안 한다.** 사후 청소 장치(스윕)를 새로 만드는 대신 원인을 없애는 쪽을 택했다.

전환 후 성립하는 불변조건:

> **`indexing_job`이 `PROCESSING` 또는 `RETRY_WAIT`인 동안에는, 그에 대응하는 커밋되지 않은 Kafka 메시지가 반드시 존재한다.**

이게 지켜지면 어느 시점에 워커가 죽든 항상 되감을 메시지가 있고, 리밸런스가 회수한다. **고아라는 개념 자체가 성립하지 않는다.** 초안에서 제안했던 `updated_at` 기반 스윕(구 P0-1)은 폐기한다.

단, 이 불변조건을 깨는 코드가 나중에 들어오면 구멍이 되살아난다. **"ack 이후에 Job 상태를 다시 `PROCESSING`으로 만드는 코드를 쓰지 않는다"**를 §7 부록 원칙에 못 박아둔다.

---

**B-Gap-2. `uq_indexing_job_active_version` 위반 시 이벤트가 조용히 사라진다.**

```kotlin
insertIfAbsent(...)                          // ON CONFLICT DO NOTHING
val job = findBySourceEventId(event.eventId)
if (job == null) {
    log.info("no job row for eventId=... — another active job already targets ...")
    return null                              // ← 그리고 리스너가 ack한다
}
```

같은 `document_version_id`를 가리키는 활성 Job이 이미 있으면 새 Job이 안 만들어지고, 이 이벤트는 로그 한 줄 남기고 종결된다. 앞선 Job이 성공하면 결과적으로 문제없지만, `FAILED`로 끝나면 뒤늦게 온 이 이벤트는 이미 ack되어 사라진 뒤다. → P1-6(DLQ 적재)

---

**B-Gap-3. 파일 전체를 메모리에 올린다 — OOM 경로가 세 겹.**

```kotlin
downloadClient.download(objectKey)            // ByteArray 전체
  → Loader.loadPDF(input.readAllBytes())      // 또 전체 복사
  → parser.parse(...).toList()                // Sequence의 스트리밍 이점을 여기서 폐기
  → chunkingService.chunk(blocks, ...)        // encode된 토큰 리스트 전체
```

스펙 §3.4는 OOM을 이유로 `Sequence`를 택했는데 `IndexingPipelineRunner`에서 `.toList()`로 즉시 realize한다. 파일 크기 상한도 없다 — 청크 수 상한(5000)은 청킹까지 도달한 뒤에야 판정되는데 OOM은 그 전에 난다. → P0-4

---

**B-Gap-4. 파싱에 타임아웃이 없다.** ★인라인 재시도 전환으로 더 중요해짐

다운로드는 30초 타임아웃이 있지만(A-15) 파싱·청킹은 무제한이다. 악성 PDF나 hwplib 무한 루프에 걸리면 `poll()`을 못 불러 `max.poll.interval.ms`(600초)를 넘기고, 리밸런스로 넘겨받은 다른 워커가 **똑같이 걸린다** — fleet 전체가 파일 하나에 잠식된다.

**인라인 재시도로 바꾸면 `max.poll.interval.ms` 예산을 재시도가 나눠 쓰게 되므로, 파싱 하나가 예산을 통째로 삼키는 걸 반드시 막아야 한다.** 이 항목이 P0-2(재시도 예산)의 전제 조건이 된다. → P0-4

---

**B-Gap-5. 시각의 출처가 섞여 있다.**

| 지점 | 시각 출처 |
|---|---|
| `recordFailure()`의 `nextRetryAt` | 애플리케이션 `LocalDateTime.now()` |
| `findRetryWaitDue(now)` | 애플리케이션 `LocalDateTime.now()` |
| `start()`의 `next_retry_at <= CURRENT_TIMESTAMP` | **DB 시각** |

워커 시계가 DB보다 앞서면 폴러는 due로 판단해 집었는데 `start()`가 조건 불일치로 0건을 반환한다. `LocalDateTime`(타임존 없음)과 `TIMESTAMPTZ`를 섞어 쓰는 문제도 있다.

**인라인 재시도로 바꾸면 심각도가 크게 낮아진다** — `next_retry_at`을 DB에 적고 다시 읽는 왕복이 없어지고, 재시도 간격을 프로세스 안에서 재기 때문이다. 다만 관측용으로 `next_retry_at`을 계속 기록한다면 타입 정리는 여전히 필요하다. → P2-11

---

**B-Gap-6. 파티션 hot spot — `documentId` 키의 대가.** ★인라인 재시도 전환으로 더 중요해짐

파티션 키가 `documentId`인 건 조기 STALE 판정을 위해 옳은 결정이지만(스펙 §3.1), 그 대가로 한 파티션에 대용량 문서가 들어오면 뒤의 모든 문서가 대기한다.

**인라인 재시도는 이걸 악화시킨다.** 재시도하는 425초 동안 그 배치가 커밋을 못 하고, 같은 배치의 다른 Job이 이미 끝났어도 다음 `poll()`을 못 한다. 배치 크기와 파티션 분산이 처리량에 직결된다. → P2-12

---

**B-Gap-7. 최신 버전이 실패하면 사용자는 옛 버전을 보면서 그 사실을 모른다.**

```
v2 처리 완료 → searchable = v2
v3 업로드 → 파싱 실패 → FAILED
→ 사용자는 v3을 올렸는데 검색은 여전히 v2 내용을 반환
→ Job 상태를 안 보면 알 방법이 없다
```

"검증 후에만 노출한다"(부록 원칙 7) 관점에서 의도된 동작이지만, 알리는 채널이 없다. → P1-7

---

**B-Gap-8. `FAILED` 종결 후 자동 복구 경로가 없고, 아무도 모른다.** ★인라인 재시도 전환으로 더 중요해짐

인라인 재시도는 재시도 횟수가 3회로 줄고 백오프도 짧아지므로(P0-2), **`FAILED`로 떨어지는 건수가 Track A보다 늘어난다.** 임베딩 서버가 5분 다운되면 그 사이 들어온 건 전부 `FAILED`다. 그래서 DLQ와 일괄 재처리가 "있으면 좋은 것"에서 **"없으면 전환 자체가 성립하지 않는 것"**으로 격상된다. → P0-3

---

**B-Gap-9. 삭제 스윕이 무한 재실행될 수 있다.**

chunk 삭제가 계속 실패하면(FK 제약, 락 경합) 매 60초마다 같은 문서를 영원히 재시도한다. 실패 횟수를 세지 않아 종료 조건이 없고, 배치 크기 50을 이런 문서들이 차지하면 정상 건이 밀린다. → P2-13

---

**B-Gap-10. 컨슈머가 DB 상태를 신경 쓰지 않는다.**

DB가 내려간 상태에서도 리스너는 계속 소비하고, 매번 예외를 던지고, **매번 ack한다.** `insertIfAbsent`조차 실패했으니 Job 행도 없고 실패 기록도 없다. offset만 앞으로 나간다. → P0-5

### 2.3 아직 결정 안 난 것 — `attempt_count`를 누가 소모하는가 ★결정 필요

**현재 구현**(plan Task 3)은 재획득 경로를 하나로 합쳐놨다.

```sql
UPDATE indexing_job
SET status = 'PROCESSING', attempt_count = attempt_count + 1, ...
WHERE id = :jobId AND attempt_count < :maxAttempts AND (status IN ('PENDING','PROCESSING') OR ...)
```

즉 **워커 크래시 후 재획득도, 비즈니스 재시도도 똑같이 `attempt_count`를 1 올린다.** 워커가 5번 죽으면 파일에 아무 문제가 없어도 `FAILED`가 된다.

**태성님 지적**: 워커가 죽은 건 워커 책임이지 Job 책임이 아닌데 왜 Job의 한도를 깎는가. 타당하다.

**반대편 근거**(스펙 §1.4-(1)): poison pill이면 캡이 없을 때 재전달 → 재획득 → 크래시가 무한 반복되어 fleet 전체가 죽는다.

| 안 | 내용 | 장점 | 단점 |
|---|---|---|---|
| A. 현행 유지 | 모든 재획득이 `attempt_count` 소모 | 컬럼 추가 없음 | 인프라 사정으로 멀쩡한 Job이 `FAILED` |
| B. 크래시는 소모 안 함 | 크래시 재획득 시 증가 안 함 | 의미가 정확 | **poison pill 방어가 사라진다** |
| **C. 카운터 분리 (권장)** | `attempt_count`(재시도) + `crash_count`(크래시)를 나누고 각각 상한 | 둘 다 만족, 원인 분석도 쉬움 | 컬럼 1개 추가 |

**C를 권장한다.** B는 안 된다 — poison pill이 fleet을 죽이는 건 데이터 유실보다 큰 사고고, **책임 소재와 방어 장치는 별개 문제**다. A는 P2-10(인프라 실패는 한도를 소모하지 않는다)과 부딪혀서 어차피 예외 분기를 파야 한다.

```sql
ALTER TABLE indexing_job ADD COLUMN crash_count INTEGER NOT NULL DEFAULT 0;
```

**인라인 재시도 전환 후에는 의미가 더 선명해진다.**

- `attempt_count` — 리스너 안에서 도는 인라인 재시도 횟수. 상한 3.
- `crash_count` — Kafka 재전달로 재획득된 횟수(= 워커가 몇 번 죽었나). 상한 10.

`crash_count` 상한을 넉넉히 잡는 이유: 크래시는 배포·스케일링 같은 정상 운영 중에도 일어나므로 빡빡하면 오탐이 난다. poison pill이면 10회는 금방 도달한다.

---

## 3. Track B — 장애별 대응 방법

우선순위는 **"막지 않으면 데이터가 조용히 사라지는가"**를 첫 기준, **"장애를 감지할 수 있는가"**를 두 번째 기준으로 잡았다.

---

### P0-1. 인라인 재시도로 전환 — 재시도 폴러 제거 (B-Gap-1) ★구조 변경

**막는 장애**: 폴러 경로 크래시로 인한 영구 유실, DB 장애 중 ack되어 상태가 어긋난 Job

**핵심 아이디어**: 재시도를 **ack 이전**으로 옮긴다. 그러면 Job이 최종 상태에 도달할 때까지 대응하는 Kafka 메시지가 계속 커밋되지 않은 채 남고, 어느 시점에 죽든 리밸런스가 회수한다.

```
[Track A — 현재]
수신 → 처리 → 실패 → RETRY_WAIT 기록 → ack ★ → (30초 후) 폴러가 재시도
                                        └ 여기서 Kafka와 끊김 → 고아 가능

[Track B — 전환 후]
수신 → 처리 → 실패 → 5초 대기 → 재시도 → 실패 → 15초 대기 → 재시도 → 성공 → ack
      └────────────── 전 구간 미커밋. 언제 죽어도 재전달됨 ──────────────┘
```

```kotlin
@KafkaListener(topics = ["indexing"], id = "indexing")
fun onMessage(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
    val event = deserialize(record.value())

    var lastError: Exception? = null
    for (attempt in 1..properties.maxInlineAttempts) {          // 기본 3
        try {
            pipelineRunner.run(event)
            ack.acknowledge()                                    // 성공 → 여기서만 ack
            return
        } catch (e: PermanentIndexingException) {
            deadLetterService.record(record, e)                  // 재시도 무의미 → 즉시 종결
            ack.acknowledge()
            return
        } catch (e: DataAccessResourceFailureException) {
            ack.nack(Duration.ofSeconds(5))                      // DB 장애 → Kafka로 되돌림(P0-5)
            return
        } catch (e: Exception) {
            lastError = e
            if (attempt < properties.maxInlineAttempts) {
                Thread.sleep(backoffMillis(attempt))             // 5s → 15s → (45s)
            }
        }
    }

    // 인라인 예산 소진 → FAILED 종결 + DLQ (P0-3이 흡수처)
    failureService.markFailed(event, lastError)
    deadLetterService.record(record, lastError)
    ack.acknowledge()
}
```

**제거되는 것**

| 제거 대상 | 이유 |
|---|---|
| `IndexingRetryScheduler` (plan Task 17) | 재시도가 리스너 안에서 끝난다 |
| `RetryEventSource` (plan Task 17) | 이벤트를 DB에서 재구성할 필요가 없다. 원본 메시지가 손에 있다 |
| `findRetryWaitDue()` 쿼리 | 폴링 대상이 없다 |
| 초안의 고아 회수 스윕 | **문제 자체가 사라짐**(B-Gap-1) |

**유지되는 것**

- `RETRY_WAIT` 상태값과 `next_retry_at` 컬럼 — **관측용으로 계속 쓴다.** 인라인 재시도 대기 중임을 진행률 API(P1-7)에서 보여줘야 하고, 스키마·API 초안과의 정합도 유지된다. 다만 **이 값을 읽어서 깨우는 주체는 없다.**
- `start()`의 재획득 조건에 `RETRY_WAIT` 포함 — 인라인 재시도 대기(sleep) 중에 워커가 죽으면 재전달된 메시지가 `RETRY_WAIT` 상태의 Job을 만나기 때문이다. 이 경우 `crash_count`를 올린다(§2.3).

**주의 — 이 전환이 만드는 새 제약이 §P0-2다.** 재시도가 리스너를 점유하므로 `max.poll.interval.ms` 안에 들어가야 한다. 그 예산 설계 없이 이 코드만 넣으면 리밸런스 폭풍이 난다.

---

### P0-2. 재시도 예산 설계 — `max.poll.interval.ms` 안에 넣기 ★P0-1의 전제

**막는 장애**: 인라인 재시도가 poll 간격을 넘겨 발생하는 리밸런스 폭풍

**흔한 오해부터 정리한다.** "배치 안에서 Job들을 병렬로 돌리면 배치 시간이 가장 오래 걸리는 Job 하나 수준으로 줄어든다"는 맞는 말이지만, **제약을 푸는 데는 도움이 안 된다.** 문제는 Job들의 합이 아니라 **Job 하나가 혼자 쓰는 시간**이기 때문이다.

```
Track A의 백오프를 그대로 인라인으로 옮기면 (5회, 지수 백오프)
  대기  30 + 60 + 120 + 240 + 480 = 930초
  처리  120초 × 5회               = 600초
  ───────────────────────────────────────
  Job 하나가                        1530초

max.poll.interval.ms = 600초 → 병렬이든 뭐든 이미 초과
```

**따라서 예산을 먼저 정하고 재시도 횟수·백오프를 거기 맞춰 잘라야 한다.**

```
처리시간 × 시도횟수 + 백오프 총합 + 마진 < max.poll.interval.ms
```

| 항목 | 값 | 근거 |
|---|---|---|
| 대용량 문서 1회 처리 | ~120초 | 다운로드+파싱+청킹+임베딩. **P0-4의 파싱 타임아웃으로 상한이 보장돼야 한다** |
| 인라인 재시도 | **3회** | 아래 계산 결과 |
| 백오프 | 5s → 15s → 45s (총 65초) | 지터 ±50% 포함 시 최대 ~98초 |
| **합계** | 120×3 + 98 = **458초** | 600초 대비 마진 142초 ✅ |

```yaml
indexing:
  retry:
    max-inline-attempts: 3
    inline-backoff-base: PT5S       # 5s → 15s → 45s (3배씩)
    inline-backoff-jitter: 0.5      # 0.5~1.0 배
  consumer:
    max-poll-interval-ms: 600000    # 이 값을 바꾸면 위 예산을 다시 계산할 것
```

**조절 손잡이는 두 개다. 하나를 늘리면 다른 하나가 줄어든다.**

| 손잡이 | 늘리면 | 대가 |
|---|---|---|
| `max.poll.interval.ms`를 900초로 | 재시도 4~5회 가능 | **hang 감지가 15분으로 늦어진다**(B-Gap-4 악화). 진짜 죽은 워커의 파티션 회수도 그만큼 늦어짐 |
| 인라인 재시도 횟수 | 흡수 범위 확대 | 파티션 헤드 블로킹 증가(B-Gap-6), 예산 초과 위험 |

**흡수 범위와 그 바깥**

```
✅ 인라인으로 흡수:  순간적 429, 커넥션 리셋, 짧은 네트워크 흔들림,
                    임베딩 서버 재시작(수십 초)

❌ 흡수 못 함:      임베딩 서버 5분 이상 다운, 장기 페일오버
                    → FAILED + DLQ → 관리자 일괄 재처리 (P0-3)
```

**Track A 대비 명백한 후퇴다.** Track A는 5회 × 최대 480초 백오프로 총 15분 이상을 버틸 수 있었지만, 인라인은 1분 남짓이다. **이 후퇴를 받아들이는 대신 고아 유실 경로를 없애고 스케줄러를 지운다** — 자동 복구 범위를 줄이고 사람이 개입하는 지점을 만드는 트레이드오프이므로, P0-3(DLQ 일괄 재처리)이 반드시 함께 가야 한다.

**동시성 제어**: 배치 컨슈머로 전환해 배치 안의 Job을 병렬 처리하는 것은 **처리량** 목적으로는 유효하다(예산 문제와 무관). 임베딩 쿼터는 세마포어로 동시 호출 수를 제한한다.

```kotlin
private val embeddingSemaphore = Semaphore(properties.maxConcurrentEmbeddingCalls)
```

다만 **배치 전체가 끝나야 커밋되므로**, 배치 안에 재시도로 458초를 쓰는 Job이 하나 있으면 나머지가 다 끝나도 다음 `poll()`을 못 한다. 배치 크기를 키울수록 이 위험이 커진다 — 처음에는 `max-poll-records`를 작게(3~5) 유지하고 P1-8의 lag 지표를 보며 조정한다.

---

### P0-3. DLQ + 일괄 재처리 (B-Gap-2, B-Gap-8) ★P0-1의 안전망

**막는 장애**: 인라인 예산을 초과한 장기 장애로 인한 대량 `FAILED` 방치, 역직렬화/검증 실패 이벤트의 완전 소실

P0-2에서 재시도 범위를 1분으로 줄였으므로, **그 바깥을 받아줄 곳이 반드시 있어야 한다.** Track A에서는 "있으면 좋은 것"이었지만 이제는 전환의 필수 조건이다.

```sql
CREATE TABLE dead_letter (
    id                  BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source_event_id     UUID,
    document_id         BIGINT,
    document_version_id BIGINT,
    error_code          VARCHAR(100) NOT NULL,
    error_message       TEXT,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    original_payload    TEXT NOT NULL,      -- ★ 이게 없으면 되살릴 수 없다
    kafka_topic         VARCHAR(255),
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    trace_id            VARCHAR(255),
    resolved_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dead_letter_unresolved
    ON dead_letter (created_at) WHERE resolved_at IS NULL;
```

**DLQ로 보낼 대상**

| 경로 | Track A 현재 | Track B |
|---|---|---|
| **인라인 재시도 3회 소진** | (없던 경로) | **DLQ 적재 + `FAILED`** ← 신설, 가장 많이 탈 경로 |
| `DeserializationException` | 로그 + ack | DLQ 적재(원문 보존) + ack |
| `InvalidEventException` | 로그 + ack | DLQ 적재 + ack |
| `PermanentIndexingException` | (분류 없음) | DLQ 적재 + 즉시 `FAILED` |
| `acquireJobId()`가 null (B-Gap-2) | 로그 + ack | DLQ 적재 — "선행 Job에 흡수됨"으로 기록 |

**일괄 재처리가 핵심이다.** 임베딩 서버가 10분 다운되면 수십~수백 건이 한꺼번에 DLQ에 들어간다. 한 건씩 누르는 UI로는 감당이 안 된다.

```
POST /admin/indexing/dead-letters/retry
  { "errorCode": "EMBEDDING_SERVER_ERROR", "since": "2026-08-17T09:00:00Z" }

  1. 조건에 맞는 미해결 dead_letter를 조회
  2. 각각의 original_payload를 Kafka 토픽으로 재발행
     ★ 여기서는 Kafka로 되돌린다 — 인라인 구조에서는 재처리 트리거가 Kafka뿐이므로
  3. indexing_job: status=PENDING, attempt_count=0, crash_count=0으로 리셋
  4. dead_letter.resolved_at 기록
```

**Track A 설계와 달라지는 지점**: 스펙 §3.8은 "Kafka를 다시 타지 않는다"였다. 폴러가 있었으니 DB만 건드리면 됐기 때문이다. **폴러를 없앴으므로 재처리는 Kafka 재발행으로 해야 한다.** 중복 발행 위험은 `source_event_id UNIQUE`(A-2)가 막아주므로 안전하다.

**알림**: `indexing_dlq_total`이 급증하면 즉시 알림. 인라인 구조에서는 이 지표가 곧 "지금 장애 중"의 신호다.

---

### P0-4. 리소스 가드 — 파일 크기 상한 + 파싱 타임아웃 + 스트리밍 (B-Gap-3, B-Gap-4) ★P0-2의 전제

**막는 장애**: OOM으로 인한 fleet 붕괴, 악성 파일에 의한 파티션 잠식, **그리고 재시도 예산 붕괴**

P0-2의 예산 계산은 "1회 처리 시간 ≤ 120초"를 전제로 한다. **이 상한이 보장되지 않으면 예산 전체가 무의미하다.** 그래서 이 항목이 P0-2와 세트다.

**(a) 파일 크기 상한 — 다운로드 *전에* 판정**

```kotlin
// document_version.file_size는 이미 DB에 있다
if (documentVersion.fileSize > properties.maxFileSizeBytes) {
    throw PermanentIndexingException("FILE_TOO_LARGE", "...")
}
```

**(b) 파싱 타임아웃 — 별도 실행자에서 시간을 건다**

```kotlin
private val parseExecutor = Executors.newFixedThreadPool(concurrency)

fun parseWithTimeout(parser: DocumentParser, source: Path): List<ParsedBlock> {
    val future = parseExecutor.submit<List<ParsedBlock>> { parser.parse(source).toList() }
    return try {
        future.get(properties.parseTimeout.seconds, TimeUnit.SECONDS)   // 60초
    } catch (e: TimeoutException) {
        future.cancel(true)
        throw TransientIndexingException("PARSE_TIMEOUT", "...")
    }
}
```

```yaml
indexing:
  limits:
    max-file-size-bytes: 209715200      # 200MB
    parse-timeout: PT60S                # 1회 처리 120초 예산 중 파싱 몫
    download-timeout: PT30S             # 기존
    embedding-timeout: PT30S            # 태성 협업
```

세 타임아웃 합(120초)이 P0-2의 "1회 처리 시간" 상한과 일치해야 한다. **`parse-timeout`을 늘리면 P0-2의 예산 표를 다시 계산해야 한다.**

`future.cancel(true)`가 네이티브 루프를 실제로 멈추지는 못한다는 걸 인정해야 한다 — 스레드가 누수될 수 있다. 현실적 목표는 "메시지 처리를 진행시키는 것"이고, 누수 스레드 수를 메트릭으로 노출해 임계 초과 시 워커를 재시작한다.

**중요**: 타임아웃 처리는 워커를 **살려두므로** `TransientIndexingException`으로 인라인 재시도를 탄다. hang을 방치해 리밸런스가 나는 것과 결정적으로 다르다.

**(c) 스트리밍 복원**

- `parser.parse(...)`의 `Sequence`를 청커까지 흘린다(`.toList()` 제거)
- PDFBox는 `Loader.loadPDF(File)` 오버로드 사용
- 다운로드는 임시 파일로 스풀, 해시는 `DigestInputStream`으로 스트리밍 계산
- 임시 파일은 `finally`에서 삭제 + 컨테이너 기동 시 임시 디렉터리 비우기

---

### P0-5. 컨슈머 게이트 + `nack` (B-Gap-10)

**막는 장애**: DB 다운 중 메시지 소각, 페일오버 구간 유실

**(a) DB 장애는 ack이 아니라 `nack`이다**

Track A는 무조건 ack해서, DB에 아무것도 못 적은 실패까지 "봤다"고 표시한다. 워커는 기록 실패를 **아는데도** ack한다.

다만 "ack을 안 하는 것"만으로는 재전달이 안 된다. 커밋 안 된 offset은 그대로여도 컨슈머의 읽기 위치(position)는 이미 앞으로 갔기 때문에, 그 메시지는 리밸런스 전까지 다시 오지 않는다. **되감기(seek)가 필요하고, Spring Kafka가 그걸 메서드로 제공한다.**

```kotlin
ack.nack(Duration.ofSeconds(5))
// ① 이번 poll의 남은 레코드를 버리고
// ② 실패한 레코드 위치로 파티션을 되감고
// ③ 5초 쉬었다 다시 폴링 → 그 메시지가 다시 온다
```

`acknowledge()`와 `nack()`이 같은 인터페이스에 **따로** 있다는 사실 자체가 "ack 생략 ≠ 재전달"의 증거다.

**(b) 컨테이너 pause로 hot loop 방지**

`nack`의 sleep은 짧은 흔들림만 흡수한다. DB가 오래 죽어 있으면 게이트가 폴링 자체를 멈춘다.

```kotlin
@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
) {
    @Scheduled(fixedDelay = 5_000)
    fun check() {
        val healthy = runCatching { jdbcTemplate.queryForObject("SELECT 1", Int::class.java) }.isSuccess
        val container = registry.getListenerContainer("indexing") ?: return
        when {
            !healthy && container.isRunning -> { container.pause(); log.warn("DB down — consumer paused") }
            healthy && container.isPauseRequested -> { container.resume(); log.info("DB up — consumer resumed") }
        }
    }
}
```

`pause()`는 폴링 자체를 멈추지 않으므로(heartbeat 계속) 리밸런스가 나지 않는다. `stop()`을 쓰면 리밸런스가 터진다.

> **참고**: "스케줄러 제거"가 목표라도 이 게이트는 `@Scheduled` 하나가 남는다. 다만 이건 **작업 분배용이 아니라 헬스체크**이므로 P0-1이 없애려는 대상과 성격이 다르다. Kafka 컨슈머 헬스 인디케이터로 대체할 수도 있으나, 명시적인 편이 디버깅에 낫다.

**(c) always-ack 원칙의 예외 명문화**

> **DB에 아무것도 기록하지 못한 실패는 ack하지 않고 `nack`한다.** 원칙은 "실패를 **기록했다면** 반드시 ack"이고, 기록조차 못 했으면 되돌리는 게 일관된다.

---

### P0-6. 에러 분류 (Permanent / Transient) ★P0-1의 분기 조건

**막는 장애**: 영구 실패에 인라인 재시도 낭비

인라인 구조에서는 이게 **리스너의 분기 조건 자체**다. `PermanentIndexingException`이면 즉시 DLQ로 보내고, 아니면 재시도한다(P0-1 코드 참고). 분류가 없으면 0바이트 PDF에도 3회 × 백오프를 쓰고, 그동안 파티션이 막힌다.

```kotlin
sealed class IndexingException(val code: String, message: String, cause: Throwable? = null)
    : RuntimeException(message, cause) { abstract val permanent: Boolean }

class PermanentIndexingException(...) : IndexingException(...) { override val permanent = true }
class TransientIndexingException(...) : IndexingException(...) { override val permanent = false }
```

| 분류 | 코드 | 발생 지점 |
|---|---|---|
| Permanent | `EMPTY_EXTRACTION` | `ChunkGuard` |
| Permanent | `CHUNK_LIMIT_EXCEEDED` | `ChunkGuard` |
| Permanent | `HASH_MISMATCH` | `ContentIntegrityException` |
| Permanent | `UNSUPPORTED_MIME` | `DocumentParserRegistry` |
| Permanent | `CORRUPTED_FILE` | 파서가 열지 못함 |
| Permanent | `FILE_TOO_LARGE` | P0-4 |
| Permanent | `DOCUMENT_DELETED` | `failActiveJobsForDocument()` |
| Transient | `STORAGE_TIMEOUT` / `STORAGE_SERVER_ERROR` | S3 |
| Transient | `EMBEDDING_RATE_LIMIT` (429) | 임베딩 |
| Transient | `EMBEDDING_SERVER_ERROR` (5xx) | 임베딩 |
| Transient | `PARSE_TIMEOUT` | P0-4 |
| (별도) | `DB_CONNECTION_LOST` | → `nack`(P0-5). 재시도 예산을 쓰지 않는다 |

**429의 `Retry-After` 헤더**가 인라인 백오프보다 크면 **인라인 재시도를 포기하고 즉시 DLQ로 보낸다.** 예산 안에 못 들어가는 대기를 리스너에서 버티면 안 된다.

**태성님께 요청**: `IndexingProcessor`에서 임베딩 실패를 위 두 예외로 감싸주기. `Retry-After` 값도 예외에 실어주면 위 판단에 쓴다.

---

### P1-7. 진행률 API / SSE (B-Gap-7)

```sql
ALTER TABLE indexing_job ADD COLUMN phase            VARCHAR(30);   -- DOWNLOADING/PARSING/CHUNKING/EMBEDDING/PUBLISHING
ALTER TABLE indexing_job ADD COLUMN total_chunks     INTEGER;
ALTER TABLE indexing_job ADD COLUMN processed_chunks INTEGER;
```

인라인 재시도 구조에서 **더 중요해진다.** 재시도가 리스너 안에서 도는 동안 외부에서는 아무것도 안 보이므로, `phase`와 `attempt_count`를 노출하지 않으면 사용자는 "멈춘 것"으로 인식한다.

- `GET .../indexing` — 폴링
- `GET .../indexing/events` — SSE

버전 목록 응답에 `searchable: boolean` + `indexingStatus`를 함께 노출해 "최신 버전을 올렸지만 실패해서 이전 버전이 검색된다"는 상태를 보이게 한다.

---

### P1-8. 메트릭 + 알림

| 메트릭 | 타입 | 알림 | 무엇을 알려주나 |
|---|---|---|---|
| `indexing_dlq_total{errorCode}` | Counter | **즉시** | 인라인 예산 밖 장애 발생. 재처리 필요 |
| `kafka_rebalance_total` | Counter | **즉시** | **재시도 예산이 poll 간격을 넘고 있다**(P0-2 실패 신호) |
| `indexing_inline_retry_total{attempt}` | Counter | 급증 시 | 어느 시도에서 성공/실패하나. 예산 튜닝 근거 |
| `indexing_job_duration_seconds{phase}` | Histogram | p99 감시 | **1회 처리 120초 가정이 유효한지** 검증 |
| `indexing_job_crash_count_high` | Gauge | 임계 초과 | poison pill 의심(§2.3) |
| `outbox_reconciled_total` | Counter | **즉시** | Kafka 경로 유실 |
| `kafka_consumer_lag{partition}` | Gauge | 지속 증가 | 파티션 hot spot(B-Gap-6), 배치 블로킹 |
| `db_health_gate_paused_total` | Counter | **즉시** | DB가 흔들리고 있다 |
| `parse_thread_leaked` | Gauge | 임계 초과 | P0-4의 취소 실패 누적 |

**`kafka_rebalance_total`과 `indexing_job_duration_seconds` p99가 이번 전환의 핵심 지표다.** 전자가 오르면 예산을 넘긴 것이고, 후자가 120초를 넘으면 예산 가정이 깨진 것이다. 둘 다 대시보드 상단에 둔다.

end-to-end "업로드 → 검색 가능" 시간도 함께.

---

### P1-9. Outbox 보정 배치

**막는 장애**: Kafka 브로커 장애로 "`PUBLISHED`인데 아무도 처리 안 함"

```sql
SELECT e.* FROM outbox_event e
LEFT JOIN indexing_job j ON j.source_event_id = e.id
WHERE e.status = 'PUBLISHED' AND j.id IS NULL
  AND e.published_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
ORDER BY e.published_at LIMIT 100 FOR UPDATE SKIP LOCKED;
```

```sql
CREATE INDEX idx_outbox_published ON outbox_event (published_at) WHERE status = 'PUBLISHED';
```

**경계 주의**: `outbox_event`는 `doc-relay` 소유다(스펙 §0.2). **`doc-relay` 담당과 사전 합의 필수**이고, 경계상 그쪽에 두는 게 깨끗하므로 **먼저 그쪽에 제안한다.** 합의 포인트는 리셋 조건(5분)이 자체 재시도 주기와 겹치지 않는지, `retry_count`를 안 건드릴 것(= `DEAD` 판정 오염 방지).

---

### P2-10. DB 페일오버 대응 (2차 과제)

```
DB_URL=jdbc:postgresql://node1:5432,node2:5432/osscontest?targetServerType=primary&loadBalanceHosts=false
```

```yaml
spring:
  datasource:
    hikari:
      connection-timeout: 5000
      validation-timeout: 3000
      keepalive-time: 30000
      max-lifetime: 600000
```

**인라인 재시도 구조에서 페일오버 대응이 오히려 단순해진다.**

```
페일오버 발생
  → DataAccessResourceFailureException
  → nack(5초)  → 메시지가 Kafka로 되돌아감 (P0-5)
  → DbHealthGate가 컨테이너 pause
  → promote 완료 → HikariCP가 새 Primary로 재연결 → resume
  → 되돌려둔 메시지부터 재처리
```

**고아 회수가 필요 없다.** Track A 구조였다면 페일오버 중 `PROCESSING`으로 남은 Job을 청소해야 했지만, 인라인 구조에서는 그 Job에 대응하는 미커밋 메시지가 항상 있다(§2.2 불변조건).

인프라 실패는 `attempt_count`를 소모하지 않는다 — `nack` 경로는 애초에 재시도 카운터를 안 건드린다(§2.3 C안과 자연스럽게 맞물린다).

**측정 지표**

| 지표 | 측정 방법 |
|---|---|
| 데이터 유실 건수 | 페일오버 중 업로드한 N건 중 최종 완료 수 → **N/N** |
| RTO | Primary kill → 첫 정상 완료 |
| 되돌린 메시지 수 | `nack` 호출 카운터 |
| 컨슈머 복구 시간 | pause → resume |

---

### P2-11. 시각 통일 (B-Gap-5)

인라인 전환으로 심각도가 낮아졌지만 정리는 필요하다.

- `next_retry_at`을 관측용으로 계속 쓴다면 UPDATE 안에서 `CURRENT_TIMESTAMP + :interval`로 계산
- `LocalDateTime` → `Instant`/`OffsetDateTime`(`TIMESTAMPTZ`와 정합)
- 워커 JVM `TZ=UTC` 고정

---

### P2-12. 파티션 / 처리량 튜닝 (B-Gap-6)

인라인 재시도는 파티션 헤드 블로킹을 악화시키므로, **P1-8의 lag 지표를 보며 조정한다.**

| 문제 | 대응 |
|---|---|
| 재시도 중인 Job이 배치 커밋을 붙잡음 | `max-poll-records`를 작게(3~5) 유지. 배치 내 병렬 처리로 완화 |
| 대용량 문서가 파티션 점유 | 파일 크기별 토픽 분리(`indexing.bulk`) |
| 특정 테넌트 독점 | 테넌트별 동시 처리 상한(세마포어) |
| 파티션 증설 시 순서 보장 붕괴 | 증설 전 컨슈머 정지 → 백로그 소진 → 증설 → 재개 |

실측 없이 미리 튜닝하지 않는다.

---

### P2-13. 삭제 스윕 무한 재시도 방어 (B-Gap-9)

```sql
ALTER TABLE document ADD COLUMN chunk_purge_failed_count INTEGER NOT NULL DEFAULT 0;
```

실패 시 카운터 증가, 스윕 쿼리에 `AND chunk_purge_failed_count < :max` 추가, 초과분은 DLQ + 알림.

> **참고**: 삭제 스윕(`DocumentDeletionSweepScheduler`)은 **제거하지 않는다.** 재시도 폴러와 달리 이건 "이벤트 자체가 유실됐을 때"의 백업이고, 유령 청크가 검색에 노출되는 건 정합성·보안 문제라 백업이 필요하다. P0-1이 없애는 건 재시도 폴러 하나다.

---

### P2-14. 비동기 경계 트레이싱

- API 서버: `SET LOCAL app.trace_id = :traceId` (스펙 §6.3)
- 트리거: `current_setting('app.trace_id', true)` → `outbox_event.trace_id`
- `doc-relay`: Kafka payload `traceId`로 전파
- 워커: MDC 세팅 → `indexing_job.trace_id` 저장 → 모든 로그에 포함

인라인 재시도는 같은 스레드에서 도니 MDC가 자동으로 유지된다 — 폴러 구조보다 트레이싱이 쉬워진다.

---

## 4. 구현 순서

| 순서 | 항목 | 선행 | 이유 |
|---|---|---|---|
| **0** | **§2.3 `attempt_count` 정책 결정** | — | 재획득 쿼리 모양이 여기 달렸다. 코드 쓰기 전에 정할 것 |
| 1 | **P0-6 에러 분류** | 0 | 인라인 리스너의 **분기 조건 자체**. 없으면 P0-1을 못 쓴다 |
| 2 | **P0-4 리소스 가드** | 1 | "1회 처리 ≤120초" 상한이 있어야 P0-2 예산이 성립 |
| 3 | **P0-2 재시도 예산 설계** | 1, 2 | 숫자를 확정해야 P0-1 코드를 쓸 수 있다 |
| 4 | **P0-3 DLQ + 일괄 재처리** | 1 | 예산 밖 장애의 흡수처. **없으면 전환 자체가 성립 안 함** |
| 5 | **P0-1 인라인 재시도 전환 + 폴러 제거** | 1~4 | 앞의 넷이 다 있어야 안전하게 전환된다 |
| 6 | **P0-5 컨슈머 게이트 + `nack`** | 5 | 리스너 구조가 바뀐 뒤에 얹는 게 충돌이 적다 |
| 7 | **P1-8 메트릭** | 5, 6 | 전환이 실제로 안전한지 확인할 유일한 수단. `kafka_rebalance_total` 필수 |
| 8 | **P1-7 진행률 API/SSE** | 스키마 3컬럼 | 인라인 재시도 가시성 |
| 9 | **P1-9 Outbox 보정** | doc-relay 합의 | 경계를 넘음 |
| 10 | **P2-10 DB 페일오버** | 5, 6 | 5·6의 재활용 + 인프라 선행 |
| 11 | **P2-11 시각 통일** | — | 미루면 재현 안 되는 버그로 돌아온다 |
| 12 | **P2-13 삭제 스윕 방어** | 4 | DLQ가 종착지 |
| 13 | **P2-14 트레이싱** | API 서버 `SET LOCAL` | 외부 의존 |
| 14 | **P2-12 파티션 튜닝** | 7 | 실측 후에만 |

**5번(전환)을 1~4번 뒤에 두는 이유**: 에러 분류 없이 전환하면 영구 실패에도 3회 재시도로 파티션이 막히고, 리소스 가드 없이 전환하면 처리 시간 상한이 없어 예산이 무너지고, DLQ 없이 전환하면 예산 밖 장애가 갈 곳이 없다. **전환은 마지막에 스위치를 켜는 작업이다.**

---

## 5. 테스트 시나리오

### 5.0 (회귀) 워커 크래시는 Kafka가 회수한다

```bash
docker kill -s SIGKILL worker-1   # 리스너가 처리 중일 때
```
**기대**: 리밸런스 → 다른 워커가 재처리 → 완료. 청크 중복 0건. 전환 후에도 이 경로는 그대로여야 한다.

### 5.1 인라인 재시도 — 예산 안에서 성공 (P0-1, P0-2)

임베딩을 2회 실패 후 성공하도록 WireMock 구성.

```sql
SELECT status, attempt_count FROM indexing_job WHERE id = :jobId;
-- 기대: COMPLETED, attempt_count = 3
```
**기대**: Kafka에 추가 메시지 발행 없음. `kafka_rebalance_total` **증가하지 않음**. 폴러가 없어도 완료된다.

### 5.2 ★ 재시도 예산 초과 검증 (P0-2) — 가장 중요

인위적으로 1회 처리 시간을 250초로 만든 뒤(대용량 파일 + 느린 임베딩) 3회 재시도를 유발한다. 총 750초 + 백오프 > 600초.

**기대**: `kafka_rebalance_total`이 증가하고 파티션이 재할당된다. **이 테스트는 "실패해야 정상"이 아니라, 예산 계산이 맞는지 확인하는 경계 테스트다.** 여기서 리밸런스가 나면 P0-2의 숫자를 다시 잡아야 한다.

그리고 정상 예산(120초 × 3 + 백오프)에서는 리밸런스가 **나지 않아야** 한다. 두 조건을 모두 확인한다.

### 5.3 ★ 폴러 경로 고아가 더 이상 안 생긴다 (B-Gap-1 해소 검증)

```bash
# 임베딩 실패로 인라인 재시도 대기(sleep) 중일 때 워커를 죽인다
docker kill -s SIGKILL worker-1
```

**기대**: ack이 아직 안 됐으므로 리밸런스 → 재전달 → 다른 워커가 처음부터 재처리 → 완료. `indexing_job`에 `PROCESSING`으로 남는 행이 **없다.**

Track A 구조였다면 여기서 고아가 생겼다. **이 테스트가 전환의 성과를 보여주는 지점이다.**

### 5.4 에러 분류 (P0-6)

| 입력 | 기대 |
|---|---|
| 0바이트 PDF | `attempt_count = 1`에서 즉시 `FAILED('CORRUPTED_FILE')` + DLQ. 재시도 없음 |
| 스캔 PDF | 즉시 `FAILED('EMPTY_EXTRACTION')`, **검색 버전 전환 안 됨** |
| 300MB 파일 | 다운로드 **전에** `FAILED('FILE_TOO_LARGE')` — S3 트래픽 0 |
| 429 + `Retry-After: 300` | 인라인 백오프보다 크므로 재시도 포기 → 즉시 DLQ |

### 5.5 DLQ 왕복 + 일괄 재처리 (P0-3)

```bash
# 임베딩 서버를 5분간 정지 → 그 사이 문서 30건 업로드
```
**기대**: 30건 모두 인라인 3회 소진 → `FAILED` + DLQ 적재. `indexing_dlq_total` 30 증가 + 알림 발생.

```
POST /admin/indexing/dead-letters/retry
  { "errorCode": "EMBEDDING_SERVER_ERROR" }
```
**기대**: 30건이 Kafka로 재발행 → 전부 `COMPLETED`. `resolved_at` 기록. **이 왕복이 안 되면 P0-1 전환을 하면 안 된다.**

### 5.6 DB 장애 → `nack` (P0-5)

```bash
docker stop postgres
# 문서 3건 업로드
sleep 30
docker start postgres
```
**기대**: `nack` 호출 → 컨슈머 pause → 복구 후 resume → 3건 전부 처리. **Kafka lag이 3 → 0.** `indexing_job`에 유령 `PROCESSING` 행 없음.

```sql
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE application_name = 'worker' AND pid <> pg_backend_pid();
```

### 5.7 리소스 가드 (P0-4)

- 1만 페이지 PDF → 힙 상한 내 유지
- 순환 참조 PDF → `PARSE_TIMEOUT`(60초) 후 다음 메시지 정상 처리. **`kafka_rebalance_total` 증가 없음**
- `indexing_job_duration_seconds` p99 < 120초 확인 → P0-2 예산 가정 검증

### 5.8 Kafka 장애 + Outbox 보정 (P1-9)

```bash
docker stop kafka   # 문서 3건 업로드 → outbox PENDING
docker start kafka
```
**기대**: 자동 발행 → 완료. 유실 0.

```sql
UPDATE outbox_event SET status='PUBLISHED', published_at = CURRENT_TIMESTAMP - INTERVAL '10 minutes' WHERE id=:id;
DELETE FROM indexing_job WHERE source_event_id = :id;
```
**기대**: 보정 배치가 `PENDING` 리셋 → 재발행 → 완료.

### 5.9 카오스 테스트 (최종 성적표)

```bash
while true; do
  sleep $((RANDOM % 60 + 30))
  docker kill -s SIGKILL "worker-$((RANDOM % 3 + 1))"
  sleep 10; docker compose up -d
done
```

30분 후:

```sql
-- ① 모든 Job이 최종 상태에 도달했는가
SELECT status, count(*) FROM indexing_job GROUP BY status;
-- ★ PROCESSING이 하나라도 남아 있으면 = 불변조건 위반 = 전환 실패
--   (인라인 구조에서는 리밸런스가 전부 회수해야 정상)

-- ② 검색 버전의 청크 수가 실측과 일치하는가
SELECT d.id, j.total_chunks, count(c.id) AS actual
FROM document d
JOIN document_version v ON v.id = d.searchable_version_id
JOIN indexing_job j ON j.document_version_id = v.id AND j.status = 'COMPLETED'
LEFT JOIN document_chunk c ON c.document_version_id = v.id
GROUP BY d.id, j.total_chunks
HAVING j.total_chunks <> count(c.id);
-- 0건이어야 한다
```

**①에서 `PROCESSING` 잔존이 0인 것이 이번 전환의 최종 증명이다.** Track A 구조에서는 여기에 고아가 남을 수 있었다.

---

## 6. 데모 시나리오

**① 워커 강제 종료 → Kafka 자동 재할당** *(Track A 성과)*
100페이지 PDF 인덱싱 중 `docker kill` → 리밸런스 → 다른 워커가 처음부터 재처리 → 완료.
*설명 포인트*: **"ack을 마지막에 하기 때문에 Kafka가 알아서 넘겨준다."** 여기 우리가 만든 장치는 없고, 그게 오히려 포인트 — 수렴 설계 덕분에 lease도 heartbeat도 소유권 재확인도 필요 없었다.

**② 재시도 중 워커 사망 → 그래도 회수됨** *(Track B 전환 성과)*
임베딩 실패로 인라인 재시도 대기 중 `docker kill` → 리밸런스 → 재처리 → 완료.
*설명 포인트*: **"재시도를 ack 이전으로 옮겼더니 회수 장치가 필요 없어졌다."** Track A 구조였다면 여기서 Job이 영원히 `PROCESSING`에 갇혔다는 것을 대비해서 보여준다. **구조를 바꿔 문제를 없앤 사례**라 설계 얘기로 풀기 좋다.

**③ 손상 파일 → 즉시 실패 + 재처리**
스캔 PDF → `EMPTY_EXTRACTION`으로 **1회 만에** FAILED + DLQ → **기존 검색 버전 유지 확인** → 정상 파일 교체 후 재처리 → 완료.
*설명 포인트*: `attempt_count = 1`. 에러 분류 유무의 차이.

**④ 임베딩 서버 장기 다운 → DLQ 일괄 복구**
임베딩 서버 5분 정지 상태에서 문서 30건 업로드 → 전부 DLQ → 서버 복구 → **버튼 한 번으로 30건 일괄 재처리** → 전부 완료.
*설명 포인트*: **"자동 재시도 범위를 1분으로 줄인 대신, 그 바깥은 사람이 한 번에 되살린다."** 스케줄러를 없앤 대가와 그 대가를 어떻게 감당하는지를 같이 보여준다.

**⑤ DB 페일오버 → 무손실 복구** (2차)
업로드 중 Primary kill → `nack`으로 메시지 되돌림 → 컨슈머 pause → promote → resume → 전부 완료.
*설명 포인트*: 유실 0, RTO. 고아 청소가 필요 없다는 점도 함께.

---

## 7. 다른 담당자에게 요청할 것

### 7.1 태성님 (`IndexingProcessor`)

- **§2.3 `attempt_count` 정책 결정** — C안(카운터 분리) 제안. 이게 정해져야 착수 가능
- **§3 P0-1 구조 변경 합의** — 재시도 폴러를 없애고 인라인으로 옮기는 것, 그리고 그 대가(자동 재시도 범위가 15분 → 1분으로 축소, 대신 DLQ 일괄 재처리)에 대한 동의
- 임베딩 실패를 `TransientIndexingException`/`PermanentIndexingException`으로 감싸기. 429의 `Retry-After` 값도 예외에 실어주기
- **임베딩 호출에 타임아웃 30초** — P0-2 예산의 구성 요소
- `indexing_job.processed_chunks` 갱신(P1-7)
- 벡터 차원 불일치는 `Permanent`

### 7.2 스키마 / API 서버 담당

| 대상 | 요청 |
|---|---|
| `dead_letter` 테이블 | **신규 생성 (P0-3). 전환의 필수 선행** |
| `indexing_job` | `crash_count` 추가(§2.3 C안) |
| `indexing_job` | `phase`, `total_chunks`, `processed_chunks` 추가(P1-7) |
| `outbox_event` | `idx_outbox_published` 부분 인덱스(P1-9) |
| `document` | `chunk_purge_failed_count` 추가(P2-13) |
| 트랜잭션 | `SET LOCAL app.trace_id`(P2-14) |
| 재처리 API | 게이트 조건을 `status = 'FAILED'`로(스펙 §6.3) |
| **관리자 API** | **DLQ 일괄 재처리 엔드포인트(P0-3)** |

### 7.3 `doc-relay` 담당

- **P1-9 Outbox 보정 배치의 소유권 합의.** 경계상 `doc-relay` 쪽이 깨끗하므로 먼저 제안
- **DLQ 재처리가 Kafka 재발행 방식**으로 바뀌었음을 공유(스펙 §3.8의 "Kafka를 다시 타지 않는다"가 폴러 제거로 뒤집힘). 재발행 시 `source_event_id`를 원본 그대로 쓰므로 중복 Job은 생기지 않는다
- 발행 실패 지속 시 서킷 브레이커

### 7.4 인프라 담당

- Kafka를 독립 배포 단위로(스펙 §6.5)
- OpenSQL 클러스터 + 페일오버 절차(P2-10)
- Toxiproxy를 개발 환경 compose에 포함
- 워커 컨테이너 `TZ=UTC` 고정(P2-11)

---

## 부록. Track B 핵심 원칙

1. **`PROCESSING`/`RETRY_WAIT`인 Job에는 항상 대응하는 미커밋 Kafka 메시지가 있다.** 이번 전환의 불변조건이자, 고아 회수라는 장치를 통째로 없앤 근거다. **ack 이후에 Job을 다시 활성 상태로 만드는 코드를 쓰지 않는다** — 그 순간 이 불변조건이 깨지고 회수 장치가 다시 필요해진다.
2. **Kafka가 이미 해주는 일을 다시 만들지 않는다.** ack이 처리 완료 후이므로 워커 크래시 복구는 Kafka의 몫이다. 우리 장치는 Kafka가 손댈 수 없는 구간만 대상으로 하고, 그 구간은 이제 없다.
3. **문제를 막을 장치를 만들기 전에, 문제가 안 생기는 구조가 있는지 먼저 본다.** 고아 회수 스윕을 만드는 대신 재시도를 ack 이전으로 옮겼다. 장치를 하나 더 얹는 것보다 원인을 없애는 쪽이 유지보수가 싸다.
4. **재시도는 `max.poll.interval.ms` 예산 안에서만 한다.** 이 예산을 넘기는 순간 리밸런스가 나고, 리밸런스는 재시도의 대체재가 아니라 장애 확산 경로다. 예산 밖은 DLQ가 받는다.
5. **자동 복구를 줄인 만큼 수동 복구를 쉽게 만든다.** 인라인 재시도는 1분밖에 못 버티므로, DLQ 일괄 재처리가 없으면 이 설계는 성립하지 않는다. 둘은 한 세트다.
6. **기록하지 못한 실패는 ack하지 않고 `nack`한다.** always-ack 원칙의 유일한 예외이자, 그 원칙을 일관되게 만드는 조항이다.
7. **책임 소재와 방어 장치는 별개다.** "워커가 죽은 건 Job 잘못이 아니다"는 맞지만, 그렇다고 poison pill 방어를 없앨 수는 없다. 카운터를 나눠 둘 다 만족시킨다(§2.3).
8. **Track A의 정합성 메커니즘을 대체하지 않는다.** 세 겹 멱등성, `embedding_version_no` fencing, UPSERT 수렴, 청킹 결정성은 그대로다. 인라인 재시도도 결국 같은 처리 함수를 다시 부르는 것이라 결정성 위에 서 있다.
9. **관측이 먼저, 튜닝은 나중.** 특히 `kafka_rebalance_total`과 `indexing_job_duration_seconds` p99 — 이번 전환이 안전한지는 이 두 숫자가 말해준다.
10. **감지할 수 없으면 대비한 게 아니다.**
