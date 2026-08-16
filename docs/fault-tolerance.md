# Track B 장애 대비 설계 — 수집 파이프라인 워커

> **담당**: 은지 (A — 수집 & 워커 확장성, `[임베딩 워커 - 그룹2]`)
> **작성일**: 2026-08-17
> **상태**: Track A 구현 완료 → Track B 착수 전 범위 확정용

---

## 0. 이 문서의 목적

Track A는 **"정합성이 깨지지 않는 파이프라인"**을 목표로 했고, 그 목표는 달성했다. Track B는 정합성을 *다시 만드는* 작업이 아니라, Track A가 의도적으로 미뤄둔 것과 **Track A를 실제로 구현하고 나서야 드러난 구멍**을 메우는 작업이다.

그래서 이 문서는 세 부분이다.

1. **§1 — Track A로 막아진 장애**: 이미 해결된 것. Track B에서 다시 건드리지 않는다.
2. **§2 — Track A가 남긴 구멍**: 스펙 단계에서 "Track B로 미룬다"고 명시한 것 + 구현하고 나서 발견한 것. Track B 항목의 근거가 여기 있다.
3. **§3~§5 — Track B 항목별 대응 방법**: 각 장애를 무엇으로 어떻게 막는지, 우선순위와 검증 방법까지.

---

## 1. Track A 구현으로 막아진 장애

### 1.1 요약표

| # | 장애 | 막은 장치 | 근거 |
|---|---|---|---|
| A-1 | 워커 프로세스 크래시(SIGKILL, OOM 킬) | Kafka offset 미커밋 → 자동 재전달 + `status IN ('PENDING','PROCESSING')` 재획득 | 스펙 §1.3 문제1, §1.4-(1) |
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
| A-15 | Storage 무응답으로 워커 스레드 영구 잠김 | 다운로드 타임아웃 30초(`max.poll.interval.ms`보다 짧게) | 스펙 §3.3 |
| A-16 | 비즈니스 예외 후 재시도 경로 없음(ack했으므로 Kafka가 안 줌) | `RETRY_WAIT` + `next_retry_at` + 워커 내부 재시도 폴러 | 스펙 §1.4-(4), §3.8 |
| A-17 | 여러 폴러 인스턴스의 중복 재시도 | 조건부 UPDATE(`WHERE status='RETRY_WAIT'`) — 분산 락 불필요 | 스펙 §3.8 |
| A-18 | 삭제된 문서의 유령 청크가 검색에 노출 | `DOCUMENT_DELETED` 처리 + `document_chunk` 삭제 | 스펙 §3.9 |
| A-19 | 삭제 이벤트 유실 시 청크 잔존 | 정리 스윕 스케줄러(chunk 존재 자체가 멱등 마커) | 스펙 §3.9 |
| A-20 | 삭제된 문서를 재시도 폴러가 되살림 | `failActiveJobsForDocument()` — 활성 Job을 먼저 `FAILED('DOCUMENT_DELETED')` | 스펙 §3.9, plan Task 19 |
| A-21 | 삭제 후 뒤늦게 끝난 Job이 검색 버전을 되살림 | 버전 전환 UPDATE에 `d.deleted_at IS NULL` 가드 | 스펙 §3.9, §6.1 |
| A-22 | 업로드↔삭제 순서 역전(재업로드가 삭제에 지워짐) | 같은 토픽·같은 파티션(`documentId` 키) | 스펙 §0.3 |

### 1.2 특히 강조할 만한 것 세 가지

**① 상호 배제를 포기하고 수렴을 택한 것.** 두 워커가 같은 Job을 동시에 만지는 것을 **허용**하고, 청킹 결정성 + UPSERT로 결과가 같은 값에 수렴하게 했다.

**② 두 복구 경로를 분리한 것.** Kafka는 컨슈머 생존 여부만 안다 — "비즈니스 로직이 실패했다"는 걸 절대 모른다. 그래서:

- 워커가 **죽은** 실패 → offset 미커밋 → Kafka 재전달이 곧 복구
- 워커가 **살아서 catch한** 실패 → 이미 ack했으므로 Kafka는 다시 안 줌 → 내부 폴러가 전담

이 둘을 하나로 착각하면 "재시도가 안 도는데 원인을 모르는" 상태가 된다.

---

## 2. Track A가 남긴 구멍

두 종류다. **(a) 스펙에서 의도적으로 미룬 것**과 **(b) 구현하고 나서 드러난 것**. (b)가 중요하다 — 스펙 §4의 Track B 목록에 없는 항목들이다.

### 2.1 의도적으로 미룬 것 (스펙 §4에 이미 명시)

| 구멍 | 현재 상태 | 왜 문제인가 |
|---|---|---|
| 에러 분류 없음 | 원인 불문 `attempt_count` 상한(5회)까지 재시도 | 0바이트 PDF에 5회 × 30·60·90·120초 = 5분 낭비. 반대로 임베딩 429는 5회로 부족할 수 있다 |
| 선형 백오프만 | `next_retry_at = now() + 30s × attempt_count` | 임베딩 서버가 잠깐 죽었다 살아나면 실패한 Job들이 **거의 동시에** 재시도를 날려 다시 죽인다(thundering herd) |
| DLQ 없음 | 역직렬화 실패·검증 실패는 **로그만 남기고 사라진다** | 원인을 고쳐도 되살릴 방법이 없다. 이벤트 원문이 어디에도 없다 |
| Outbox 보정 없음 | `outbox_event.status='PUBLISHED'`인데 Job이 없는 경우를 아무도 감지 못함 | 브로커 재시작(개발 환경 `replication.factor=1`)만으로도 발생. 유실이 조용히 일어난다 |
| 진행률 미노출 | `phase`, `total_chunks`, `processed_chunks` 컬럼 자체가 없음 | 사용자는 100페이지 PDF가 처리 중인지 멈춘 건지 구분 못 함 |
| 트레이싱 없음 | `trace_id`는 컬럼에만 있고 전파 안 됨 | API→트리거→Kafka→워커→DB 구간이 관측 공백 |
| 메트릭 없음 | 카운터/게이지 전무 | **감지할 수 없으면 대비한 게 아니다** |
| DB HA 대응 없음 | 페일오버 중 워커 동작 미정의 | 2차 과제 요구사항 |

### 2.2 구현하고 나서 드러난 것 ★

여기가 이 문서의 핵심이다. 스펙 §4에는 없지만 실제 코드 경로를 따라가보면 나오는 것들.

---

**B-Gap-1. 재시도 경로에서 크래시하면 Job이 `PROCESSING`에 영구히 갇힌다.** ★최우선

Track A는 "PROCESSING 좀비는 Kafka 재전달이 회수한다"고 전제했다. 이 전제는 **Kafka 리스너 경로에서만** 성립한다.

```
[리스너 경로]  Kafka 수신 → start() → PROCESSING → 크래시
               → offset 미커밋 → Kafka 재전달 → 재획득 ✅ 복구됨

[폴러 경로]    RETRY_WAIT → 폴러가 start() → PROCESSING → 크래시
               → 이 메시지는 이미 오래 전에 ack됨. Kafka는 다시 안 준다
               → 폴러는 `WHERE status = 'RETRY_WAIT'`만 조회한다
               → ❌ 이 Job은 영원히 PROCESSING. 아무도, 영원히 안 집는다
```

`IndexingRetryScheduler.retryDueJobs()`가 `findRetryWaitDue()`로 `RETRY_WAIT`만 찾는 이상, 폴러가 만든 `PROCESSING`은 회수 주체가 없다. 문서 하나가 조용히 인덱싱되지 않고 끝나며, **에러 로그조차 안 남는다**(가장 나쁜 실패 모드).

발생 조건이 좁아 보이지만(재시도 중 크래시), 재시도는 애초에 무언가 불안정할 때 도는 경로다 — 임베딩 서버가 흔들리는 구간에서 워커가 OOM으로 죽으면 바로 이 상황이다.

---

**B-Gap-2. `uq_indexing_job_active_version` 위반 시 이벤트가 조용히 사라진다.**

`IndexingPipelineRunner.acquireJobId()`는 이렇게 동작한다.

```kotlin
insertIfAbsent(...)                          // ON CONFLICT DO NOTHING
val job = findBySourceEventId(event.eventId)
if (job == null) {
    log.info("no job row for eventId=... — another active job already targets ...")
    return null                              // ← 그리고 리스너가 ack한다
}
```

같은 `document_version_id`를 가리키는 활성 Job이 이미 있으면 새 Job이 안 만들어지고, 이 이벤트는 **로그 한 줄 남기고 종결**된다. 앞선 Job이 성공하면 결과적으로 문제없지만, 앞선 Job이 `FAILED`로 끝나면 뒤늦게 온 이 이벤트는 이미 ack되어 사라진 뒤다. 재처리를 트리거할 것이 아무것도 없다.

같은 문제가 `start()` 실패 경로에도 있다 — `acquired != 1`이면 "이미 완료됐거나 상한 초과"로 보고 return하는데, 실제로는 `RETRY_WAIT`이면서 `next_retry_at`이 아직 미래인 경우도 여기 걸린다(정상 동작이지만, 그 사이 Kafka 메시지는 소비된다).

---

**B-Gap-3. 파일 전체를 메모리에 올린다 — OOM 경로가 세 겹.**

```kotlin
downloadClient.download(objectKey)            // ByteArray 전체
  → Loader.loadPDF(input.readAllBytes())      // 또 전체 복사
  → parser.parse(...).toList()                // Sequence의 스트리밍 이점을 여기서 폐기
  → chunkingService.chunk(blocks, ...)        // encode된 토큰 리스트 전체
```

스펙 §3.4는 "`List`로 전부 올리면 대용량 PDF에서 OOM"이라며 `Sequence`를 택했는데, `IndexingPipelineRunner`에서 `.toList()`로 즉시 realize한다. 결국 **원문 바이트 + 파서 내부 버퍼 + ParsedBlock 리스트 + 토큰 리스트 + Chunk 리스트**가 동시에 힙에 상주한다. 500MB PDF면 워커가 죽고, 죽으면 A-4의 `attempt_count` 캡이 5회 크래시 뒤에야 멈춘다.

게다가 **파일 크기 상한이 어디에도 없다.** 청크 수 상한(5000)은 청킹까지 도달한 뒤에야 판정된다 — OOM은 그 전에 난다.

---

**B-Gap-4. 파싱에 타임아웃이 없다.**

다운로드는 30초 타임아웃이 있지만(A-15), 파싱·청킹은 무제한이다. 악의적으로 구성된 PDF(zip bomb류, 순환 참조 오브젝트)나 hwplib의 무한 루프에 걸리면 워커 스레드가 `max.poll.interval.ms`(600초)를 넘기고 리밸런스가 발생한다. 그러면 다른 워커가 같은 메시지를 받아 **똑같이 걸린다** — 워커 fleet 전체가 한 파일에 잠식된다. `attempt_count` 캡은 이 상황을 못 막는다(크래시가 아니라 hang이라 `start()`가 다시 호출되지 않음).

---

**B-Gap-5. 시각의 출처가 섞여 있다.**

| 지점 | 시각 출처 |
|---|---|
| `IndexingFailureService.recordFailure()`의 `nextRetryAt` | 애플리케이션 `LocalDateTime.now()` |
| `findRetryWaitDue(now)` | 애플리케이션 `LocalDateTime.now()` |
| `start()`의 `next_retry_at <= CURRENT_TIMESTAMP` | **DB 시각** |
| `failActiveJobsForDocument()`의 `completed_at` | DB 시각 |

워커 컨테이너 시계가 DB보다 앞서면, 폴러는 due로 판단해 집었는데 `start()`가 조건 불일치로 0건을 반환한다 — 재시도가 조용히 no-op되고 로그에는 "already handled"로 찍힌다. 반대로 뒤서면 재시도가 예정보다 늦는다. 컨테이너 환경에서 수 초 오차는 흔하다.

추가로 `LocalDateTime`(타임존 없음)과 `TIMESTAMPTZ` 컬럼을 섞어 쓰고 있어, 워커 JVM 기본 타임존이 UTC가 아니면 오프셋만큼 어긋난다.

---

**B-Gap-6. 파티션 hot spot — `documentId` 키의 대가.**

파티션 키를 `documentId`로 잡은 건 조기 STALE 판정을 위해 옳은 결정이다(스펙 §3.1). 다만 그 대가로:

- 같은 문서를 반복 업데이트하는 사용자가 한 파티션을 독점한다
- `max-poll-records: 1` + `concurrency: 3`이므로, 한 파티션에 대용량 문서가 들어오면 그 파티션 뒤의 모든 문서가 대기한다
- 파티션 수는 워커 인스턴스의 3배(12개)로 고정 — 파티션 수를 늘리면 `documentId` 해시 매핑이 바뀌어 순서 보장이 일시적으로 깨진다

Track A는 이걸 감수했지만, 부하가 몰릴 때 "특정 테넌트 때문에 전체가 느려지는" 형태로 드러난다.

---

**B-Gap-7. 조기 STALE 판정 + 최신 버전 실패 = 두 버전 모두 검색 불가.**

```
v2(embedding_version_no=3) 업로드 → 처리 중
v3(embedding_version_no=5) 업로드
v3이 먼저 완료 → searchable = v3
v2 Job은 조기 STALE 판정 → COMPLETED(chunk_count=null), 청크 없음   ← 정상
```

여기까지는 설계대로다. 문제는 순서가 반대일 때:

```
v3이 먼저 처리 시작 → 파싱 실패 → 5회 재시도 후 FAILED
v2는 그 사이 조기 STALE 판정(v3보다 작으므로)... 은 아니다.
  → searchable은 아직 v1이므로 v2는 정상 처리된다 ✅
```

실제로 위험한 건 이 조합이다:

```
v2 처리 완료 → searchable = v2 (embedding_version_no=3)
v3 업로드(embedding_version_no=5) → 파싱 실패 → FAILED
→ 사용자는 v3을 올렸는데 검색은 여전히 v2 내용을 반환한다
→ Job 상태를 안 보면 이 불일치를 알 방법이 없다
```

Track A는 "검증 후에만 노출한다"(부록 원칙 7) 관점에서 **의도된 동작**이다. 하지만 사용자에게 이 상태를 알리는 채널이 Track B(진행률 API/알림)에 있으므로, Track B 전까지는 조용한 불일치로 남는다. Track B에서 반드시 노출해야 한다.

---

**B-Gap-8. `FAILED` 종결 후 자동 복구 경로가 없고, 아무도 모른다.**

`attempt_count >= maxAttempts` → `FAILED` → 끝. 복구는 사용자가 `POST .../indexing/retry`를 호출해야만 일어난다. 그런데 사용자가 그걸 호출할 이유를 알려면 실패했다는 걸 알아야 하고, 알림이 없다. 메트릭도 없다. **DLQ에 쌓이는데 아무도 모르는 게 최악**이라는 기존 문서의 지적이 그대로 유효하다.

---

**B-Gap-9. 삭제 스윕이 무한 재실행될 수 있다.**

`findDeletedDocumentsWithRemainingChunks()`는 "삭제됐는데 chunk가 남은 문서"를 찾고, `handleDocumentDeleted()`가 chunk를 지운다. 정상이면 다음 스윕에서 안 잡힌다. 그런데 chunk 삭제가 계속 실패하면(FK 제약, 락 경합, 파티션 문제) **매 60초마다 같은 문서를 영원히 재시도**한다. 실패 횟수를 세지 않으므로 종료 조건이 없고, 로그만 `WARN`으로 쌓인다. 배치 크기 50을 이런 문서들이 다 차지하면 정상 삭제 건이 밀린다.

---

**B-Gap-10. 컨슈머가 DB 상태를 신경 쓰지 않는다.**

DB가 내려간 상태에서도 리스너는 계속 메시지를 소비하고, 매번 예외를 던지고, **매번 ack한다.** 그 사이 들어온 메시지는 전부 소실된다 — `insertIfAbsent`조차 실패했으므로 `indexing_job` 행도 없고, `recordFailure`도 실패하므로 실패 기록도 없다. Kafka offset만 앞으로 나간다.

이건 A-5(always-ack 원칙)의 그림자다. always-ack은 hot loop를 막지만, **DB가 없을 때는 "봤다"고 표시하면 안 되는 것까지 표시해버린다.**

---

## 3. Track B — 장애별 대응 방법

우선순위는 **"막지 않으면 데이터가 조용히 사라지는가"**를 첫 기준, **"장애를 감지할 수 있는가"**를 두 번째 기준으로 잡았다.

---

### P0-1. `PROCESSING` 고아 회수 (B-Gap-1)

**막는 장애**: 재시도 경로 크래시로 영구 유실, DB 페일오버 중 롤백된 Job

**방법 — 스윕으로 해결한다.**

같은 문제를 훨씬 싸게 푸는 방법이 이미 코드베이스에 있다 — **삭제 스윕과 동일한 패턴**이다.

```sql
-- 오래 PROCESSING에 머문 Job을 RETRY_WAIT으로 되돌린다(재획득이 아니라 "되돌리기").
UPDATE indexing_job
SET status        = 'RETRY_WAIT',
    next_retry_at = CURRENT_TIMESTAMP,
    last_error_code = 'PROCESSING_STALLED',
    last_error_message = 'Job stayed in PROCESSING beyond threshold; recovered by sweep',
    updated_at    = CURRENT_TIMESTAMP
WHERE status = 'PROCESSING'
  AND updated_at < CURRENT_TIMESTAMP - :stalledThreshold   -- 기본 15분
  AND attempt_count < :maxAttempts
RETURNING id;
```

- **`lease_expires_at` 컬럼을 새로 만들지 않는다.** `updated_at`이 이미 "마지막으로 이 Job을 만진 시각"이다. `start()`가 `updated_at`을 갱신하므로 그대로 쓸 수 있다.
- **heartbeat도 만들지 않는다.** 임계값을 `max.poll.interval.ms`(600초)보다 넉넉히(15분) 잡으면, 정상 처리 중인 장시간 Job을 잘못 회수할 위험이 실질적으로 없다. 설령 잘못 회수해도 A-3(수렴 설계)이 이미 두 워커 동시 실행을 안전하게 만들어놨다 — **이게 Track A의 수렴 설계가 준 배당금이다.** lease 방식이었다면 여기서 소유권 재확인 로직이 필요했다.
- `attempt_count < :maxAttempts` 조건으로 무한 회수 루프를 막는다. 상한 초과분은 별도 UPDATE로 `FAILED('MAX_ATTEMPTS_EXCEEDED')` 종결.
- 회수 후 `RETRY_WAIT`으로 두면 **기존 재시도 폴러(§3.8)가 그대로 이어받는다.** 새 실행 경로를 만들 필요가 없다.

**주기**: 재시도 폴러와 같은 스케줄러에 얹거나(권장, 인스턴스 증가 없음), 별도 `@Scheduled(fixedDelay = 60_000)`.

**메트릭**: `indexing_job_stalled_recovered_total` — 이 값이 0이 아니면 어딘가에서 워커가 죽고 있다는 뜻이므로 반드시 알림.

---

### P0-2. 에러 분류 (Permanent / Transient)

**막는 장애**: 영구 실패에 재시도 낭비, 반대로 일시 실패에 재시도 부족

Track A는 원인을 가리지 않고 5회 재시도한다. 분류를 넣으면 **`FAILED` 종결 로직 자체는 그대로**고, 그 앞에 "재시도할지 판단하는 단계"만 추가된다.

```kotlin
sealed class IndexingException(
    val code: String,
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause) {
    abstract val permanent: Boolean
}

class PermanentIndexingException(code: String, msg: String, cause: Throwable? = null)
    : IndexingException(code, msg, cause) { override val permanent = true }

class TransientIndexingException(code: String, msg: String, cause: Throwable? = null)
    : IndexingException(code, msg, cause) { override val permanent = false }
```

`IndexingFailureService.recordFailure()`의 분기만 바꾼다.

```kotlin
val nextStatus = when {
    error is IndexingException && error.permanent -> FAILED   // attempt_count 무관 즉시 종결
    job.attemptCount >= maxAttempts               -> FAILED
    else                                          -> RETRY_WAIT
}
```

**분류표** (Track A에서 이미 던지고 있는 예외를 매핑)

| 분류 | 코드 | Track A의 발생 지점 |
|---|---|---|
| Permanent | `EMPTY_EXTRACTION` | `ChunkGuard` — `EmptyExtractionException` |
| Permanent | `CHUNK_LIMIT_EXCEEDED` | `ChunkGuard` — `ChunkLimitExceededException` |
| Permanent | `HASH_MISMATCH` | `IndexingPipelineRunner` — `ContentIntegrityException` |
| Permanent | `UNSUPPORTED_MIME` | `DocumentParserRegistry` — `UnsupportedMimeTypeException` |
| Permanent | `CORRUPTED_FILE` | 파서가 열지 못함(PDFBox/POI/hwplib 예외) |
| Permanent | `FILE_TOO_LARGE` | P1-6에서 신설 |
| Permanent | `DOCUMENT_DELETED` | `failActiveJobsForDocument()` |
| Transient | `STORAGE_TIMEOUT` | S3 다운로드 타임아웃 |
| Transient | `STORAGE_SERVER_ERROR` | S3 5xx |
| Transient | `EMBEDDING_RATE_LIMIT` | 임베딩 429 |
| Transient | `EMBEDDING_SERVER_ERROR` | 임베딩 5xx |
| Transient | `PARSE_TIMEOUT` | P0-5에서 신설 |
| Transient | `DB_CONNECTION_LOST` | 페일오버 |
| Transient | `PROCESSING_STALLED` | P0-1 스윕 회수 |

**인프라 실패는 `attempt_count`를 올리지 않는다.** DB 페일오버로 재시도 한도가 소진되면 안 된다 — P2-11 참고.

**태성님께 요청**: `IndexingProcessor` 내부에서 임베딩 API 실패를 던질 때 위 `TransientIndexingException`/`PermanentIndexingException`으로 감싸주면 된다. 분류 인터페이스는 A가 정의하고 제공한다.

---

### P0-3. 지수 백오프 + 지터

**막는 장애**: thundering herd — 임베딩 서버가 복구되는 순간 재시도가 동시에 몰려 다시 죽임

Track A의 `next_retry_at = now() + 30s × attempt_count`는 **모든 Job이 거의 같은 시각에 재시도된다.** 실패 시각이 비슷하면(서버 다운) 재시도 시각도 비슷해진다.

```kotlin
fun nextRetryAt(attempt: Int, now: Instant): Instant {
    val base = 30L
    val exp = minOf(base shl (attempt - 1), 600L)          // 30 → 60 → 120 → 240 → 480, 상한 600초
    val jittered = (exp * ThreadLocalRandom.current().nextDouble(0.5, 1.0)).toLong()
    return now.plusSeconds(jittered)
}
```

- 지터 범위 0.5~1.0(full jitter가 아닌 equal jitter) — 최소 대기는 보장하면서 분산시킨다.
- `Retry-After` 헤더가 있으면(429) 그 값을 우선한다.
- 상한 10분: 그 이상은 사용자가 기다릴 만한 시간이 아니고, 어차피 5회 상한에 도달한다.

---

### P0-4. 컨슈머 게이트 (B-Gap-10)

**막는 장애**: DB 다운 중 메시지 소각, 페일오버 구간 유실

DB가 없으면 **메시지를 소비하지 않는 게 맞다.** Kafka에 그대로 대기시키면 복구 후 자연히 처리된다.

```kotlin
@Component
class DbHealthGate(
    private val registry: KafkaListenerEndpointRegistry,
    private val jdbcTemplate: JdbcTemplate,
) {
    @Scheduled(fixedDelay = 5_000)
    fun check() {
        val healthy = runCatching {
            jdbcTemplate.queryForObject("SELECT 1", Int::class.java)
        }.isSuccess
        val container = registry.getListenerContainer("indexing") ?: return
        when {
            !healthy && container.isRunning ->
                { container.pause(); log.warn("DB down — consumer paused") }
            healthy && container.isPauseRequested ->
                { container.resume(); log.info("DB up — consumer resumed") }
        }
    }
}
```

**주의점 둘.**

- `pause()`는 폴링을 멈추지 않는다(heartbeat는 계속 감) — 리밸런스가 나지 않으므로 안전하다. `stop()`을 쓰면 리밸런스가 터진다.
- **재시도 폴러와 삭제 스윕도 같이 멈춰야 한다.** 안 그러면 DB가 없는데 폴러가 계속 예외를 던지며 로그를 채운다. 게이트 상태를 공유하는 플래그 하나로 처리한다.

또한 always-ack 원칙에 **예외 하나**를 추가한다:

> **DB에 아무것도 기록하지 못한 실패는 ack하지 않는다.** 구체적으로 `insertIfAbsent`/`start`/`recordFailure`가 전부 실패한 경우(= `DataAccessResourceFailureException` 계열)는 ack를 건너뛰고 컨테이너를 pause시킨다. 이때 hot loop 위험은 pause가 막는다.

이 예외를 명문화하지 않으면 A-5와 충돌한 것처럼 보인다 — 원칙은 "실패를 **기록했다면** 반드시 ack"이고, 기록조차 못 했으면 ack하지 않는 게 일관된다.

---

### P0-5. 리소스 가드 — 파일 크기 상한 + 파싱 타임아웃 + 스트리밍 (B-Gap-3, B-Gap-4)

**막는 장애**: OOM으로 인한 워커 fleet 붕괴, 악성 파일에 의한 파티션 잠식

**(a) 파일 크기 상한 — 다운로드 *전에* 판정한다.**

```kotlin
// document_version.file_size는 이미 DB에 있다 — 다운로드 없이 판정 가능
if (documentVersion.fileSize > properties.maxFileSizeBytes) {
    throw PermanentIndexingException(
        "FILE_TOO_LARGE",
        "file size ${documentVersion.fileSize} exceeds limit ${properties.maxFileSizeBytes}",
    )
}
```

```yaml
indexing:
  limits:
    max-file-size-bytes: 209715200      # 200MB
    parse-timeout: PT120S
    max-heap-per-job-mb: 512
```

**(b) 파싱 타임아웃 — 별도 실행자에서 돌리고 시간을 건다.**

```kotlin
private val parseExecutor = Executors.newFixedThreadPool(concurrency)

fun parseWithTimeout(parser: DocumentParser, bytes: ByteArray): List<ParsedBlock> {
    val future = parseExecutor.submit<List<ParsedBlock>> { parser.parse(bytes.inputStream()).toList() }
    return try {
        future.get(properties.parseTimeout.seconds, TimeUnit.SECONDS)
    } catch (e: TimeoutException) {
        future.cancel(true)
        throw TransientIndexingException("PARSE_TIMEOUT", "parsing exceeded ${properties.parseTimeout}")
    }
}
```

`future.cancel(true)`가 네이티브 라이브러리 안에서 도는 루프를 실제로 멈추지는 못한다는 걸 인정해야 한다 — 그래서 이 스레드가 누수될 수 있다. 완전한 격리가 필요하면 별도 파싱 프로세스로 분리해야 하지만, 그건 과하다. **현실적 목표는 "메시지 처리를 진행시키는 것"**이고, 누수된 스레드는 워커 재시작으로 회수한다. 누수 스레드 수를 메트릭으로 노출해 임계 초과 시 워커를 재시작하는 것까지가 Track B 범위.

**(c) 스트리밍 복원.**

- `parser.parse(...)`의 `Sequence`를 청커까지 그대로 흘린다(`.toList()` 제거).
- 청커가 블록을 소비하면서 청크를 만들고, 일정 개수마다 배치로 넘긴다.
- PDFBox는 `Loader.loadPDF(File)` 오버로드로 임시 파일 기반 로딩을 쓴다 — 바이트 배열 전체 복사를 피한다.
- 다운로드도 `ByteArray` 대신 임시 파일로 스풀하고, 해시는 스트리밍으로 계산한다(`DigestInputStream`).

```kotlin
val temp = Files.createTempFile("indexing-", ".bin")
DigestInputStream(s3Stream, MessageDigest.getInstance("SHA-256")).use { dis ->
    Files.copy(dis, temp, REPLACE_EXISTING)
    val actual = "sha256:" + dis.messageDigest.digest().toHex()
    if (actual != documentVersion.contentHash) throw ...
}
```

**주의**: 임시 파일은 반드시 `finally`에서 지운다. 워커가 죽으면 남으므로, 컨테이너 기동 시 임시 디렉터리를 비우는 것도 함께.

---

### P1-6. DLQ + 재처리 API (B-Gap-8, 그리고 §2.1)

**막는 장애**: 역직렬화/검증 실패 이벤트의 완전 소실, `FAILED` 방치

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

**DLQ로 보낼 대상** (Track A에서 로그만 남기던 것들):

| 경로 | Track A 현재 | Track B |
|---|---|---|
| `DeserializationException` | 로그 + ack | DLQ 적재(원문 payload 보존) + ack |
| `InvalidEventException` (스키마/tenant/없는 버전) | 로그 + ack | DLQ 적재 + ack |
| `UNKNOWN_EVENT_TYPE` | 로그 + ack | DLQ 적재 + ack |
| `FAILED` 종결된 Job | DB에만 남음 | DLQ 적재(재처리 UI 단일화) |
| `acquireJobId()`가 null (B-Gap-2) | 로그 + ack | DLQ 적재 — "선행 Job에 흡수됨"으로 기록 |

**재처리 경로가 반드시 있어야 한다.** 원인을 고쳐도 되살릴 방법이 없으면 DLQ는 쓰레기통이다.

```
POST /admin/indexing/dead-letters/{id}/retry
  1. original_payload를 다시 파싱
  2. indexing_job: 기존 행이 있으면 status=PENDING, attempt_count=0, next_retry_at=now()
                   없으면 새로 insert
  3. dead_letter.resolved_at 기록
  4. (Kafka 재발행 없이) 재시도 폴러가 집어가게 둔다 — §3.8 경로 재사용
```

Kafka로 되돌리지 않는 게 핵심이다. `indexing_job`에 재처리에 필요한 정보가 다 있고, 새 이벤트를 발행하면 `uk_indexing_job_source_event`와 충돌 소지만 늘어난다(스펙 §3.8의 논리를 그대로 따른다).

**알림**: `indexing_dlq_total` 증가 시 즉시 알림. DLQ에 쌓이는데 아무도 모르는 게 최악이다.

---

### P1-7. Outbox 보정 배치 + 서킷 브레이커

**막는 장애**: Kafka 브로커 장애로 "`PUBLISHED`인데 아무도 처리 안 함"

Kafka가 죽어도 `outbox_event`가 `PENDING`으로 남으므로 유실은 없다. 문제는 **`PUBLISHED`로 찍혔는데 실제로는 컨슈머까지 도달하지 않은** 경우다. `replication.factor=1`인 개발 환경에서는 브로커 재시작만으로 발생한다.

```sql
-- 발행된 지 5분 지났는데 Job이 없다 = 유실 의심
SELECT e.*
FROM outbox_event e
LEFT JOIN indexing_job j ON j.source_event_id = e.id
WHERE e.status = 'PUBLISHED'
  AND j.id IS NULL
  AND e.published_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
ORDER BY e.published_at
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

```sql
CREATE INDEX idx_outbox_published
    ON outbox_event (published_at) WHERE status = 'PUBLISHED';
```

찾은 행은 `status = 'PENDING'`으로 리셋해 `doc-relay`가 다시 집게 한다. 중복 발행돼도 `source_event_id UNIQUE`(A-2)가 막아주므로 안전하다.

**경계 주의**: `outbox_event`는 `doc-relay` 소유다(스펙 §0.2). 이 배치는 유일한 예외이므로 **`doc-relay` 담당과 반드시 사전 합의**한다. 합의 포인트:

- 리셋 조건(5분)이 `doc-relay`의 자체 재시도 주기와 겹치지 않는지
- `retry_count`를 건드릴지 여부(건드리지 않는 쪽 권장 — `doc-relay`의 `DEAD` 판정 로직을 오염시키지 않기 위해)
- 이 배치를 워커가 아니라 `doc-relay` 쪽에 두는 게 나은지(경계상 그쪽이 더 깨끗하다 — **합의 시 이쪽을 먼저 제안한다**)

**서킷 브레이커**는 `doc-relay` 범위지만, 워커 쪽에도 하나 필요하다 — 임베딩 API가 지속 실패할 때 재시도 폴러가 계속 때리지 않도록. 연속 실패 N회 시 폴러를 일정 시간 open 시킨다.

---

### P1-8. 진행률 API / SSE

**막는 장애**: "멈춘 건지 처리 중인지 모름", B-Gap-7의 조용한 불일치

**스키마 추가 필요**:

```sql
ALTER TABLE indexing_job ADD COLUMN phase            VARCHAR(30);   -- DOWNLOADING/PARSING/CHUNKING/EMBEDDING/PUBLISHING
ALTER TABLE indexing_job ADD COLUMN total_chunks     INTEGER;
ALTER TABLE indexing_job ADD COLUMN processed_chunks INTEGER;
```

`IndexingPipelineRunner`의 각 단계 진입 시 `phase`를 갱신한다. **부수 효과가 하나 더 있다** — `phase`가 있으면 P0-1의 stalled 스윕이 "어느 단계에서 멈췄는지"까지 알려주므로 원인 분석이 쉬워진다. 그리고 `updated_at`이 단계마다 갱신되므로 stalled 임계값을 더 짧게(예: 5분) 잡을 수 있다.

**노출**:

- `GET /documents/{id}/versions/{vid}/indexing` — 폴링
- `GET /documents/{id}/versions/{vid}/indexing/events` — SSE

SSE 백엔드로 `LISTEN/NOTIFY`를 쓴다면 **끊긴 걸 조용히 모르는 게 최악이다**(SSE가 아무것도 안 보내는데 에러도 안 남). 연결 유효성을 주기적으로 확인하고, 재연결 직후 폴백 폴링으로 1회 보정한다.

**B-Gap-7 대응**: 버전 목록 응답에 `searchable: boolean`과 함께 `indexingStatus`를 노출해, "최신 버전을 올렸지만 실패해서 이전 버전이 검색된다"는 상태를 사용자가 볼 수 있게 한다.

---

### P1-9. 메트릭 + 알림

**감지할 수 없으면 대비한 게 아니다.**

| 메트릭 | 타입 | 알림 | 무엇을 알려주나 |
|---|---|---|---|
| `indexing_dlq_total` | Counter | **즉시** | 되살려야 할 게 쌓이고 있다 |
| `indexing_job_stalled_recovered_total` | Counter | **즉시** | 워커가 죽고 있다(B-Gap-1이 실제로 발생) |
| `outbox_reconciled_total` | Counter | **즉시** | Kafka 경로에서 유실이 나고 있다 |
| `indexing_job_failed_total{errorCode}` | Counter | 급증 시 | 어떤 종류가 늘고 있나 |
| `indexing_retry_total{errorCode}` | Counter | — | 재시도 낭비 지점 |
| `indexing_job_duration_seconds{phase}` | Histogram | — | 어느 단계가 병목인가 |
| `indexing_job_active` | Gauge | — | 동시 처리량 |
| `kafka_consumer_lag{partition}` | Gauge | 지속 증가 | 파티션 hot spot(B-Gap-6) 조기 발견 |
| `db_health_gate_paused_total` | Counter | **즉시** | DB가 흔들리고 있다 |
| `parse_thread_leaked` | Gauge | 임계 초과 | P0-5의 취소 실패 누적 |
| `suspicious_extraction_total` | Counter | 급증 시 | 100KB 파일에서 50자 추출 등 |

**대시보드 최상단에는 "업로드 → 검색 가능"의 end-to-end 시간을 크게 띄운다.** 심사위원이 한눈에 보는 숫자이자, 우리가 매일 봐야 하는 숫자다.

---

### P2-10. 비동기 경계 트레이싱

**막는 장애**: 관측 공백 — API에서 DB를 건너 Kafka를 건너 워커로 가는 구간이 끊겨 있다

- API 서버: 매 트랜잭션 시작 시 `SET LOCAL app.trace_id = :traceId` (스펙 §6.3에서 이미 요청)
- 트리거: `current_setting('app.trace_id', true)`로 읽어 `outbox_event.trace_id`에 기록
- `doc-relay`: Kafka payload의 `traceId` 필드에 전파(스키마에 이미 있음)
- 워커: 수신 시 MDC에 세팅 → `indexing_job.trace_id` 저장 → 모든 로그에 자동 포함
- 재시도 폴러: `RetryEventSource.toEvent()`가 `job.traceId`를 그대로 실어 보내므로(Track A에 이미 구현됨) 재시도 구간도 이어진다

OpenTelemetry까지 갈 필요는 없다. `trace_id` 하나가 로그 전 구간에 찍히는 것만으로도 대부분의 조사가 끝난다.

---

### P2-11. DB 페일오버 대응 (2차 과제)

클러스터 구성은 인프라 몫이지만, **페일오버 순간 처리 중이던 작업을 살리는 건 전적으로 워커 설계 문제다.** 다른 팀은 대부분 "클러스터 띄웠습니다"에서 끝난다.

**(a) 커넥션 레이어**

```
DB_URL=jdbc:postgresql://node1:5432,node2:5432/osscontest?targetServerType=primary&loadBalanceHosts=false
```

```yaml
spring:
  datasource:
    hikari:
      connection-timeout: 5000
      validation-timeout: 3000
      keepalive-time: 30000       # 죽은 커넥션 조기 감지
      max-lifetime: 600000
```

`targetServerType=primary`가 핵심 — 재연결 시 드라이버가 각 호스트의 `pg_is_in_recovery()`를 확인해 Primary를 고른다. `keepalive-time`이 없으면 끊긴 커넥션을 계속 붙잡고 있다가 실제 쿼리 시점에야 실패한다.

**(b) 인프라 실패는 `attempt_count`를 소모하지 않는다**

```kotlin
fun isInfrastructureFailure(e: Throwable): Boolean =
    e is SQLTransientConnectionException ||
    (e is PSQLException && e.sqlState in setOf("57P01", "57P02", "08006", "08001"))
```

이 경우 Job 상태를 아예 건드리지 않고 예외만 던진다. 어차피 P0-1의 stalled 스윕이 회수한다. **여기서도 P0-1이 재사용된다** — lease를 안 만들고 스윕으로 푼 결정이 2차에서 다시 값을 한다.

**(c) 컨슈머 게이트** — P0-4가 그대로 페일오버 대응이다. 별도 작업이 아니다.

**(d) 측정 — 이게 발표 자료가 된다**

| 지표 | 측정 방법 |
|---|---|
| 데이터 유실 건수 | 페일오버 중 업로드한 N건 중 최종 완료 수 → **N/N이어야 함** |
| 복구 소요 시간(RTO) | Primary kill 시각 → 첫 정상 완료 시각 |
| 좀비 회수 건수 | `indexing_job_stalled_recovered_total` 증가분 |
| 보정 건수 | `outbox_reconciled_total` 증가분 |
| 컨슈머 복구 시간 | pause → resume 간격 |

> 발표 문장 예시: *"Primary 노드를 강제 종료한 뒤에도 진행 중이던 12건이 유실 없이 복구됐습니다. 평균 RTO는 47초, 그중 대부분은 stalled 임계값 대기 시간입니다."*

---

### P2-12. 시각 통일 (B-Gap-5)

**방법**: **모든 시각 판단을 DB로 단일화한다.**

- `next_retry_at` 계산: 애플리케이션에서 `LocalDateTime.now()`로 만들지 말고, UPDATE 문 안에서 `CURRENT_TIMESTAMP + :interval`로 계산
- `findRetryWaitDue`: 파라미터 `:now`를 없애고 쿼리 안에서 `next_retry_at <= CURRENT_TIMESTAMP`
- 컬럼 타입에 맞춰 `LocalDateTime` → `Instant`/`OffsetDateTime`으로 통일(`TIMESTAMPTZ`와 정합)
- 워커 JVM `TZ=UTC` 고정

지루하지만, 이걸 안 하면 재시도가 "가끔 안 도는" 재현 안 되는 버그로 나타난다.

---

### P2-13. 파티션 / 처리량 튜닝 (B-Gap-6)

| 문제 | 대응 |
|---|---|
| 대용량 문서가 파티션을 점유 | 파일 크기별 우선순위 큐 분리(별도 토픽 `indexing.bulk`) 또는 `max-poll-records` 유지 + 파티션 증설 |
| 파티션 증설 시 순서 보장 일시 붕괴 | 증설 전 컨슈머 정지 → 백로그 소진 → 증설 → 재개. 무중단이 필요하면 커스텀 파티셔너로 기존 매핑 유지 |
| 특정 테넌트 독점 | 테넌트별 동시 처리 상한(세마포어), 초과분은 `RETRY_WAIT`으로 되돌리기 |
| 재시도 백로그 처리량 부족 | `batch-size`/`poll-interval` 튜닝 + `indexing_retry_backlog` 게이지로 관측 |

이 항목은 실측(P1-9의 `kafka_consumer_lag`) 없이 미리 튜닝하지 않는다. **먼저 관측하고, 문제가 보이면 손댄다.**

---

### P2-14. 삭제 스윕 무한 재시도 방어 (B-Gap-9)

```sql
-- 스윕 대상에서 "계속 실패하는 문서"를 제외한다
ALTER TABLE document ADD COLUMN chunk_purge_failed_count INTEGER NOT NULL DEFAULT 0;
```

- `handleDocumentDeleted()` 실패 시 카운터 증가
- 스윕 쿼리에 `AND d.chunk_purge_failed_count < :maxPurgeAttempts` 추가
- 상한 초과분은 DLQ에 적재 + 알림 (수동 개입 필요)

`document`에 컬럼을 추가하는 게 부담스러우면 별도 `deletion_failure` 테이블도 가능하지만, 컬럼 하나가 더 간단하다. API 서버 담당과 조율 필요.

---

### P2-15. `acquireJobId` null 경로 보정 (B-Gap-2)

두 단계로 처리한다.

1. **즉시**: DLQ 적재(P1-6) — 최소한 흔적은 남긴다.
2. **후속 보정**: Job이 최종 상태(`COMPLETED`/`FAILED`)에 도달했을 때, 같은 `document_version_id`를 가리키던 흡수된 이벤트가 DLQ에 있으면 재평가한다. 선행 Job이 `COMPLETED`면 `resolved_at`을 찍고 종결, `FAILED`면 재처리 대상으로 남긴다.

또는 더 단순하게: **P1-7의 Outbox 보정 배치가 이 경우도 커버한다.** `outbox_event`는 `PUBLISHED`인데 `indexing_job`이 없으므로 보정 쿼리에 걸린다. 이 경우 보정 배치만으로 충분할 수 있으니, **P1-7 구현 후 B-Gap-2가 실제로 남는지 먼저 검증하고 판단한다.**

---

## 4. 구현 순서

의존성 순. 앞이 없으면 뒤를 만들거나 검증할 수 없다.

| 순서 | 항목 | 선행 | 이유 |
|---|---|---|---|
| 1 | **P0-2 에러 분류** | — | 나머지 전부가 "이 실패가 재시도 가능한가"를 물어본다. 나중에 넣으면 모든 지점을 다시 손봐야 한다 |
| 2 | **P0-1 PROCESSING 고아 회수** | — | 유일하게 "조용한 영구 유실"을 만드는 구멍. 다른 항목보다 손해가 크다 |
| 3 | **P0-3 지수 백오프 + 지터** | 1 | 에러 분류가 있어야 Transient에만 적용할 의미가 생긴다 |
| 4 | **P0-5 리소스 가드** | 1 | `FILE_TOO_LARGE`/`PARSE_TIMEOUT`이 에러 분류 체계를 쓴다 |
| 5 | **P0-4 컨슈머 게이트** | — | 독립적. DB 장애 테스트의 전제 |
| 6 | **P1-6 DLQ + 재처리 API** | 1, `dead_letter` 테이블 | 에러 코드 체계가 있어야 DLQ가 유용해진다 |
| 7 | **P1-9 메트릭** | 1~6 | 앞 항목들이 실제로 도는지 확인할 유일한 수단 |
| 8 | **P1-7 Outbox 보정** | `idx_outbox_published`, doc-relay 합의 | 경계를 넘으므로 합의가 선행 |
| 9 | **P1-8 진행률 API/SSE** | 스키마 3컬럼 추가 | API 담당과 병행 |
| 10 | **P2-10 트레이싱** | API 서버의 `SET LOCAL` | 외부 의존 |
| 11 | **P2-12 시각 통일** | — | 언제 해도 되지만, 미루면 재현 안 되는 버그로 돌아온다 |
| 12 | **P2-14 삭제 스윕 방어** | 6 | DLQ가 있어야 종착지가 생긴다 |
| 13 | **P2-15 흡수 이벤트 보정** | 8 | 8이 커버하는지 먼저 검증 |
| 14 | **P2-11 DB 페일오버** | 2, 5, 클러스터 구성 | 2·5의 재활용 + 인프라 선행 |
| 15 | **P2-13 파티션 튜닝** | 7 | 실측 없이 미리 튜닝하지 않는다 |

**1번(에러 분류)과 2번(고아 회수)만은 순서를 바꾸지 않는다.** 1은 나머지 전부의 기반이고, 2는 유일하게 데이터가 조용히 사라지는 구멍이다.

---

## 5. 테스트 시나리오

Track A의 §5는 정합성 검증이었다. Track B는 **복구가 실제로 도는지**를 검증한다.

### 5.1 PROCESSING 고아 회수 (P0-1)

```bash
# 1) 재시도 상태를 만든다: 임베딩을 일시적으로 실패시켜 RETRY_WAIT 전이
# 2) 폴러가 집어 PROCESSING이 된 직후 워커를 죽인다
docker kill -s SIGKILL worker-1
```

```sql
-- 기대: 임계값(15분) 경과 후 RETRY_WAIT으로 되돌아오고, 폴러가 다시 집어 COMPLETED
SELECT status, attempt_count, last_error_code, updated_at FROM indexing_job WHERE id = :jobId;
-- PROCESSING_STALLED가 last_error_code에 찍혀야 한다
```

**이 테스트가 통과하지 않으면 Track B의 나머지는 의미가 없다.** 데이터가 사라지는 유일한 경로다.

### 5.2 에러 분류 (P0-2)

| 입력 | 기대 |
|---|---|
| 0바이트 PDF | `attempt_count = 1`에서 즉시 `FAILED('CORRUPTED_FILE')` — 재시도 없음 |
| 스캔 PDF | `attempt_count = 1`에서 즉시 `FAILED('EMPTY_EXTRACTION')`, **검색 버전 전환 안 됨** |
| 임베딩 429(WireMock, 3회 후 성공) | `RETRY_WAIT` 3회 → `COMPLETED` |
| 300MB 파일 | 다운로드 **전에** `FAILED('FILE_TOO_LARGE')` — S3 트래픽 0 |

### 5.3 Thundering herd (P0-3)

```bash
# 임베딩 서버를 죽인 상태에서 문서 50건 투입 → 전부 RETRY_WAIT
# 서버 복구 후 재시도 도착 시각 분포 확인
```

```sql
SELECT date_trunc('second', next_retry_at) AS sec, count(*)
FROM indexing_job WHERE status = 'RETRY_WAIT'
GROUP BY 1 ORDER BY 1;
-- 기대: 특정 초에 몰리지 않고 30~60초 구간에 분산
```

### 5.4 컨슈머 게이트 (P0-4)

```bash
docker stop postgres
# 이 상태에서 문서 3건 업로드
sleep 30
docker start postgres
```

**기대**: 컨슈머 pause 로그 → DB 복구 감지 → resume → 3건 전부 처리. **Kafka lag이 3에서 0으로 줄어든다**(메시지가 소각되지 않고 대기했다는 증거).

```sql
-- 커넥션만 끊기 (프로세스는 살아있음)
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE application_name = 'worker' AND pid <> pg_backend_pid();
```

### 5.5 리소스 가드 (P0-5)

- 1만 페이지 PDF → 힙 사용량이 상한 내에 머무는지 JFR/힙덤프로 확인
- 순환 참조 PDF(악성 픽스처) → `PARSE_TIMEOUT` 후 다음 메시지가 정상 처리되는지
- 워커 3대에 동시에 대용량 파일 투입 → OOM 없이 완주

### 5.6 Kafka 장애 + 보정 (P1-7)

```bash
docker stop kafka
# 문서 3건 업로드 → outbox에 PENDING 대기
docker start kafka
```
**기대**: 자동 발행 → 전부 완료. 유실 0.

```bash
# PUBLISHED인데 Job이 없는 상황을 인위적으로 만든다
```
```sql
UPDATE outbox_event SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP - INTERVAL '10 minutes'
WHERE id = :eventId;
DELETE FROM indexing_job WHERE source_event_id = :eventId;
```
**기대**: 보정 배치가 `PENDING`으로 리셋 → `doc-relay`가 재발행 → 처리 완료. `outbox_reconciled_total` 증가.

### 5.7 DLQ 왕복 (P1-6)

```bash
# 깨진 JSON을 토픽에 직접 넣는다
docker exec kafka kafka-console-producer --topic indexing --bootstrap-server localhost:9092
> not-json
```
**기대**: `dead_letter`에 `original_payload = 'not-json'`으로 적재 + ack(파티션 안 막힘). 이후 재처리 API 호출 → `resolved_at` 기록.

### 5.8 네트워크 장애 (Toxiproxy)

```bash
# DB 앞 지연 주입
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -d '{"type":"latency","attributes":{"latency":3000,"jitter":1000}}'

# Storage 대역폭 제한 — 다운로드 타임아웃 재현
curl -X POST http://localhost:8474/proxies/storage/toxics \
  -d '{"type":"bandwidth","attributes":{"rate":10}}'
```

**기대**: 타임아웃이 실제로 동작하고, 워커 스레드가 영구히 잠기지 않는다. `STORAGE_TIMEOUT`으로 분류되어 재시도된다.

### 5.9 카오스 테스트 (최종 성적표)

k6로 지속 부하를 주면서 30분간 무작위로 워커를 죽인다.

```bash
while true; do
  sleep $((RANDOM % 60 + 30))
  docker kill -s SIGKILL "worker-$((RANDOM % 3 + 1))"
  sleep 10
  docker compose up -d
done
```

종료 후 두 쿼리가 성적표다.

```sql
-- ① 모든 Job이 최종 상태에 도달했는가
SELECT status, count(*) FROM indexing_job GROUP BY status;
-- PROCESSING/PENDING이 남아 있으면 = P0-1 실패
-- RETRY_WAIT이 next_retry_at을 한참 지나고도 남아 있으면 = 폴러 실패

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

**30분 카오스 후에도 둘 다 깨끗하면 그대로 발표에 쓴다.**

---

## 6. 데모 시나리오 4종

각 1~2분. 라이브로 보여줄 것.

**① 워커 강제 종료 → 자동 회수**
100페이지 PDF 인덱싱 중 `docker kill` → 진행률 정지 → 다른 워커가 이어받아 완료.
*보여줄 것*: 청크 중복 0건, `indexing_job_stalled_recovered_total` 증가. **"소유권을 배타적으로 관리하지 않고 결과를 수렴시킨다"**는 설계 결정을 설명하기 좋은 지점.

**② Kafka 정지 → 무손실 복구**
브로커 정지 상태에서 문서 3건 업로드 → outbox에 PENDING 적재 → 브로커 재시작 → 전부 자동 처리.
*보여줄 것*: 유실 0. "메시지 브로커가 죽어도 데이터가 안 사라지는 이유"를 Outbox로 설명.

**③ 손상 파일 → 즉시 실패 + 재처리**
스캔 PDF 업로드 → `EMPTY_EXTRACTION`으로 **1회 만에** FAILED(재시도 낭비 없음) → **기존 검색 버전 유지 확인** → 정상 파일로 교체 후 재처리 → 완료.
*보여줄 것*: `attempt_count = 1`. 에러 분류가 있고 없고의 차이(5분 vs 즉시).

**④ DB 페일오버 → 무손실 복구** (2차)
업로드 중 Primary kill → 컨슈머 자동 pause → promote → resume → 고아 Job 회수 → 전부 완료.
*보여줄 것*: 유실 0, RTO 측정값. **과제 체크리스트의 "고가용성"과 "실시간 정합성"을 한 장면에서 충족한다.**

---

## 7. 다른 담당자에게 요청할 것

### 7.1 태성님 (`IndexingProcessor`)

- 임베딩 API 실패를 `TransientIndexingException`/`PermanentIndexingException`으로 감싸서 던지기(분류 인터페이스는 A가 제공). 429는 `Retry-After` 헤더 값을 예외에 실어주면 P0-3에서 활용한다.
- 임베딩 배치 진행 중 `indexing_job.processed_chunks` 갱신(P1-8). 배치 단위로만 갱신하면 충분하다.
- 벡터 차원 불일치는 `Permanent`로 분류 — 재시도해도 절대 안 된다.

### 7.2 스키마 / API 서버 담당

| 대상 | 요청 |
|---|---|
| `dead_letter` 테이블 | 신규 생성(P1-6 DDL 참고) |
| `indexing_job` | `phase`, `total_chunks`, `processed_chunks` 컬럼 추가(P1-8) |
| `outbox_event` | `idx_outbox_published` 부분 인덱스 추가(P1-7) |
| `document` | `chunk_purge_failed_count` 컬럼 추가(P2-14) — 대안 논의 가능 |
| `document_version` | `file_size`를 다운로드 전 판정에 쓴다(이미 존재, 신뢰성만 확인) |
| 트랜잭션 | `SET LOCAL app.trace_id`(P2-10) |
| 재처리 API | 게이트 조건을 `status = 'FAILED'`로(스펙 §6.3에서 이미 요청) |

### 7.3 `doc-relay` 담당

- **P1-7 Outbox 보정 배치의 소유권 합의.** 경계상 `doc-relay` 쪽에 두는 게 깨끗하다고 보며, 그쪽에서 맡아준다면 워커는 손대지 않는다. 워커가 맡는다면 `PENDING` 리셋 조건과 `retry_count` 비간섭을 합의해야 한다.
- 발행 실패 지속 시 서킷 브레이커(스펙 §4에 이미 있음).

### 7.4 인프라 담당

- Kafka를 `api-server`/`worker-server`와 독립된 세 번째 배포 단위로(스펙 §6.5, 미해결이면 재확인)
- OpenSQL 클러스터 구성 + 페일오버 절차(P2-11 선행)
- Toxiproxy를 개발 환경 compose에 포함(테스트용)
- 워커 컨테이너 `TZ=UTC` 고정(P2-12)

---

## 부록. Track B 핵심 원칙

1. **Track A의 정합성 메커니즘을 대체하지 않는다.** 에러 분류가 들어와도 `FAILED` 종결 로직 자체는 그대로고, 그 앞에 판단 단계만 붙는다.
2. **새 상태기계를 만들지 않는다.** 고아 회수는 `RETRY_WAIT`으로 되돌려 기존 폴러가 이어받게 하고, DLQ 재처리도 기존 폴러 경로를 재사용한다. 복구 경로가 늘어날수록 "어느 경로가 이 Job을 책임지는가"가 모호해진다.
3. **수렴 설계의 배당금을 계속 받는다.** 두 워커의 동시 실행이 안전하기 때문에, 고아 회수에 lease도 heartbeat도 소유권 재확인도 필요 없다. 새 기능을 넣을 때마다 "이게 수렴을 깨지 않는가"를 먼저 확인한다.
4. **기록하지 못한 실패는 ack하지 않는다.** always-ack 원칙의 유일한 예외이자, 그 원칙을 일관되게 만드는 조항이다.
5. **관측이 먼저, 튜닝은 나중.** 파티션 수·배치 크기·동시성은 메트릭을 보고 나서 손댄다.
6. **감지할 수 없으면 대비한 게 아니다.** 메트릭과 알림이 마지막 조각이자, 없으면 앞의 모든 게 "돌고 있다고 믿는 코드"가 된다.
