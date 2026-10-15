# scripts

Worker의 장애 처리를 로컬에서 재현하고 확인하는 스크립트 모음이다. 장애 시나리오 하나가 폴더 하나이고, 시험 방법은 각 폴더의 `README.md`에 있다.

## 장애 시나리오

| 폴더 | 재현하는 장애 | 구성 | 시험 방법 |
|---|---|---|---|
| [`embedding-outage/`](embedding-outage/) | 외부 임베딩 API 일시 장애(5xx·quota·429·timeout·401)와 breaker 전이 | 가짜 서버 `embedding-stub.py`, 진행 화면 `watch.py` | [시험 방법](embedding-outage/README.md) |

## 새 시나리오를 추가할 때

1. `scripts/<시나리오-이름>/` 폴더를 만들고 스크립트를 둔다.
2. 폴더 안 `README.md`에 모드·기대 결과·시험 방법을 적는다. 형식은 [`embedding-outage/README.md`](embedding-outage/README.md)를 따른다.
3. 위 표에 한 줄을 더한다.
