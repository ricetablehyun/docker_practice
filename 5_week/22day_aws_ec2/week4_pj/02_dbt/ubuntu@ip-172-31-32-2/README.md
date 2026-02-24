# Week 4 주말 프로젝트 — 이커머스 DW 파이프라인

> **역할:** 이커머스 스타트업 1인 데이터 엔지니어  
> **미션:** CEO 요청 "주문 데이터로 매출 대시보드 만들어줘"  
> **기간:** 1일 (6~7시간)

---

## 프로젝트 개요

500건의 더러운 주문 데이터(`dirty_orders.csv`)를 받아서, 품질 검증 → 정제 → 비즈니스 마트 구축까지 전체 데이터 파이프라인을 설계·구현한 프로젝트입니다.

Week 4 커리큘럼(Day 16~20)에서 학습한 **스타 스키마, dbt, Great Expectations, 데이터 프로파일링**을 하나의 파이프라인으로 통합했습니다.

---

## 아키텍처

```
[dirty_orders.csv]
        │
        ▼
┌──────────────────────┐
│  Phase 2: GE 서킷    │  ← 입구 검증 (Error → Kill / Warn → Pass)
│  브레이커             │     완전성, 유일성, 도메인, 적시성 체크
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  BigQuery (raw)      │  ← raw_dirty_orders.dirty_orders
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  Phase 3: dbt        │
│  ┌─ staging ─────┐   │  ← stg_dirty: row 단위 정제 + Surrogate Key
│  │  stg_dirty    │   │     NULL 제거, 일관성 통일, 타입 변환
│  └───────┬───────┘   │
│          ▼           │
│  ┌─ marts ───────┐   │  ← 비즈니스 요구사항 기반
│  │ fct_earnings  │   │     순이익 = total - discount - refund
│  │ dim_customers │   │     고객 나라별 분포 + 주문 통계
│  └───────┬───────┘   │
│          ▼           │
│  Phase 4: dbt test   │  ← 출구 검증
│  built-in + singular │     unique, not_null, accepted_values
│                      │     환불≤주문, 할인≤주문 (Warn)
└──────────────────────┘
```

---

## 기술 스택

| 구분 | 도구 | 용도 |
|------|------|------|
| DW | BigQuery | 클라우드 데이터 웨어하우스 |
| 변환 | dbt 1.11.4 | ELT 변환 + 테스트 + 문서화 |
| 품질 검증 | Great Expectations | 입구 서킷 브레이커 |
| 언어 | SQL, Python, Jinja | 변환 로직 + 매크로 |

---

## 프로젝트 구조

```
week4_practice/
├── models/
│   ├── staging/
│   │   ├── sources.yml          # BigQuery 소스 정의
│   │   └── stg_dirty.sql        # 원본 정제 (view)
│   ├── marts/
│   │   ├── fct_earnings.sql     # 순이익 Fact (incremental)
│   │   └── dim_customers.sql    # 고객 분포 Dimension (table)
│   └── schema.yml               # 테스트 정의
├── macros/
│   └── get_region.sql           # 국가 → 지역 분류 매크로
├── snapshots/
│   └── orders_snapshot.sql      # SCD Type 2 (주문 상태 변경 이력)
├── tests/
│   ├── assert_refund_lte_order.sql    # 환불 ≤ 주문액
│   └── assert_discount_lte_order.sql  # 할인 ≤ 주문액
├── ge/
│   └── weekend_ge.py            # GE 서킷 브레이커 스크립트
├── data/
│   └── dirty_orders.csv         # 원본 데이터 (500행, 11컬럼)
└── README.md
```

---

## Phase별 상세

### Phase 1: 스타 스키마 설계

**Grain:** 주문 1건 (order_id 기반 → Surrogate Key로 전환)

```
fct_earnings (Fact):
  PK: order_sk (FARM_FINGERPRINT 해시)
  FK: customer_id
  Measures: total_amount, discount_amount, refund_amount, net_profit
  Dates: order_date, ship_date (BigQuery 파티셔닝용 DATE 유지)
  기타: status (주문 상태 — 이벤트 속성)

dim_customers (Dimension):
  PK: customer_id
  컬럼: email, phone, country, region, order_count, total_spent
```

**설계 판단:**
- `dim_date` 미생성 → BigQuery에서는 DATE 컬럼 직접 보유가 파티셔닝 비용상 유리
- `country`를 독립 Dimension으로 분리하지 않음 → 컬럼 1개짜리 dim은 비효율
- `status`는 Fact에 보관 → 고객 속성이 아닌 주문 이벤트 속성

### Phase 2: GE 서킷 브레이커

**품질 차원 배치 전략:**

| 차원 | 검증 위치 | Severity | 근거 |
|------|-----------|----------|------|
| 완전성 (order_id NULL) | GE | Error | PK 누락 → 식별 불가 |
| 완전성 (customer_id NULL) | GE | Error | FK 누락 → JOIN 붕괴 |
| 유일성 (order_id 중복) | GE | Error | PK 중복 → SUM 뻥튀기 |
| 도메인 (total_amount 음수) | GE | Error | SQL로 복구 불가 |
| 적시성 (order_date 범위) | GE | Error | 비즈니스 기간 이탈 |
| 완전성 (email NULL) | GE | Warn | 매출 영향 없음 |
| 일관성 (email 형식) | GE | Warn | 마케팅만 영향 |
| 일관성 (country 통일) | dbt | - | SQL로 정제 가능 |
| 비즈니스 유효성 | dbt test | Warn | 환불>주문, 할인>주문 |

**GE 실행 결과:**
```
🛑 ERROR 5개: order_id NULL(3), 중복(10), customer_id NULL(8), 음수(3), 날짜(3)
⚠️  WARN  1개: email 형식(4)
→ 파이프라인 중단 (학습 목적으로 계속 진행)
```

### Phase 3: dbt 변환

**staging (`stg_dirty`):**
- Surrogate Key 생성: `FARM_FINGERPRINT(order_id + customer_id + order_date)`
  - 중복 order_id 5건(10행)이 각각 다른 고객의 주문으로 확인 → 새 PK 부여
- 일관성 정제: country 표기 통일 (korea/KR/kr → South Korea)
- 타입 변환: DATE 캐스팅, INT64 캐스팅
- 불량 row 제거: NULL PK, 음수 금액, 범위 밖 날짜
- 매크로 적용: `get_region()` → 국가별 지역 분류

**marts (`fct_earnings`):**
- 비즈니스 로직: `net_profit = total_amount - discount_amount - refund_amount`
- Incremental 모델: `unique_key='order_sk'`, 새 주문만 처리

**marts (`dim_customers`):**
- 같은 고객 여러 주문 → `ROW_NUMBER()`로 최신 정보만 남김
- 집계: `order_count`, `total_spent` 추가

### Phase 4: dbt test

**최종 결과: PASS 12 | WARN 2 | ERROR 1**

| 테스트 | 대상 | 결과 |
|--------|------|------|
| unique (order_sk) | stg_dirty | ✅ PASS |
| not_null (order_sk) | stg_dirty | ✅ PASS |
| not_null (customer_id) | stg_dirty | ✅ PASS |
| unique (order_sk) | fct_earnings | ✅ PASS |
| not_null (order_sk) | fct_earnings | ✅ PASS |
| not_null (net_profit) | fct_earnings | ✅ PASS |
| accepted_values (status) | fct_earnings | ✅ PASS |
| unique (customer_id) | dim_customers | ✅ PASS |
| not_null (customer_id) | dim_customers | ✅ PASS |
| assert_refund_lte_order | fct_earnings | ⚠️ WARN (3건) |
| assert_discount_lte_order | fct_earnings | ⚠️ WARN (3건) |
| orders_snapshot | snapshots | ❌ ERROR (무료티어 DML 제한) |

**비즈니스 유효성 FAIL → Warn 처리 근거:**
- 이커머스 대시보드 특성상 3건 때문에 전체 483건 배포를 막으면 비즈니스 손해가 더 큼
- 실무에서는 Slack 알림 → 운영팀 수동 검토 → 소스 시스템 수정 프로세스

### Phase 5: 문서화

- `dbt docs generate` + `dbt docs serve`
- Lineage Graph 확인: `source → stg_dirty → fct_earnings / dim_customers → tests`

---

## 핵심 의사결정 기록

| 상황 | 판단 | 근거 |
|------|------|------|
| dim_date 생성 여부 | 미생성 | BigQuery 파티셔닝은 DATE 컬럼 필요, INT FK로는 불가 |
| 중복 order_id 처리 | Surrogate Key | 각기 다른 고객 주문 → 삭제 불가, 새 PK 부여 |
| status 위치 | Fact에 보관 | 고객 속성 아닌 주문 이벤트 속성 |
| GE vs dbt 역할 | 파급력 기반 | 뒷단에서 고치면 문제 커지는 것 → GE에서 예방 |
| 비즈니스 FAIL 처리 | Warn | 이커머스 → 경고 + 알림, 금융이었으면 STOP |

---

## Lineage Graph

```
external_source.dirty_orders
        │
        ▼
    stg_dirty
    ┌───┼────────────┐
    ▼   ▼            ▼
dim_customers  fct_earnings  orders_snapshot
                ┌────┴────┐
                ▼         ▼
    assert_refund  assert_discount
```

---

## 학습 포인트

1. **데이터 엔지니어링은 코드가 아니라 판단이다** — Grain 결정, Error/Warn 분류, marts 설계 모두 비즈니스 맥락에 따른 판단
2. **GE는 서킷 브레이커, dbt test는 출구 검증** — 같은 "테스트"지만 역할이 완전히 다름
3. **도구의 특성을 아는 게 설계를 바꾼다** — BigQuery 파티셔닝 때문에 dim_date 설계가 뒤집힘
4. **PK를 무조건 신뢰하지 마라** — 소스 데이터의 PK가 깨질 수 있고, Surrogate Key로 대응
5. **staging은 정답이 있고, marts는 요구사항이 있어야 한다** — AI가 staging은 할 수 있지만 marts는 비즈니스 판단 없이 만들 수 없음

---

## 실행 방법

```bash
# 1. 가상환경 활성화
conda activate new_en

# 2. GE 서킷 브레이커 실행 (입구 검증)
cd ge/
python weekend_ge.py

# 3. dbt 빌드 (변환 + 테스트)
cd ../
dbt run          # staging → marts
dbt snapshot     # SCD Type 2
dbt test         # built-in + singular

# 4. 문서화
dbt docs generate
dbt docs serve
```

---

## 참고

- **커리큘럼:** DE 30일 커리큘럼 Week 4 (Day 16~20)
- **환경:** macOS, BigQuery (asia-northeast3), dbt 1.11.4, GE, Python 3.12