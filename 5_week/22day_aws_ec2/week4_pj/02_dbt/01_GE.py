# import pandas as pd
# import great_expectations as gx

# print("=" * 70)
# print("  주말 프로젝트 — GE 서킷 브레이커 (스타 스키마 기반)")
# print("=" * 70)

# # ============================================================
# # Phase 1: 데이터 로드 + 타입 강제
# # ============================================================
# df = pd.read_csv("dirty_orders.csv")
# df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
# df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

# print(f"\n📦 로드 완료: {len(df)}행, {len(df.columns)}컬럼")

# # ============================================================
# # Phase 2: GE 세팅
# # ============================================================
# context = gx.get_context()
# datasource = context.data_sources.add_pandas("weekend_source")
# data_asset = datasource.add_dataframe_asset(name="dirty_orders")
# batch_def = data_asset.add_batch_definition_whole_dataframe("full")

# # ============================================================
# # Phase 3: Expectation Suite — 스타 스키마 기반 규칙
# # ============================================================
# suite = gx.ExpectationSuite(name="weekend_circuit_breaker")

# # ─── ERROR: 위반 시 파이프라인 Kill ─────────────────────────

# # ① 완전성 — Fact PK/FK NULL → JOIN 붕괴
# suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
#     column="order_id",
#     meta={"severity": "error", "dim": "완전성", "why": "Fact PK 누락 → 주문 식별 불가"}
# ))
# suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
#     column="customer_id",
#     meta={"severity": "error", "dim": "완전성", "why": "FK 누락 → dim_customers JOIN 깨짐"}
# ))

# # ② 유일성 — PK 중복 → 매출 이중 계산
# suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
#     column="order_id",
#     meta={"severity": "error", "dim": "유일성", "why": "PK 중복 → SUM 뻥튀기"}
# ))

# # ③ 도메인 타당성 — 음수 결제는 소스 오류, SQL로 복구 불가
# suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
#     column="total_amount", min_value=0, max_value=None,
#     meta={"severity": "error", "dim": "도메인", "why": "음수 결제 → 소스 시스템 버그"}
# ))

# # ④ 적시성 — 비즈니스 기간 밖 날짜는 소스 오류
# suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
#     column="order_date",
#     min_value=pd.Timestamp("2024-01-01"),
#     max_value=pd.Timestamp("2025-12-31"),
#     meta={"severity": "error", "dim": "적시성", "why": "과거/미래 날짜 → 데이터 신뢰도 붕괴"}
# ))

# # ─── WARN: 경고만, 파이프라인 계속 ─────────────────────────

# # ⑤ email — 매출 집계 영향 없음, 마케팅만 지장
# suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
#     column="email",
#     meta={"severity": "warn", "dim": "완전성", "why": "JOIN키 아님, 마케팅만 영향"}
# ))
# suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
#     column="email", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$",
#     meta={"severity": "warn", "dim": "일관성", "why": "형식 오류, 파이프라인 영향 없음"}
# ))

# context.suites.add(suite)

# # ============================================================
# # Phase 4: 실행 + 커스텀 스위치
# # ============================================================
# vd = gx.ValidationDefinition(data=batch_def, suite=suite, name="vd_weekend")
# context.validation_definitions.add(vd)
# cp = gx.Checkpoint(name="cp_weekend", validation_definitions=[vd])
# result = cp.run(batch_parameters={"dataframe": df})

# # ============================================================
# # Phase 5: 결과 출력 — meta 기반 Error/Warn 분리
# # ============================================================
# print("\n" + "=" * 70)
# print("  📊 검증 결과 리포트")
# print("=" * 70)

# final_kill = False
# error_count = 0
# warn_count = 0

# for run_id, run_result in result.run_results.items():
#     for r in run_result.results:
#         if r.success:
#             continue

#         col = r.expectation_config.kwargs.get("column", "?")
#         exp_type = r.expectation_config.type
#         meta = r.expectation_config.meta or {}
#         severity = meta.get("severity", "unknown")
#         dim = meta.get("dim", "?")
#         why = meta.get("why", "")

#         # 위반 건수 추출
#         count = "?"
#         if r.result:
#             if "unexpected_count" in r.result and r.result["unexpected_count"] is not None:
#                 count = f"{r.result['unexpected_count']}건"
#             elif "observed_value" in r.result:
#                 count = f"관측: {r.result['observed_value']}"

#         if severity == "error":
#             print(f"  🛑 ERROR [{dim}] '{col}' → {count} 위반 | {why}")
#             final_kill = True
#             error_count += 1
#         else:
#             print(f"  ⚠️  WARN  [{dim}] '{col}' → {count} 위반 | {why}")
#             warn_count += 1

# print("\n" + "-" * 70)
# print(f"  Error: {error_count}개 | Warn: {warn_count}개")
# print(f"  최종 판정: {'❌ 파이프라인 중단 (Error 발생)' if final_kill else '✅ 파이프라인 통과 (경고만)'}")
# print("=" * 70)

# if final_kill:
#     print("\n  [실무라면] 소스팀에 보고 → 원인 파악 → 규칙 재조정")
#     print("  [학습 목적] Phase 3(dbt)으로 계속 진행합니다.")

import pandas as pd
import datetime
import great_expectations as gx

print("=" * 70)
print("  주말 프로젝트 — GE 서킷 브레이커 (마스터 아키텍처 v1.x)")
print("=" * 70)

# ============================================================
# Phase 1: 데이터 로드 + 타입 강제
# ============================================================
df = pd.read_csv("dirty_orders.csv")
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

print(f"\n📦 로드 완료: {len(df)}행, {len(df.columns)}컬럼")

# ============================================================
# Phase 2: 최신 GE 스마트 환경 세팅
# ============================================================
context = gx.get_context()
datasource = context.data_sources.add_pandas("weekend_source")
# 구버전 RuntimeBatchRequest 대신, 최신 DataFrame Asset 직결 방식 사용
data_asset = datasource.add_dataframe_asset(name="dirty_orders")
batch_def = data_asset.add_batch_definition_whole_dataframe("full_data")

# ============================================================
# Phase 3: Expectation Suite — 설계자님의 6차원 완벽 방어선
# ============================================================
suite = gx.ExpectationSuite(name="weekend_circuit_breaker")

# ─── 🛑 ERROR: 위반 시 파이프라인 Kill ─────────────────────────
# ① 완전성
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="order_id", meta={"severity": "error", "dim": "완전성", "why": "Fact PK 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="customer_id", meta={"severity": "error", "dim": "완전성", "why": "FK 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="status", meta={"severity": "error", "dim": "완전성", "why": "상태 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="total_amount", meta={"severity": "error", "dim": "완전성", "why": "매출 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="order_date", meta={"severity": "error", "dim": "완전성", "why": "날짜 누락"}))

# ② 유일성
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
    column="order_id", meta={"severity": "error", "dim": "유일성", "why": "PK 중복"}))

# ③ 도메인 (유효성)
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="status", value_set=["cancelled", "delivered", "pending", "shipped", "refunded"],
    meta={"severity": "error", "dim": "도메인", "why": "잘못된 상태 코드"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="country", value_set=["Japan", "USA", "South Korea", "France", "Germany", "KR", "korea"],
    meta={"severity": "error", "dim": "도메인", "why": "미등록 국가 유입"}))

# ④ 적시성
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="order_date", 
    min_value=datetime.datetime(2024, 12, 1), 
    max_value=datetime.datetime.now(),
    meta={"severity": "error", "dim": "적시성", "why": "과거/미래 날짜 유입"}))


# ─── ⚠️ WARN: 경고만, 파이프라인 계속 (dbt 수술 대상) ───────────
# ⑤ 일관성
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
    column="email", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    meta={"severity": "warn", "dim": "일관성", "why": "이메일 형식 오류"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
    column="phone", regex=r"^010-\d{4}-\d{4}$",
    meta={"severity": "warn", "dim": "일관성", "why": "전화번호 포맷 오류"}))

context.suites.add(suite)

# ============================================================
# Phase 4: 최신 Checkpoint 실행
# ============================================================
validation_definition = gx.ValidationDefinition(data=batch_def, suite=suite, name="vd_weekend")
context.validation_definitions.add(validation_definition)
checkpoint = gx.Checkpoint(name="cp_weekend", validation_definitions=[validation_definition])

# 구버전 dict 떡칠 대신 깔끔한 파라미터 주입
result = checkpoint.run(batch_parameters={"dataframe": df})

# ============================================================
# Phase 5: 관제탑 결과 출력 (에러 방어 로직 추가)
# ============================================================
print("\n" + "=" * 70)
print("  📊 검증 결과 리포트")
print("=" * 70)

final_kill = False
error_count = 0
warn_count = 0

for run_id, run_result in result.run_results.items():
    for r in run_result.results:
        if r.success:
            continue

        col = r.expectation_config.kwargs.get("column", "?")
        exp_type = r.expectation_config.type
        meta = r.expectation_config.meta if r.expectation_config.meta else {}
        severity = meta.get("severity", "unknown")
        dim = meta.get("dim", "?")
        why = meta.get("why", "")

        count = "?"
        # 예외 처리 방어 로직 추가 (데이터 타입 충돌 등)
        if r.exception_info and r.exception_info.get("raised_exception"):
            count = f"연산 에러"
        elif r.result:
            if "unexpected_count" in r.result and r.result["unexpected_count"] is not None:
                count = f"{r.result['unexpected_count']}건"
            elif "observed_value" in r.result:
                count = f"관측: {r.result['observed_value']}"

        if severity == "error":
            print(f"  🛑 ERROR [{dim}] '{col}' → {count} 위반 | {why}")
            final_kill = True
            error_count += 1
        else:
            print(f"  ⚠️  WARN  [{dim}] '{col}' → {count} 위반 | {why}")
            warn_count += 1

print("\n" + "-" * 70)
print(f"  Error: {error_count}개 | Warn: {warn_count}개")
print(f"  최종 판정: {'❌ 파이프라인 중단 (Error 발생)' if final_kill else '✅ 파이프라인 통과 (경고만)'}")
print("=" * 70)

# 리포트 빌드
context.build_data_docs()
