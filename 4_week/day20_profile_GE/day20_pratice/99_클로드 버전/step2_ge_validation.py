"""
Phase 2: Great Expectations — 6가지 차원 자동 검증
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
프로파일링에서 발견한 문제를 자동으로 잡는 규칙을 만든다.

실무 흐름:
  1. 프로파일링으로 문제 발견 (Phase 1에서 완료)
  2. 발견한 문제 기반으로 Expectation Suite 설계 (이 파일)
  3. Airflow DAG에 태워서 매일 자동 실행 (Week 5에서)
"""
import great_expectations as gx
import great_expectations.expectations as gxe
import warnings
warnings.filterwarnings("ignore")

print("=" * 70)
print("🔧 Great Expectations v1.x — 6가지 차원 자동 검증")
print("=" * 70)

# ============================================================
# STEP 1: GE 초기 설정 (Data Context + Data Source + Batch)
# ============================================================
print("\n📌 STEP 1: GE 초기 설정")

# Data Context: GE의 본부 (모든 설정이 여기에 모임)
context = gx.get_context()

# Data Source: 데이터가 어디에 있는지 등록
datasource = context.data_sources.add_pandas("ecommerce_source")

# Data Asset: 어떤 파일/테이블을 검증할지
data_asset = datasource.add_csv_asset(
    name="dirty_orders",
    filepath_or_buffer="/home/claude/day20_practice/dirty_orders.csv"
)

# Batch Definition: 데이터를 어떻게 가져올지
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_data")

print("  ✅ Context → DataSource → Asset → Batch 설정 완료")

# ============================================================
# STEP 2: Expectation Suite 생성 (6가지 차원별 규칙)
# ============================================================
print("\n📌 STEP 2: Expectation Suite 생성 (6가지 차원)")

suite = context.suites.add(
    gx.ExpectationSuite(name="orders_quality_suite")
)

# ----------------------------------------------------------
# ① 완전성 (Completeness): 필수 컬럼에 NULL이 없어야 한다
# ----------------------------------------------------------
print("\n  ① 완전성 (Completeness)")

# 필수 컬럼: order_id, customer_id (JOIN에 쓰이는 키)
suite.add_expectation(
    gxe.ExpectColumnValuesToNotBeNull(column="order_id")
)
print("    → order_id NOT NULL")

suite.add_expectation(
    gxe.ExpectColumnValuesToNotBeNull(column="customer_id")
)
print("    → customer_id NOT NULL")

# 행 수가 0이 아닌지 (빈 성공 방지)
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=1)
)
print("    → 행 수 >= 1 (빈 테이블 방지)")

# ----------------------------------------------------------
# ② 유일성 (Uniqueness): PK 중복이 없어야 한다
# ----------------------------------------------------------
print("\n  ② 유일성 (Uniqueness)")

suite.add_expectation(
    gxe.ExpectColumnValuesToBeUnique(column="order_id")
)
print("    → order_id UNIQUE")

# ----------------------------------------------------------
# ③ 정확성 (Accuracy): 값이 현실적 범위 안에 있어야 한다
# ----------------------------------------------------------
print("\n  ③ 정확성 (Accuracy)")

# total_amount: 0 ~ 10,000,000원 (이커머스 기준 합리적 범위)
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="total_amount",
        min_value=0,
        max_value=10000000
    )
)
print("    → total_amount: 0 ~ 10,000,000")

# discount_amount: 0 이상
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="discount_amount",
        min_value=0,
        max_value=10000000
    )
)
print("    → discount_amount: 0 ~ 10,000,000")

# refund_amount: 0 이상
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="refund_amount",
        min_value=0,
        max_value=10000000
    )
)
print("    → refund_amount: 0 ~ 10,000,000")

# ----------------------------------------------------------
# ④ 일관성 (Consistency): 포맷이 통일되어야 한다
# ----------------------------------------------------------
print("\n  ④ 일관성 (Consistency)")

# status는 정해진 값만 허용
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["pending", "shipped", "delivered", "cancelled"]
    )
)
print("    → status: pending/shipped/delivered/cancelled만 허용")

# country는 정해진 값만 허용
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(
        column="country",
        value_set=["South Korea", "Japan", "USA", "Germany", "France"]
    )
)
print("    → country: 5개국 정확한 표기만 허용")

# email 형식 (정규표현식 — dbt test로는 못 하는 검증!)
suite.add_expectation(
    gxe.ExpectColumnValuesToMatchRegex(
        column="email",
        regex=r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    )
)
print("    → email: 정규표현식 형식 검증")

# phone 형식
suite.add_expectation(
    gxe.ExpectColumnValuesToMatchRegex(
        column="phone",
        regex=r"^010-\d{4}-\d{4}$"
    )
)
print("    → phone: 010-XXXX-XXXX 형식 검증")

# ----------------------------------------------------------
# ⑤ 적시성 (Timeliness): 데이터가 합리적 시간 범위 안에 있어야 한다
# ----------------------------------------------------------
print("\n  ⑤ 적시성 (Timeliness)")

# 행 수가 합리적 범위 (빈 성공 + 폭증 방지)
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(
        min_value=100,
        max_value=100000
    )
)
print("    → 행 수: 100 ~ 100,000 (비정상 급감/급증 방지)")

# ----------------------------------------------------------
# ⑥ 유효성 (Validity): 비즈니스 규칙을 만족해야 한다
# ----------------------------------------------------------
print("\n  ⑥ 유효성 (Validity)")

# 환불액 ≤ 주문액 → total_amount ≥ refund_amount
# GE에서는 column_A >= column_B를 "A to be greater than B"로 표현
suite.add_expectation(
    gxe.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="total_amount",
        column_B="refund_amount",
        or_equal=True  # >= 허용
    )
)
print("    → total_amount >= refund_amount (환불≤주문)")

# 할인액 ≤ 주문액
suite.add_expectation(
    gxe.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="total_amount",
        column_B="discount_amount",
        or_equal=True
    )
)
print("    → total_amount >= discount_amount (할인≤주문)")

print(f"\n  📋 총 Expectation 수: {len(suite.expectations)}개")

# ============================================================
# STEP 3: 검증 실행 (Checkpoint)
# ============================================================
print("\n" + "=" * 70)
print("📌 STEP 3: 검증 실행")
print("=" * 70)

# Validation Definition: Suite와 Batch를 연결
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="orders_validation",
        data=batch_definition,
        suite=suite
    )
)

# Checkpoint: 실행기
checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name="orders_checkpoint",
        validation_definitions=[validation_definition]
    )
)

# 실행!
result = checkpoint.run()

# ============================================================
# STEP 4: 결과 해석
# ============================================================
print("\n" + "=" * 70)
print("📌 STEP 4: 검증 결과")
print("=" * 70)

# 차원별 매핑 (어떤 Expectation이 어떤 차원인지)
dimension_map = {
    "expect_column_values_to_not_be_null": "① 완전성",
    "expect_table_row_count_to_be_between": "⑤ 적시성",
    "expect_column_values_to_be_unique": "② 유일성",
    "expect_column_values_to_be_between": "③ 정확성",
    "expect_column_values_to_be_in_set": "④ 일관성",
    "expect_column_values_to_match_regex": "④ 일관성",
    "expect_table_row_count_to_be_between": "⑤ 적시성",
    "expect_column_pair_values_a_to_be_greater_than_b": "⑥ 유효성",
}

pass_count = 0
fail_count = 0

for vr_key, vr in result.run_results.items():
    for er in vr.results:
        exp_type = er.expectation_config.type
        success = er.success
        dimension = dimension_map.get(exp_type, "?")
        
        # Expectation에서 컬럼명 추출
        kwargs = er.expectation_config.kwargs
        col_info = kwargs.get("column", kwargs.get("column_A", ""))
        
        status = "PASS ✅" if success else "FAIL ❌"
        
        if success:
            pass_count += 1
            print(f"  {status} [{dimension}] {exp_type}")
            print(f"         컬럼: {col_info}")
        else:
            fail_count += 1
            print(f"  {status} [{dimension}] {exp_type}")
            print(f"         컬럼: {col_info}")
            
            # 실패 상세 정보
            r = er.result
            if "unexpected_count" in r:
                total = r.get("element_count", "?")
                unexpected = r.get("unexpected_count", "?")
                pct = r.get("unexpected_percent", r.get("unexpected_percent_total", "?"))
                print(f"         위반: {unexpected}건 / {total}건 ({pct}%)")
                
                # 위반 값 샘플
                samples = r.get("partial_unexpected_list", [])
                if samples:
                    print(f"         샘플: {samples[:5]}")
        print()

# ============================================================
# 최종 요약
# ============================================================
print("=" * 70)
print("📋 최종 요약")
print("=" * 70)
print(f"  전체 Expectation: {pass_count + fail_count}개")
print(f"  PASS: {pass_count}개")
print(f"  FAIL: {fail_count}개")
print(f"  전체 통과: {'✅ YES' if result.success else '❌ NO'}")

print(f"""
{'=' * 70}
💡 실무에서 이 다음 단계
{'=' * 70}
  1. FAIL → Slack 알람 전송 (Airflow에서)
  2. FAIL → dbt run 실행 차단 (파이프라인 중단)
  3. PASS → dbt run 진행 → dbt test (출구 필터)
  4. 결과를 HTML 리포트로 팀 공유 (build_data_docs)

  Airflow DAG 의사코드:
    ge_validate >> [PASS] >> dbt_run >> dbt_test >> report
                >> [FAIL] >> slack_alert >> STOP
""")
