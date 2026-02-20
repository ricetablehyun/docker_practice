import pandas as pd
import great_expectations as gx
import datetime # [NEW] 파이썬 내장 날짜 라이브러리 추가

print("=" * 70)
print("[GE 서킷 브레이커 v4.2] (타입 완전 일치 아키텍처)")
print("=" * 70)

# ---------------------------------------------------------
# [Phase 1: Input] 스키마 강제
# ---------------------------------------------------------
df = pd.read_csv("dirty_orders.csv")
# 여기서 1차 형변환 수행 (문자열 -> 날짜형)
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

context = gx.get_context()
datasource = context.data_sources.add_pandas("ecommerce_source")
data_asset = datasource.add_dataframe_asset(name="dirty_orders")
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_data")

# ---------------------------------------------------------
# [Phase 2: Process] 교전 규칙 세팅
# ---------------------------------------------------------
suite = gx.ExpectationSuite(name="orders_circuit_breaker_v4_2")

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="order_id", meta={"severity": "error", "notes": "PK 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="customer_id", meta={"severity": "error", "notes": "FK 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
    column="order_id", meta={"severity": "error", "notes": "PK 충돌"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="total_amount", min_value=0, max_value=None, 
    meta={"severity": "error", "notes": "결제액 음수"}))

# [FIX] 기준점도 완벽한 날짜 객체(datetime.date)로 2차 형변환 일치시킴
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="order_date", 
    min_value=pd.Timestamp("2024-01-01"),
    max_value=pd.Timestamp("2025-12-31"),
    meta={"severity": "error", "notes": "과거/미래 날짜 유입"}))

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="email", meta={"severity": "warn", "notes": "이메일 누락"}))
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
    column="email", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    meta={"severity": "warn", "notes": "이메일 형식 오타"}))

context.suites.add(suite)

# ---------------------------------------------------------
# [Phase 3: Output] 런타임 주입 및 결과 출력
# ---------------------------------------------------------
validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name="val_orders")
context.validation_definitions.add(validation_definition)
checkpoint = gx.Checkpoint(name="chk_orders", validation_definitions=[validation_definition])

result = checkpoint.run(batch_parameters={"dataframe": df})

print("\n[검증 결과 리포트]")
final_pipeline_kill = False 

for run_id, run_result in result.run_results.items():
    for r in run_result.results:
        exp_type = r.expectation_config.type
        col = r.expectation_config.kwargs.get("column", "unknown")
        meta = r.expectation_config.meta if r.expectation_config.meta else {}
        severity = meta.get("severity", "unknown")
        
        unexpected = "?"
        
        if r.exception_info and r.exception_info.get("raised_exception"):
            err_msg = r.exception_info.get("exception_message", "알 수 없는 에러")
            unexpected = f"내부 연산 붕괴({err_msg.split(':')[0]})"
        elif r.result:
            if "unexpected_count" in r.result and r.result["unexpected_count"] is not None:
                unexpected = f"{r.result['unexpected_count']}건"
            elif "observed_value" in r.result:
                unexpected = f"관측값: {r.result['observed_value']}"
        
        if not r.success:
            if severity == "error":
                print(f"[ERROR] '{col}' -> {exp_type} | {unexpected} 위반 | (Kill)")
                final_pipeline_kill = True
            else:
                print(f"[WARN]  '{col}' -> {exp_type} | {unexpected} 위반 | (Pass)")

print("\n최종 파이프라인 통과 여부:", "중단 (Error 발생)" if final_pipeline_kill else "통과 (경고만 있음)")