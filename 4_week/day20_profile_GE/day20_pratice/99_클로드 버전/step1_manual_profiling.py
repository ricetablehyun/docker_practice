"""
Phase 1: 수동 프로파일링
- GE 쓰기 전에 pandas로 데이터 상태를 눈으로 확인
- 이걸 해야 "어떤 Expectation을 만들지" 판단 가능
"""
import pandas as pd
import numpy as np

# === 데이터 로드 ===
df = pd.read_csv("/home/claude/day20_practice/dirty_orders.csv")

print("=" * 70)
print("📊 STEP 1: 기본 정보 (데이터의 크기와 타입)")
print("=" * 70)
print(f"행 수: {len(df)}")
print(f"컬럼 수: {len(df.columns)}")
print(f"\n컬럼별 타입:")
print(df.dtypes)

print("\n" + "=" * 70)
print("📊 STEP 2: 완전성 프로파일링 (NULL 현황)")
print("=" * 70)
null_report = pd.DataFrame({
    "null_count": df.isnull().sum(),
    "null_pct": (df.isnull().sum() / len(df) * 100).round(2),
    "non_null": df.notnull().sum()
})
print(null_report[null_report["null_count"] > 0])
print(f"\n→ NULL이 있는 컬럼: {null_report[null_report['null_count'] > 0].index.tolist()}")

print("\n" + "=" * 70)
print("📊 STEP 3: 유일성 프로파일링 (중복 현황)")
print("=" * 70)
for col in ["order_id", "customer_id", "email"]:
    total = df[col].notna().sum()  # NULL 제외
    unique = df[col].nunique()
    dup_count = total - unique
    print(f"  {col}: 전체 {total}건, 고유값 {unique}건, 중복 {dup_count}건")

# order_id 중복 상세
dup_orders = df[df["order_id"].duplicated(keep=False) & df["order_id"].notna()]
if len(dup_orders) > 0:
    print(f"\n  ⚠️ 중복 order_id 상세:")
    print(dup_orders[["order_id", "customer_id", "total_amount"]].head(10))

print("\n" + "=" * 70)
print("📊 STEP 4: 정확성 프로파일링 (숫자 컬럼 범위)")
print("=" * 70)
numeric_cols = ["total_amount", "discount_amount", "refund_amount"]
for col in numeric_cols:
    stats = df[col].describe()
    negative = (df[col] < 0).sum()
    zero = (df[col] == 0).sum()
    print(f"\n  [{col}]")
    print(f"    최솟값: {stats['min']:,.0f}")
    print(f"    최댓값: {stats['max']:,.0f}")
    print(f"    평균: {stats['mean']:,.0f}")
    print(f"    중앙값: {stats['50%']:,.0f}")
    print(f"    음수: {negative}건")
    print(f"    0: {zero}건")

print("\n" + "=" * 70)
print("📊 STEP 5: 일관성 프로파일링 (카테고리 & 패턴)")
print("=" * 70)

# country 고유값 (표기 불일치 확인)
print(f"\n  [country] 고유값 ({df['country'].nunique()}개):")
print(df["country"].value_counts().to_string())

# status 고유값
print(f"\n  [status] 고유값 ({df['status'].nunique()}개):")
print(df["status"].value_counts().to_string())

# email 형식 체크
import re
email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
invalid_emails = df[~df["email"].apply(lambda x: bool(email_pattern.match(str(x))))]
print(f"\n  [email] 형식 위반: {len(invalid_emails)}건")
if len(invalid_emails) > 0:
    print(invalid_emails[["order_id", "email"]].head(10))

# phone 형식 체크
phone_pattern = re.compile(r'^010-\d{4}-\d{4}$')
invalid_phones = df[~df["phone"].apply(lambda x: bool(phone_pattern.match(str(x))))]
print(f"\n  [phone] 형식 위반 (010-XXXX-XXXX 기준): {len(invalid_phones)}건")
if len(invalid_phones) > 0:
    print(invalid_phones[["order_id", "phone"]].head(5))

print("\n" + "=" * 70)
print("📊 STEP 6: 적시성 프로파일링 (날짜 범위)")
print("=" * 70)
df["order_date"] = pd.to_datetime(df["order_date"])
df["ship_date"] = pd.to_datetime(df["ship_date"])

print(f"\n  [order_date]")
print(f"    최소 날짜: {df['order_date'].min()}")
print(f"    최대 날짜: {df['order_date'].max()}")

# 예상 범위 밖 (2024-12 ~ 2025-01이 정상)
old_dates = df[df["order_date"] < "2024-01-01"]
future_dates = df[df["order_date"] > "2025-12-31"]
print(f"    2024년 이전: {len(old_dates)}건")
print(f"    2025년 이후(미래): {len(future_dates)}건")

print("\n" + "=" * 70)
print("📊 STEP 7: 유효성 프로파일링 (비즈니스 규칙)")
print("=" * 70)

# 환불액 > 주문액
invalid_refund = df[df["refund_amount"] > df["total_amount"]]
print(f"\n  환불액 > 주문액: {len(invalid_refund)}건")
if len(invalid_refund) > 0:
    print(invalid_refund[["order_id", "total_amount", "refund_amount"]].head())

# 할인액 > 주문액
invalid_discount = df[df["discount_amount"] > df["total_amount"]]
print(f"\n  할인액 > 주문액: {len(invalid_discount)}건")
if len(invalid_discount) > 0:
    print(invalid_discount[["order_id", "total_amount", "discount_amount"]].head())

# 배송일 < 주문일 (시간 역전)
time_reversal = df[df["ship_date"] < df["order_date"]]
print(f"\n  배송일 < 주문일 (시간 역전): {len(time_reversal)}건")
if len(time_reversal) > 0:
    print(time_reversal[["order_id", "order_date", "ship_date"]].head())

print("\n" + "=" * 70)
print("📋 프로파일링 요약: 발견된 문제")
print("=" * 70)
print("""
  ① 완전성: order_id NULL, customer_id NULL 존재
  ② 유일성: order_id 중복 존재
  ③ 정확성: total_amount에 음수, 비현실적 고액
  ④ 일관성: country 표기 5가지+, email/phone 형식 불일치, status 대소문자
  ⑤ 적시성: 2020년 데이터, 2099년 미래 데이터
  ⑥ 유효성: 환불>주문, 할인>주문, 배송일<주문일

→ 이 결과를 바탕으로 GE Expectation Suite를 설계한다!
""")
