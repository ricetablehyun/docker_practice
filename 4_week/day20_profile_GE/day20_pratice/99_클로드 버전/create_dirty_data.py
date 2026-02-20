"""
Day 20 실습: 더러운 이커머스 데이터 생성
- 6가지 품질 차원의 문제가 의도적으로 심어져 있음
- 네가 프로파일링으로 이 문제들을 찾아내야 함
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

n_orders = 500

# === 정상 데이터 기본 틀 ===
order_ids = list(range(1001, 1001 + n_orders))
customer_ids = np.random.randint(1, 201, n_orders)
countries = np.random.choice(
    ["South Korea", "Japan", "USA", "Germany", "France"], 
    n_orders, p=[0.4, 0.2, 0.2, 0.1, 0.1]
)
statuses = np.random.choice(
    ["pending", "shipped", "delivered", "cancelled"],
    n_orders, p=[0.1, 0.2, 0.5, 0.2]
)
emails = [f"user{i}@example.com" for i in customer_ids]
amounts = np.round(np.random.uniform(5000, 500000, n_orders), 0)
discounts = np.round(amounts * np.random.uniform(0, 0.3, n_orders), 0)
refunds = np.where(
    statuses == "cancelled",
    amounts * np.random.uniform(0.5, 1.0, n_orders),
    0
)
refunds = np.round(refunds, 0)

base_date = datetime(2024, 12, 1)
order_dates = [base_date + timedelta(days=int(d)) for d in np.random.randint(0, 60, n_orders)]
ship_dates = [od + timedelta(days=int(d)) for od, d in zip(order_dates, np.random.randint(1, 7, n_orders))]

phones = [f"010-{np.random.randint(1000,9999)}-{np.random.randint(1000,9999)}" for _ in range(n_orders)]

df = pd.DataFrame({
    "order_id": order_ids,
    "customer_id": customer_ids,
    "email": emails,
    "phone": phones,
    "country": countries,
    "status": statuses,
    "total_amount": amounts,
    "discount_amount": discounts,
    "refund_amount": refunds,
    "order_date": order_dates,
    "ship_date": ship_dates,
})

# ============================================================
# 이제부터 의도적으로 문제를 심는다
# ============================================================

# --- ① 완전성 (Completeness) 위반 ---
# 필수 컬럼인 order_id에 NULL 3건
df.loc[[10, 55, 200], "order_id"] = np.nan
# customer_id에 NULL 8건
df.loc[[3, 17, 88, 120, 250, 301, 399, 450], "customer_id"] = np.nan

# --- ② 유일성 (Uniqueness) 위반 ---
# order_id 중복 5건 (소스 retry 시뮬레이션)
df.loc[100, "order_id"] = 1050.0  # 1050은 이미 존재
df.loc[150, "order_id"] = 1080.0
df.loc[300, "order_id"] = 1100.0
df.loc[350, "order_id"] = 1150.0
df.loc[400, "order_id"] = 1200.0

# --- ③ 정확성 (Accuracy) 위반 ---
# 음수 금액 3건
df.loc[20, "total_amount"] = -50000
df.loc[77, "total_amount"] = -12000
df.loc[333, "total_amount"] = -8000
# 비현실적 고액 2건 (테스트 데이터 혼입)
df.loc[160, "total_amount"] = 99999999
df.loc[280, "total_amount"] = 88888888

# --- ④ 일관성 (Consistency) 위반 ---
# country 표기 혼재
df.loc[5, "country"] = "korea"
df.loc[30, "country"] = "KR"
df.loc[60, "country"] = "대한민국"
df.loc[90, "country"] = "south korea"
df.loc[130, "country"] = "KOREA"
# email 형식 깨짐
df.loc[15, "email"] = "not-an-email"
df.loc[45, "email"] = "missing@"
df.loc[75, "email"] = "@nodomain.com"
df.loc[105, "email"] = "spaces in@email.com"
# phone 형식 불일치
df.loc[25, "phone"] = "01012345678"       # 하이픈 없음
df.loc[50, "phone"] = "+82-10-1234-5678"  # 국제 포맷
df.loc[85, "phone"] = "02-123-4567"       # 지역번호

# --- ⑤ 적시성 (Timeliness) 위반 ---
# 매우 오래된 날짜 (2020년)
df.loc[180, "order_date"] = datetime(2020, 3, 15)
# 미래 날짜
df.loc[220, "order_date"] = datetime(2099, 12, 31)
df.loc[260, "order_date"] = datetime(2030, 6, 15)

# --- ⑥ 유효성 (Validity) 위반 ---
# 환불액 > 주문액 (비즈니스 규칙 위반)
df.loc[40, "refund_amount"] = 600000
df.loc[40, "total_amount"] = 100000
df.loc[170, "refund_amount"] = 800000
df.loc[170, "total_amount"] = 200000
# 할인액 > 주문액
df.loc[210, "discount_amount"] = 500000
df.loc[210, "total_amount"] = 100000
# 배송일 < 주문일 (시간 역전)
df.loc[310, "ship_date"] = datetime(2024, 11, 1)
df.loc[310, "order_date"] = datetime(2024, 12, 15)
# 비정상 status
df.loc[420, "status"] = "SHIPPED"    # 대소문자 불일치
df.loc[440, "status"] = "complete"   # 존재하지 않는 상태
df.loc[460, "status"] = "refunded"   # 존재하지 않는 상태

# 저장
df.to_csv("/home/claude/day20_practice/dirty_orders.csv", index=False)
print(f"✅ 더러운 데이터 생성 완료: {len(df)}행")
print(f"   저장 위치: /home/claude/day20_practice/dirty_orders.csv")
print(f"\n⚠️ 이 데이터에는 6가지 차원의 문제가 숨어있음")
print(f"   프로파일링으로 찾아내봐!")
