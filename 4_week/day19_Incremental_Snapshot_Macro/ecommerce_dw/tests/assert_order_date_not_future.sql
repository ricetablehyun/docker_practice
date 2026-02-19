-- 비즈니스 룰: "미래 날짜 주문은 없어야 함"
-- 미래 날짜 주문 찾기

SELECT
    order_id,
    order_date,
    CURRENT_DATE() as today
FROM {{ ref('stg_orders') }}
WHERE order_date > CURRENT_DATE()

 /**핵심 이해:**
```
Singular test = "잘못된 데이터 찾기"

정상: order_date <= CURRENT_DATE() (과거 주문)
비정상: order_date > CURRENT_DATE() (미래 주문)

테스트는 "비정상" 찾는 거!
  → WHERE order_date > CURRENT_DATE()
  → 이런 행이 0건이면 PASS ✅
  → 이런 행이 1건 이상이면 FAIL ❌
```*/ 