-- 미션: 배달 앱 주문 데이터 설계

-- Step 1: Fact 테이블 생성
CREATE TABLE fact_deliveries (
    delivery_id BIGINT PRIMARY KEY,
    customer_key INT,   -- FK
    restaurant_key INT, -- FK
    rider_key INT,      -- FK
    date_key INT,       -- FK
    
    -- 측정값 (네가 직접 추가해봐)
    order_amount DECIMAL,
    delivery_fee DECIMAL,
    -- 뭐가 더 필요할까? 힌트: 배달 시간?
);

-- Step 2: Dimension 테이블 (네가 만들어봐)
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    -- 어떤 컬럼 필요? 힌트: 이름, 주소, 전화번호...
);
