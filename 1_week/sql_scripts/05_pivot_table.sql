-- 테이블 생성 (월별 매출)
CREATE TABLE monthly_sales (
    month INT,
    category VARCHAR(20),
    amount INT
);

-- 데이터 심기 (전자제품과 의류의 1, 2월 매출)
INSERT INTO monthly_sales VALUES
(1, 'Electronics', 1000),
(1, 'Electronics', 2000),
(1, 'Clothing', 500),
(2, 'Electronics', 1500),
(2, 'Clothing', 1000),
(2, 'Clothing', 300);

-- 일반 group by시 결과는 세로로 나오지만 가로로 하고싶다면 pivate-table인 case when을 쓴다.
SELECT month, category, SUM(amount) 
FROM monthly_sales
GROUP BY month, category
ORDER BY month;

-- 피벗 테이블화 한다면 
-- "월별(Row)로, 카테고리(Column)를 펼쳐서 보여줘!"
SELECT 
    month,
    -- 1. 전자제품 컬럼 만들기
    SUM(CASE WHEN category = 'Electronics' THEN amount ELSE 0 END) AS elec_sales,
    -- 2. 의류 컬럼 만들기
    SUM(CASE WHEN category = 'Clothing' THEN amount ELSE 0 END) AS cloth_sales
FROM monthly_sales
GROUP BY month
ORDER BY month;