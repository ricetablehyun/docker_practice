-- 1. 주문 테이블 생성
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    amount INT,
    order_date DATE
);

-- 2. 실습 데이터 입력 (3명의 고객)
INSERT INTO orders (customer_id, amount, order_date) VALUES
(1, 10000, '2024-01-01'),
(1, 20000, '2024-01-02'), -- 1번 고객: 총 3만원
(2, 5000, '2024-01-01'),  -- 2번 고객: 총 5천원
(3, 30000, '2024-01-03'),
(3, 10000, '2024-01-05'); -- 3번 고객: 총 4만원


--예제: 총주문 금액이 25000이상인 고객을 뽑아라
select * from orders;

select customer_id
from orders
group by customer_id
having sum(amount)>=25000
order by customer_id asc;