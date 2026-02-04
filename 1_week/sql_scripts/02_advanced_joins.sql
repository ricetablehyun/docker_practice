-- 1. 기존 테이블 삭제 (깨끗하게)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

-- 2. 유저 테이블 (추천인 컬럼 포함)
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    recommended_by INT -- 멘토의 ID (Self Join을 위한 열쇠)
);

-- 3. 주문 테이블
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT, 
    product_name VARCHAR(100)
);

-- 4. 데이터 심기
-- (1) 유저: 상현(대장), 철수(상현 추천), 영희(철수 추천), 민수(독고다이)
INSERT INTO users (user_id, name, recommended_by) VALUES
(1, '상현', NULL),
(2, '철수', 1),
(3, '영희', 2),
(4, '민수', NULL);

-- (2) 주문: 상현(1번), 철수(2번), 그리고... 99번(유령)
INSERT INTO orders (user_id, product_name) VALUES
(1, '맥북 프로'),
(2, '아이폰 15'),
(99, '의문의 택배'); -- 주인 없는 주문

-- FULL OUTER JOIN
-- 문제 : FULL OUT JOIN을 사용해서, 짝이 있든 없든 모든 명단을 뽑아라.
-- users테이블의 이름 name과 orders 테이블의 상품명이 다 나와한다.
select u.name, o.product_name
from users u 
full outer join orders o on u.user_id = o.user_id; 
-- 합집합이라 조건이 필요없을 줄 알았는데 on을하고 합치는거라 필요하다.
-- on조건을 안쓰는거는 Cross join이다. 

-- CROSS JOIN : N * M이 나옴. 
-- WITH: 잠깐 쓰고 버리는 임시 테이블 정의
WITH colors AS (
    SELECT 'Red' AS color 
    UNION 
    SELECT 'Blue'
),
sizes AS (
    SELECT 'S' AS size 
    UNION 
    SELECT 'M' 
    UNION 
    SELECT 'L'
)
select * from colors 
cross join sizes
order by size DESC;

-- SELF JOIN : 하나의 테이블의 이름을 달리함으로써 두개로 쓰는거임. 
-- 문제: 제자이름과 스승 이름을 나란히 출력하라. (self join)
select * from users;

select m.name as 선임, s.name as 후임
from users s
join users m on s.recommended_by = m.user_id;

-- UNION / UNION ALL 
-- 1. UNION (중복 제거)
-- 예상: 상현, 철수 -> 총 2명만 나와야 함
SELECT name FROM users WHERE user_id IN (1, 2)
UNION
SELECT name FROM users WHERE user_id IN (1, 2);

-- 2. UNION ALL (중복 허용)
-- 예상: 상현, 철수, 상현, 철수 -> 총 4명이 나와야 함
SELECT name FROM users WHERE user_id IN (1, 2)
UNION ALL
SELECT name FROM users WHERE user_id IN (1, 2);

