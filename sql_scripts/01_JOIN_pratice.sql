select * from orders;
select * from users;

select u.name, o.product_name 
from users u 
join orders o on u.user_id = o.user_id;

select u.name, o.product_name 
from users u 
right join orders o on u.user_id = o.user_id;


-- 문제 1 : right join을 이용하여 이름이 비어있는 주문의 건의 product_name을 조회하라.
-- 풀이과정 : orders를 기준으로하여금 users user_id로 right join 한다. 
-- 이떄 where를 이용하여 user_id is null로하고 Select product_id로 한다. 
-- users도 주문 안한사람이여도 user_id는있고 그외가 null일것이다.

select o.product_name
from users u
right join orders o  on o.user_id =u.user_id 
where u.user_id is null; -- 별칭주의

-- 문제 2: join을 이용하여 이름이 상현인 고객이 구매한 모든 상품명을 조회하시오
-- 풀이과정 : user_id로 join을 하여 where name='상현'인거에 대한 Select product_name하면 될듯

select o.product_name 
from users u
join orders o on u.user_id = o.user_id 
where u.name = '상현';

drop table orders;
drop table users;

-- 1. 청소하기 (순서 중요: 자식부터 지워야 함)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

-- 2. 테이블 생성
-- (1) 고객 테이블
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(50)
);

-- (2) 주문 테이블 (누가 주문했니?)
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    order_date DATE DEFAULT CURRENT_DATE
);

-- (3) 주문 상세 테이블 (뭘 샀니? -> 여기가 1:N의 핵심!)
CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT, -- 어떤 주문서에 적힌 물건이니?
    item_name VARCHAR(50),
    price INT
);

-- 3. 데이터 심기 (시나리오: 상현이가 맥북이랑 마우스를 샀음)
-- 유저 등록
INSERT INTO users (name) VALUES ('상현');

-- 주문서 발행 (1번 주문)
INSERT INTO orders (user_id) VALUES (1);

-- 주문서에 물건 적기 (1번 주문에 물건 2개!)
INSERT INTO order_items (order_id, item_name, price) VALUES
(1, '맥북 프로', 2000000),
(1, '매직 마우스', 100000);

select * from users;
select * from orders;
select * from order_items;

-- 1:N의 구조이면 데이터가 뻥튀기가 된다.? 문제다?  
-- 상세 내역으로써는'정보'가 되지만 합계를 구할떄는 중복(Noise)가 된다. 따라서 distict을 쓰기도함. 
SELECT
    u.name AS 이름,
    o.order_id AS 주문번호,
    oi.item_name AS 상품명,
    oi.price AS 가격
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id;