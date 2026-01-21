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
