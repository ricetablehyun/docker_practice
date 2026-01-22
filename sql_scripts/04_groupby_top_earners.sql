-- 직원 테이블 생성
CREATE TABLE employee (
    employee_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    months INT,  -- 근무 개월 수
    salary INT   -- 월급
);

-- 데이터 심기
INSERT INTO employee (name, months, salary) VALUES
('Rose', 15, 2000),  -- 30,000
('Angelo', 12, 2000), -- 24,000
('Frank', 10, 3000),  -- 30,000 (Rose랑 동률!)
('Patrick', 10, 1000), -- 10,000
('Lisa', 15, 3000);   -- 45,000 (1등)

-- 문제 1: 직원 별로 총수입(월급 * 근무개)을 계산해서 가장 돈을 많이 번 사람의 수입과 그사람이 몇명인지 출력하라
select * from employee;

-- 풀이과정 : 1. 직원별로 이기 때문에 employee_id를 그룹으로 만든다. 
-- -> 직원별 id가 겹치지 않으니 번 돈으로 그룹화 하고 이떄 max의 이름을 나타내고 수를 세면 안되나? (months * salary) as earn;
-- 2. months * salary AS earn이라고 만들고 가장 큰 earn과 이떄의 수를 count한다.
select employee_id, MAX(SUM(months*salary)) as 가장많이번사람 ,COUNT(MAX(SUM(months*salary))) as 가장많이번사람
from employee
group by employee_id;

-- 틀린코드 : 관습적으로 id를 기준으로 하려 했으나 내가 뽑고 싶은게 뭔지에 따라 그룹이 정해지고 사고해야하는것임
-- 풀이과정 : 총수입이 큰거랑 그때 수를 세는거니까 그룹을 돈으로 하면됌. 
-- 수정코드
select MAx(earn) as 총수입왕,COUNT(earn) as 총수입왕수 
from employee
group by (months * salary) as earn;

-- 또 틀림 : 1. group by 에서는 as를 못쓴다. 아니면 쨰로 쓰던 2. 이미 그룹으로 묶인거에서 max해봣자 자신만 나온다.
-- 수정코드 
select (months * salary) as 총수입왕,COUNT(*)as 총수입왕수 
from employee
group by (months * salary)git git
order by 총수입왕 desc
limit 1;