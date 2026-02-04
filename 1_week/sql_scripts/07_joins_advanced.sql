CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    name VARCHAR(50),
    dept_id INT,
    manager_id INT -- 상사의 emp_id (Self Join용)
);

CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50)
);

INSERT INTO departments VALUES (101, 'Engineering'), (102, 'Sales');

INSERT INTO employees VALUES 
(1, 'Alice', 101, NULL),  -- 사장님 (상사 없음)
(2, 'Bob', 101, 1),       -- Alice의 부하
(3, 'Charlie', 102, 1),   -- Alice의 부하
(4, 'David', NULL, 2),    -- 부서 미정, Bob의 부하
(5, 'Eve', 103, 2);       -- 존재하지 않는 부서(103) 소속

-- 문제 1: Self Join]
--"모든 직원의 이름과 그 직원의 직속 상사 이름을 출력하시오." 
-- (단, 사장님(Alice)처럼 상사가 없는 사람도 출력되어야 함. 상사 이름은 NULL로 나오게.)

select * from departments;
select * from employees;

-- 풀이 과정 	: 셀프참 조건이 manger_id랑 emp_id일 경우고 이름뜨고 하면 될거같음. 
select emp.name as 직원이름,COALESCE((manager.name),"NULL") 
from employees emp
join employees manager on emp.emp_id = manager.manager_id

-- 수정된 풀이과정 
select e.name AS 직원이름 ,coalesce(m.name,'Null') AS 상사직원
from employees e
left join employees m on e.manager_id = m.emp_id;

--[문제 2: Left vs Inner]
--"모든 **부서 이름(dept_name)**과 그 부서에 속한 직원 수를 구하시오." (조건: 직원이 0명인 부서도 '0'으로 나와야 함. 부서 없는 직원은 무시.)
select * from departments;
select * from employees;

-- 풀이과정 :employees를 기준으로 left join on d.dept_id = e.dept_id한다. 
-- 이후 case when, gruup by 단순하게 그룹별 인원 세기니까 group by사용

select coalesce(d.dept_name,'0') as 부서명, COUNT(*) as 부서별인원수 --error 부분: '0'임 데이터값이니까.
from employees e
left join departments d on e.dept_id = d.dept_id
where e.dept_id is not null 
group by d.dept_name -- error 부분: d에 name 존재
order by 부서명 desc;

-- 문제에서 원하는건 지워지는게 아니라 그냥 뜨지않는거임.그니까 문제에서 원하는건 존재하지만 뜨지않는거였네. 
-- 그렇기에 이거의 기준을 departments d 로 잡고 left join한거고 이떄 d.@로 그룹화를 한다면 당연히 존재하는 두 그룹만 뜰꺼고 이걸 원한거였음 ㅇㅇ

SELECT 
    d.dept_name AS 부서명,
    COUNT(*) AS 부서별인원수
FROM departments d                 -- 1. 기준: 부서 (모든 부서 다 나와!)
LEFT JOIN employees e              -- 2. 도우미: 직원 (없으면 NULL로 와)
    ON d.dept_id = e.dept_id
GROUP BY d.dept_name               -- 3. ★필수★ 부서 이름별로 묶어라!
ORDER BY d.dept_name;
