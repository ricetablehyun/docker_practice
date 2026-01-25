-- 테이블 생성
DROP TABLE IF EXISTS sensor_readings;
CREATE TABLE sensor_readings (
    id SERIAL PRIMARY KEY,
    machine_id INT,
    value INT
);

-- 데이터 삽입 (NULL 포함, 기계 1번은 평균 낮음, 2번은 평균 높음)
INSERT INTO sensor_readings (machine_id, value) VALUES 
(1, 10), (1, NULL), (1, 20), (1, 15), -- 1번 기계: 10, 20, 15 (평균 15)
(2, 200), (2, NULL), (2, 300), (2, 250); -- 2번 기계: 200, 300, 250 (평균 250)

/* ⚔️ 3. [Mission] CTE로 파이프라인 구축하기
이제 배운 WITH 문법을 사용해 아래 로직을 구현해라. 서브쿼리는 절대 사용 금지다.
[요구사항 3단계]
CTE 1 (clean_data): value가 NULL이 아닌 행만 추출한다.
CTE 2 (machine_stats): clean_data를 이용해 기계별(machine_id) 평균 값(avg_val로 별칭)을 구한다.
Main Query: machine_stats에서 평균 값이 100 이상인 기계의 machine_id와 avg_val을 출력한다.*/

select * from sensor_readings;
-- 따로 with문을 어려개 안만들고 ,로 해서 나눠서 사용해도 된다. 
-- CTE 1 :
with cte_data as (
select id, machine_id, value
from sensor_readings
where value is not null
),-- CTE 2 :
machine_stats as (
select machine_id,AVG(value) as avg_val
from cte_data
group by machine_id
)
 -- 메인 쿼리 
select machine_id, avg_val
from machine_stats
where avg_val > 100;

