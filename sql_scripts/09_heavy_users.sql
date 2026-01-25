-- 1. 테이블 초기화
DROP TABLE IF EXISTS places;

-- 2. 테이블 생성 (공간 정보)
CREATE TABLE places (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    host_id INT
);

-- 3. 데이터 삽입
-- host_id 760849: 2개 등록 (헤비 유저)
-- host_id 30900122: 3개 등록 (헤비 유저)
-- host_id 12345: 1개 등록 (일반 유저)
-- host_id 99999: 1개 등록 (일반 유저)
INSERT INTO places (id, name, host_id) VALUES 
(14233, '이태원 펜트하우스', 760849),
(21314, '강남 파티룸', 12345),
(23658, '홍대 게스트하우스', 30900122),
(23864, '홍대 스튜디오', 30900122),
(32112, '이태원 풀빌라', 760849),
(45667, '성수동 렌탈스튜디오', 99999),
(51234, '홍대 루프탑', 30900122);

/*
 ⚔️ 2. [Mission] 헤비 유저가 소유한 장소 (CTE Ver.)
[상황]
이 사이트에서는 공간을 2개 이상 등록한 사람을 **'헤비 유저'**라고 부른다.
헤비 유저들이 등록한 모든 공간(Place)의 정보를 리스트로 뽑아서 이메일을 보내려고 한다.
[요구사항] 다음 로직을 **CTE(WITH)**를 사용하여 구현하시오. (서브쿼리 ❌)
CTE (heavy_users): places 테이블에서 host_id별로 그룹핑하여, 등록한 공간 개수(COUNT)가 2개 이상인 host_id만 추출한다.
Main Query: 원본 테이블 places와 위 CTE heavy_users를 JOIN하여, 헤비 유저가 등록한 공간의 **모든 정보(id, name, host_id)**를 출력한다.
정렬: id 순서대로 오름차순 정렬한다.*/
select * from places;

-- 일반 cte안쓰고 문제풀이 :
-- 1. host_id를 그룹으로 묶고 이때 count(*)을 함으로써 2개 이상을 해비유저로 뽑는다.
-- 2. 이를 상관 서브쿼리로 하여금 뽑아서 전체 id,name,host의 정보를 뽑는다.

-- 1.host_id를 그룹으로 묶고 이때 count(*)을 함으로써 2개 이상을 해비유저로 뽑는다.
select p.host_id
from places p
group by p.host_id --error1. id가 다 같은게 아니였음. host_id만 같음. 
having Count(*)>=2;

-- 2.이를 상관 서브쿼리로 하여금 뽑아서 전체 id,name,host의 정보를 뽑는다.
select *
from places p1
where p1.host_id in ( -- 비상관서브쿼리임. 상관서브쿼리는 서브쿼리구문 안과 밖을 묶이는 것이다.
	select p2.host_id
	from places p2
	group by p2.host_id --error2, 그룹바이로 한거에서 어느거를 p1이랑 비교할지몰라 오류뜸 
	having Count(*)>=2
) -- error에서 인지햇든 1 : n의 구조인데 =를 써서 문제인거임. in을 쓰면 해결된다. 
order by p1.id;

-- cte 사용하여 작성 
/*[요구사항] 다음 로직을 **CTE(WITH)**를 사용하여 구현하시오. (서브쿼리 ❌)
CTE (heavy_users): places 테이블에서 host_id별로 그룹핑하여, 등록한 공간 개수(COUNT)가 2개 이상인 host_id만 추출한다.
Main Query: 원본 테이블 places와 위 CTE heavy_users를 JOIN하여, 헤비 유저가 등록한 공간의 **모든 정보(id, name, host_id)**를 출력한다.
정렬: id 순서대로 오름차순 정렬한다.*/


with heavey_users as (
	select host_id
	from places p1
	group by host_id
	having count(*)>=2
)
select *
from places p2
join heavey_users on p2.host_id = heavey_users.host_id --with 별명 을 불려와서 사용하는것임. 주의 !! 
order by p2.id asc;

