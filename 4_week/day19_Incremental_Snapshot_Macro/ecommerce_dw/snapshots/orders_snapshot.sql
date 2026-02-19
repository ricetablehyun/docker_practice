-- {{}} jinja 내에 주석시 인식오류가능 
-- [1. 제어문 {% ... %}] 스냅샷 블록 시작
-- dbt에게 명령: "야, 지금부터 'orders_snapshot'이라는 이름의 스냅샷 로직을 시작한다."
-- 일반적인 테이블(Model)이 아니라, 역사 기록용 스냅샷임을 선언하는 겁니다.

-- [2. 출력문 {{ ... }}] 설정(Config) 주입
-- dbt에게 명령: "이 스냅샷의 설정값은 다음과 같아. 이대로 세팅해."

        -- [저장소] 스냅샷 테이블은 'dbt_dev' 말고 'snapshots'라는 데이터셋(폴더)에 따로 저장해라.
        -- [고유키] 이 컬럼(order_id)이 같으면 '같은 주문'으로 간주하고 추적해라.
        -- [감지 전략] 변경을 감지하는 방법은 'check(값 비교)', 'timestamp'(시간비교) 방식을 쓸 거야.
        -- [감시 대상] 만약 'ship_city'나 'ship_country' 컬럼의 값이 바뀌면?
        -- 즉시 '변경된 데이터'로 간주하고 사진(Snapshot)을 찍어라!

-- [3. 일반 SQL] 원본 데이터 지정
-- "내가 감시할 대상(CCTV 화면)은 바로 'stg_orders' 테이블이야."
-- {{ ref(...) }}는 dbt가 알아서 `dbt-project.dataset.stg_orders` 같은 진짜 이름으로 바꿔줍니다.


-- [4. 제어문 {% ... %}] 스냅샷 블록 종료
-- dbt에게 명령: "스냅샷 설정 끝! 이제 이거 바탕으로 마법을 부려봐."
{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='check',
        check_cols=['ship_city', 'ship_country']
    )
}}

SELECT * FROM {{ ref('stg_orders') }}

{% endsnapshot %}