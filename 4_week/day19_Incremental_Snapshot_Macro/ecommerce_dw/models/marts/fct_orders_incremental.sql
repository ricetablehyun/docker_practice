{{ config(materialized = 'incremental', unique_key='order_id')}}
-- 증분 모델에는 반드시 unique_key 설정을 함께 해주세요. 혹시라도 실행 도중 멈춰서 데이터가 겹치게 로드되더라도, unique_key가 있으면 dbt가 알아서 중복을 제거(Merge/Update)해줍니다.

SELECT
    order_id,
    customer_id,
    order_date,
    ship_city,
    ship_country
FROM {{ ref('stg_orders') }}

{% if is_incremental() %} -- 증분일때만 작동하도록 걸어두는거임. 함수가 "지금 첫 실행이야, 아니면 업데이트야?"를 확인 
  WHERE order_date > (SELECT MAX(order_date) FROM {{ this }}) -- 업데이트면 해당테이블에서 가장 최근 날짜를 가져와서 이후 날짜 (>)만 조회하도록 하는거임. 
{% endif %}
