{{ config(materialized='table') }}  
-- "결과를 테이블로 저장해!" (형식 지정)
-- 크고 실시간필요없음. table로 저장 (캐싱)

SELECT
    customer_id AS customer_id, 
    company_name AS company_name,
    city AS city,
    country AS country
FROM {{ source('raw', 'customers') }}