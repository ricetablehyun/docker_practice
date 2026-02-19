{{ config(materialized='view') }}

SELECT
    order_id,      -- 큰따옴표 제거
    customer_id,   -- 큰따옴표 제거
    CAST(order_date AS DATE) AS order_date,
    ship_city,
    ship_country
FROM {{ source('raw', 'orders') }}
WHERE order_date IS NOT NULL