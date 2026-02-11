{{ config(materialized='incremental', unique_key='order_id') }}  -- incremental 미리

SELECT
    o.order_id,
    o.order_date,
    o.ship_country,
    c.company_name,
    c.country AS customer_country
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id