-- {{ config(materialized='incremental', unique_key='order_id') }}  -- incremental에서는 unique_key이런게 가능하나 incremental 자체가 유료임.
{{ config(materialized = 'table')}}

SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.ship_country,
    c.company_name,
    c.country AS customer_country
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id