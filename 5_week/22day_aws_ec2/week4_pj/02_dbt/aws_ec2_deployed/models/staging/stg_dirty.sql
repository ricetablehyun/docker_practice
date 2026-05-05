{{ config(materialized='view') }}

SELECT
    -- Surrogate Key
    FARM_FINGERPRINT(
        CONCAT(
            COALESCE(CAST(order_id AS STRING), 'NULL'), '_',
            COALESCE(CAST(customer_id AS STRING), 'NULL'), '_',
            COALESCE(CAST(order_date AS STRING), 'NULL') 
        )
    ) AS order_sk,

    CAST(order_id AS INT64) AS original_order_id,
    CAST(customer_id AS INT64) AS customer_id,
    email,
    phone,

    CASE
        WHEN LOWER(TRIM(country)) IN ('korea', 'kr', 'south korea', '대한민국') THEN 'South Korea'
        WHEN LOWER(TRIM(country)) IN ('usa', 'us', 'united states') THEN 'USA'
        WHEN LOWER(TRIM(country)) IN ('japan', 'jp') THEN 'Japan'
        WHEN LOWER(TRIM(country)) IN ('china', 'cn') THEN 'China'
        ELSE INITCAP(TRIM(country))
    END AS country,

    -- [핵심] 중복 제거 및 매핑 통합
    CASE 
        WHEN LOWER(TRIM(status)) = 'complete' THEN 'delivered'
        WHEN LOWER(TRIM(status)) = 'refunded' THEN 'returned'
        ELSE LOWER(TRIM(status))
    END AS status,

    total_amount,
    discount_amount,
    refund_amount,
    CAST(order_date AS DATE) AS order_date,
    CAST(ship_date AS DATE) AS ship_date,
    {{ get_region('country') }} AS region

FROM {{ source('external_source', 'dirty_orders') }}

WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND total_amount >= 0
  AND CAST(order_date AS DATE) BETWEEN '2024-01-01' AND '2025-12-31'