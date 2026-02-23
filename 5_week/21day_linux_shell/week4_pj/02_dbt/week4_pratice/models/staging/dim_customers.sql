-- models/marts/dim_customers.sql
-- 요구사항: "고객이 속한 나라별 분포를 보고 싶다"
-- 같은 고객이 여러 주문 → 최신 주문 기준 정보 사용

{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        customer_id,
        email,
        phone,
        country,
        region,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS rn
    FROM {{ ref('stg_dirty') }}
),

customer_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(total_amount) AS total_spent
    FROM {{ ref('stg_dirty') }}
    GROUP BY customer_id
)

SELECT
    r.customer_id,
    r.email,
    r.phone,
    r.country,
    r.region,
    s.order_count,
    s.total_spent
FROM ranked r
JOIN customer_stats s ON r.customer_id = s.customer_id
WHERE r.rn = 1