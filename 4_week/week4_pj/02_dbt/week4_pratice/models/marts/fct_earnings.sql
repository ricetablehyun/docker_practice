-- models/marts/fct_earnings.sql
-- 요구사항: "주문별 순이익을 보고 싶다"

{{ config(
    materialized='table',
    unique_key='order_sk'
) }}

SELECT
    order_sk,
    original_order_id,
    customer_id,
    order_date,
    status,
    region,
    total_amount,
    discount_amount,
    refund_amount,
    (total_amount - discount_amount - refund_amount) AS net_profit

FROM {{ ref('stg_dirty') }}

{% if is_incremental() %}
  WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}