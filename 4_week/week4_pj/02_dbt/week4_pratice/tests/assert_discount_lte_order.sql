-- tests/assert_refund_lte_order.sql
{{ config(severity='warn') }}

SELECT order_sk, total_amount, refund_amount
FROM {{ ref('fct_earnings') }}
WHERE refund_amount > total_amount