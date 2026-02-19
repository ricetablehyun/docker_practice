SELECT
    order_id,
    customer_id,
    order_date,
    {{ safe_value('ship_city', 'Unknown') }}    AS ship_city,
    {{ safe_value('ship_country', 'Unknown') }} AS ship_country,
    {{ get_region('ship_country') }}            AS region
FROM {{ ref('stg_orders') }}