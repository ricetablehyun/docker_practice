{{ config(materialized='view') }}  -- view로 (실시간-ish)

SELECT
    "OrderID" AS order_id,
    "CustomerID" AS customer_id,
    "OrderDate" AS order_date,
    "ShipCity" AS ship_city,
    "ShipCountry" AS ship_country
FROM {{ source('raw', 'orders') }}
WHERE "OrderDate" IS NOT NULL