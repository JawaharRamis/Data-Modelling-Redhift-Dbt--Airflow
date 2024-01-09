{{ config(
    materialized="table",
) }}

WITH sales AS (
    SELECT "Row ID", "Order ID", "Customer ID", "Product ID", "Sales", "Quantity", "Discount", "Profit"
    FROM {{ ref('stage') }}
)

SELECT *
FROM sales s
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('customers') }} c
    WHERE s."Customer ID" = c."Customer ID"
)
AND EXISTS (
    SELECT 1
    FROM {{ ref('products') }} p
    WHERE s."Product ID" = p."Product ID"
)
AND EXISTS (
    SELECT 1
    FROM {{ ref('orders') }} o
    WHERE s."Order ID" = o."Order ID"
)