{{ config(
    materialized='incremental',
    ) 
}}

WITH sales AS (
    SELECT "Order ID", "Record Date", "Customer ID", "Product ID", "Sales", "Quantity", "Discount", "Profit"
    FROM {{ ref('stage') }}
)

select * from sales
{% if is_incremental() %}
WHERE "Record Date" >= (SELECT MAX("Record Date") FROM {{ this }})
{% endif %}
