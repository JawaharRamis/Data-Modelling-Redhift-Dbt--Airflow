{{ config(
    materialized='incremental',
    ) 
}}

with orders as (
    select distinct "Order ID", "Record Date", "Order Date", "Ship Date", "Ship Mode" from {{ ref('stage') }}
)

select * from orders
{% if is_incremental() %}
WHERE "Record Date" >= (SELECT MAX("Record Date") FROM {{ this }})
{% endif %}
