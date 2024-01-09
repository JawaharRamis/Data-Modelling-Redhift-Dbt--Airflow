{{ config(
    materialized="table"
) }}

with orders as (

    select "Order ID", "Order Date", "Ship Date", "Ship Mode" from {{ ref('stage') }}
)

select * from orders