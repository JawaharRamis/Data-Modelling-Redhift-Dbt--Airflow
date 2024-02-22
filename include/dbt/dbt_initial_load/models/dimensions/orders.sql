{{ config(
    materialized="table",
    unique_key="Order ID"
)}}

with orders as (
    select DISTINCT "Order ID","Record Date","Order Date", "Ship Date", "Ship Mode"
    from {{ ref('stage') }}
)

select * from orders