{{ config(
    materialized="table"
)}}

with customers as (

    select "Customer ID", "Customer Name", "Sub-Category", "Segment", "City", "Postal Code", "Region" from {{ ref('stage') }}
)

select * from customers