{{ config(
    materialized="table"
) }}

with products as (

    select "Product ID", "Category", "Sub-Category", "Product Name" from {{ ref('stage') }}

)

select * from products