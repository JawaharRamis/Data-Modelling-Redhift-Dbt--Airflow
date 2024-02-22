{{ config(
    materialized="table"
)}}

WITH 
all_customers_details AS (
  SELECT DISTINCT "Order Date","Customer ID", "Customer Name", "City", "Postal Code", "Region" 
  FROM {{ ref('stage') }}
),
last_customer_details AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY "Customer ID" ORDER BY "Order Date" DESC) as rn
  FROM all_customers_details
),
latest_customers_details AS (
  SELECT DISTINCT "Customer ID", "Customer Name", "City", "Postal Code", "Region"
  FROM last_customer_details
  WHERE rn=1
)
SELECT * FROM latest_customers_details