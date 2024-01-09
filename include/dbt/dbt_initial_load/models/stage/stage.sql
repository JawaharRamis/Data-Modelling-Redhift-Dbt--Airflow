{{ config(
    materialized="table",
) }}

SELECT * FROM stage.stage
