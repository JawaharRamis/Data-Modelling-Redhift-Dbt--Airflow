{{ config(
    materialized="ephemeral",
) }}

SELECT * FROM stage.staging
