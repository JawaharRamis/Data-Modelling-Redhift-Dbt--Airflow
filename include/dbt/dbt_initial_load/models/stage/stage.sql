{{ config(
    materialized="ephemeral",
) }}

SELECT * FROM stage.stage
