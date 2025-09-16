{{ config(
materialized='table',
tags=['intermediate', 'male', 'players']
) }}

WITH
stg_players_male AS (
    SELECT *
    FROM {{ ref('stg_players_male') }}
)

SELECT
    *,
    'male' AS gender
FROM stg_players_male