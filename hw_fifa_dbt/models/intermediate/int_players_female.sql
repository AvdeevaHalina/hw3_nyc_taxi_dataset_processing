{{ config(
materialized='table',
tags=['intermediate', 'female', 'players']
) }}

WITH
stg_players_female AS (
    SELECT *
    FROM {{ ref('stg_players_female') }}
)

SELECT
    *,
    'female' AS gender
FROM stg_players_female