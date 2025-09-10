{{ config(
materialized='table',
tags=['intermediate', 'male', 'aggregate']
) }}

WITH aggregated_data AS (
    SELECT
        club_team_id AS club_id,
        club_name AS club_name,
        AVG(overall) AS avg_rating,
        SUM(value_eur) AS total_team_value
    FROM {{ ref('int_players_male') }}
    GROUP BY club_team_id, club_name
)

SELECT
    club_id,
    club_name,
    avg_rating,
    total_team_value
FROM aggregated_data
WHERE club_id IS NOT NULL