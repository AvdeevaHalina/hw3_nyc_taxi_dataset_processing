{{ config(
materialized='table',
tags=['intermediate', 'female', 'aggregate', 'league']
) }}

WITH league_averages AS (
    SELECT
        p.league_id,
        p.league_name,
        AVG(tp.avg_rating) as avg_league_rating
    FROM {{ ref('int_team_profiles_female') }} tp
    JOIN {{ ref('int_players_female') }} p ON tp.club_id = p.club_team_id
    GROUP BY p.league_id, p.league_name
),

young_talents AS (
    SELECT DISTINCT
        league_id,
        CASE
            WHEN COUNT(CASE WHEN age <= 23 AND potential > 85 THEN 1 END) > 0
            THEN TRUE
            ELSE FALSE
        END AS has_young_talents
    FROM {{ ref('int_players_female') }}
    GROUP BY league_id
)

SELECT
    la.league_id,
    la.league_name,
    la.avg_league_rating,
    yt.has_young_talents
FROM league_averages la
JOIN young_talents yt ON la.league_id = yt.league_id
WHERE la.league_id IS NOT NULL