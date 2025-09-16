{{ config(
materialized='table',
tags=['mart', 'male', 'profile']
) }}

WITH player_roles AS (
    SELECT *,
        CASE
            WHEN defending > 75 AND pace > 70 AND dribbling > 65 THEN 'Wingback'
            WHEN shooting > 80 AND attacking_finishing > 80 THEN 'Poacher'
            WHEN passing > 80 AND mentality_vision > 80 AND skill_ball_control > 80 THEN 'Regista'
            WHEN defending > 80 AND physic > 75 THEN 'Center Back'
            WHEN dribbling > 80 AND pace > 80 AND shooting > 75 THEN 'Winger'
            WHEN passing > 75 AND mentality_vision > 75 AND shooting > 75 THEN 'Playmaker'
            ELSE 'All-Round Player'
        END AS player_role,
        CASE
            WHEN age < 21 THEN 'U21'
            WHEN age BETWEEN 21 AND 28 THEN 'Prime'
            ELSE 'Veteran'
        END AS age_group
    FROM {{ ref('int_players_male') }}
)

SELECT
    player_id,
    short_name,
    long_name,
    age,
    age_group,
    club_name,
    league_name,
    nationality_name,
    overall,
    potential,
    value_eur,
    player_role,
    preferred_foot,
    pace,
    shooting,
    passing,
    dribbling,
    defending,
    physic,
    gender
FROM player_roles