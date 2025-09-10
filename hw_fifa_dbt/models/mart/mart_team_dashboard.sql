{{ config(
materialized='table',
tags=['mart', 'dashboard']
) }}

WITH teams AS (
    SELECT club_id, club_name, 'male' as gender
    FROM {{ ref('int_team_profiles_male') }}
    UNION ALL
    SELECT club_id, club_name, 'female' as gender
    FROM {{ ref('int_team_profiles_female') }}
),

players AS (
    SELECT * FROM {{ ref('int_players_male') }}
    UNION ALL
    SELECT * FROM {{ ref('int_players_female') }}
),

team_stats AS (
    SELECT
        t.club_id,
        t.club_name,
        t.gender,
        COUNT(p.player_id) as total_players,
        AVG(p.overall) as avg_team_rating,
        SUM(p.value_eur) as total_team_value_eur,
        AVG(p.age) as avg_age,
        -- Age groups
        COUNT(CASE WHEN p.age < 21 THEN 1 END) AS u21_players,
        COUNT(CASE WHEN p.age BETWEEN 21 AND 28 THEN 1 END) AS prime_age_players,
        COUNT(CASE WHEN p.age > 28 THEN 1 END) AS veteran_players,
        -- Top 3 players
        CONCAT_WS(', ', COLLECT_LIST(
            CASE WHEN player_rank <= 3 THEN p.short_name END
        )) AS top_3_players,
        AVG(CASE WHEN player_rank <= 3 THEN p.overall END) AS top_3_avg_rating,
        SUM(CASE WHEN player_rank <= 3 THEN p.value_eur END) AS top_3_total_value,
        -- Overperforming players
        COUNT(CASE WHEN is_overperforming THEN 1 END) AS num_overperforming
    FROM teams t
    LEFT JOIN (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY club_team_id, gender ORDER BY overall DESC) as player_rank,
            overall > AVG(overall) OVER (PARTITION BY gender) + STDDEV(overall) OVER (PARTITION BY gender) as is_overperforming
        FROM players
    ) p ON t.club_id = p.club_team_id AND t.gender = p.gender
    GROUP BY t.club_id, t.club_name, t.gender
)

SELECT
    club_id,
    club_name,
    gender,
    total_players,
    avg_team_rating,
    total_team_value_eur,
    avg_age,
    u21_players,
    prime_age_players,
    veteran_players,
    top_3_players,
    top_3_avg_rating,
    top_3_total_value,
    num_overperforming,
    -- Calculated percentages
    (top_3_total_value / NULLIF(total_team_value_eur, 0)) * 100 AS top_3_value_percentage,
    (num_overperforming / NULLIF(total_players, 0)) * 100 AS overperforming_percentage
FROM team_stats
WHERE club_id IS NOT NULL
ORDER BY gender, club_id