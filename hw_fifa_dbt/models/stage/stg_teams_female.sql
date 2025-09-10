{{ config(
    materialized='table',
    tags=['stage', 'teams', 'female']
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ ref('female_teams') }}
),

cleaned_data AS (
    SELECT
        CAST(team_id AS BIGINT) AS team_id,
        team_url AS team_url,
        fifa_version AS fifa_version,
        fifa_update AS fifa_update,
        CAST(fifa_update_date AS DATE) AS fifa_update_date,
        team_name AS team_name,
        CAST(league_id AS BIGINT) AS league_id,
        league_name AS league_name,
        CAST(league_level AS INT) AS league_level,
        CAST(nationality_id AS BIGINT) AS nationality_id,
        nationality_name AS nationality_name,
        CAST(overall AS INT) AS overall,
        CAST(attack AS INT) AS attack,
        CAST(midfield AS INT) AS midfield,
        CAST(defence AS INT) AS defence,
        CAST(coach_id AS BIGINT) AS coach_id,
        home_stadium AS home_stadium,
        rival_team AS rival_team,
        CAST(international_prestige AS INT) AS international_prestige,
        CAST(domestic_prestige AS INT) AS domestic_prestige,
        CAST(transfer_budget_eur AS BIGINT) AS transfer_budget_eur,
        CAST(club_worth_eur AS BIGINT) AS club_worth_eur,
        CAST(starting_xi_average_age AS DECIMAL(4,2)) AS starting_xi_average_age,
        CAST(whole_team_average_age AS DECIMAL(4,2)) AS whole_team_average_age,
        captain AS captain,
        short_free_kick AS short_free_kick,
        long_free_kick AS long_free_kick,
        left_short_free_kick AS left_short_free_kick,
        right_short_free_kick AS right_short_free_kick,
        penalties AS penalties,
        left_corner AS left_corner,
        right_corner AS right_corner,
        def_style AS def_style,
        CAST(def_team_width AS INT) AS def_team_width,
        CAST(def_team_depth AS INT) AS def_team_depth,
        def_defence_pressure AS def_defence_pressure,
        def_defence_aggression AS def_defence_aggression,
        CAST(def_defence_width AS INT) AS def_defence_width,
        def_defence_defender_line AS def_defence_defender_line,
        off_style AS off_style,
        off_build_up_play AS off_build_up_play,
        off_chance_creation AS off_chance_creation,
        CAST(off_team_width AS INT) AS off_team_width,
        CAST(off_players_in_box AS INT) AS off_players_in_box,
        CAST(off_corners AS INT) AS off_corners,
        CAST(off_free_kicks AS INT) AS off_free_kicks,
        CAST(build_up_play_speed AS INT) AS build_up_play_speed,
        build_up_play_dribbling AS build_up_play_dribbling,
        build_up_play_passing AS build_up_play_passing,
        build_up_play_positioning AS build_up_play_positioning,
        chance_creation_passing AS chance_creation_passing,
        chance_creation_crossing AS chance_creation_crossing,
        chance_creation_shooting AS chance_creation_shooting,
        chance_creation_positioning AS chance_creation_positioning
    FROM raw_data
    WHERE
        team_id IS NOT NULL
        AND team_name IS NOT NULL
)

SELECT *
FROM cleaned_data;