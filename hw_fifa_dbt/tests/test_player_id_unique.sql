SELECT
    player_id,
    count(*) AS total_amount
FROM
    (
    SELECT *
    FROM {{ ref('int_players_male') }}
        UNION ALL
    SELECT *
    FROM {{ ref('int_players_female') }}
    )
GROUP BY 1
HAVING count(*) > 1