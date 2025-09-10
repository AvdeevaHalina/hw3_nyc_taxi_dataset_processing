{{ config(
    materialized='table',
    tags=['stage', 'coaches', 'female']
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ ref('female_coaches') }}
),

cleaned_data AS (
    SELECT
        CAST(coach_id AS BIGINT) AS coach_id,
        coach_url AS coach_url,
        short_name AS short_name,
        long_name AS long_name,
        CAST(dob AS DATE) AS date_of_birth,
        CAST(nationality_id AS INT) AS nationality_id,
        nationality_name AS nationality_name,
        face_url AS face_url
    FROM raw_data
    WHERE
        coach_id IS NOT NULL
        AND coach_url IS NOT NULL
        AND short_name IS NOT NULL
        AND dob > '1900-01-01'
)

SELECT *
FROM cleaned_data;