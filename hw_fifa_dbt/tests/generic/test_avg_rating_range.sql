{% test test_avg_rating_range(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} >= 40
AND {{ column_name }} <= 100

{% endtest %}