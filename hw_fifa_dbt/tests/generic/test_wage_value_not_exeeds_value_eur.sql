{% test test_wage_value_not_exeeds_value_eur(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} > value_eur
AND {{ column_name }} IS NOT NULL
AND value_eur IS NOT NULL

{% endtest %}