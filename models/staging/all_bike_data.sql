(SELECT
    * 
FROM
    {{ source('dbt_underseanate', 'clean_bike_data_old_format') }}
WHERE duration > 0
    )
UNION ALL
(SELECT
    *
FROM
    {{ source('dbt_underseanate', 'clean_bike_data_new_format') }}
WHERE duration > 0
    )