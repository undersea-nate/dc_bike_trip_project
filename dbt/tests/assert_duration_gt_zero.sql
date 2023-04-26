SELECT
    duration
FROM {{ source('dbt_underseanate', 'all_bike_data') }}
having duration <= 0