SELECT
    year
FROM {{ source('dbt_underseanate', 'all_bike_data') }}
having year < 2019