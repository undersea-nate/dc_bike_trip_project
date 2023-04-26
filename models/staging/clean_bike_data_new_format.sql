SELECT
    cast(date as date) as date,
    cast(year as integer) as year,
    cast(month as integer) as month,
    cast(extract(hour from (ended_at - started_at)) + extract(minute from (ended_at - started_at))/60 as float64) as duration,
    cast(started_at as timestamp) as start_time,
    cast(ended_at as timestamp) as end_time,  
    extract(hour from started_at) as start_hour,
    extract(hour from ended_at) as end_hour, 
    cast(start_station_name as string) as start_station,
    cast(end_station_name as string) as end_station,
    cast(upper(member_casual) as string) as member_casual,
    cast(start_lng as float64) as start_lng,
    cast(start_lat as float64) as start_lat,
    cast(end_lng as float64) as end_lng,
    cast(end_lat as float64) as end_lat,
    concat(cast(start_lat as string), ',' ,cast(start_lng as string)) as start_loc,
    concat(cast(end_lat as string), ',' ,cast(end_lng as string)) as end_loc
FROM 
    {{ source('bike_data_dev', 'new_bike_data') }} as new_bike_data 