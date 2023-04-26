SELECT
    cast(date as date) as date,
    cast(year as integer) as year,
    cast(month as integer) as month,
    cast(extract(hour from (end_date - start_date)) + extract(minute from (end_date - start_date))/60 as float64) as duration,
    cast(start_date as timestamp) as start_time,
    cast(end_date as timestamp) as end_time,
    extract(hour from start_date) as start_hour,
    extract(hour from end_date) as end_hour,
    cast(start_station as string) as start_station,
    cast(end_station as string) as end_station,
    cast(upper(member_type) as string) as member_casual,
    cast(station_long_lat_start.longitude as float64) as start_lng,  
    cast(station_long_lat_start.latitude as float64) as start_lat,
    cast(station_long_lat_end.longitude as float64) as end_lng,  
    cast(station_long_lat_end.latitude as float64) as end_lat,
    concat(cast(station_long_lat_start.latitude as string), ',' ,cast(station_long_lat_start.longitude as string)) as start_loc,
    concat(cast(station_long_lat_end.latitude as string), ',' ,cast(station_long_lat_end.longitude as string)) as end_loc
FROM 
    {{ source('bike_data_dev', 'old_bike_data') }} as old_bike_data 
LEFT JOIN 
    {{ ref('station_long_lat') }} as station_long_lat_start 
on trim(old_bike_data.start_station) = trim(station_long_lat_start.station)
LEFT JOIN
    {{ ref('station_long_lat') }} as station_long_lat_end
on trim(old_bike_data.end_station) = trim(station_long_lat_end.station)