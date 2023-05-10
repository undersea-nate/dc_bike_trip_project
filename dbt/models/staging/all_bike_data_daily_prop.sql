SELECT 
  date, start_hour, sum(prop_duration) as prop_duration 
FROM
  (SELECT 
    a.duration/b.sum_duration as prop_duration, a.date, a.start_hour, a.member_casual 
  FROM
    {{ source('dbt_underseanate', 'all_bike_data') }} 
  as a,
  (SELECT 
    date, sum(duration) as sum_duration 
   FROM 
    {{ source('dbt_underseanate', 'all_bike_data') }} 
   group by date) as b
WHERE a.date = b.date)
GROUP BY date, start_hour
ORDER BY date, start_hour
