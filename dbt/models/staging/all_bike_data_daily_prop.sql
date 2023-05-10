create table  dbt_underseanate.all_bike_data_daily_prop as
SELECT date, start_hour, sum(prop_duration) as prop_duration FROM
(SELECT a.duration/b.sum_duration as prop_duration, a.date, a.start_hour, a.member_casual from `stately-transit-383518.dbt_underseanate.all_bike_data` as a,
(SELECT date, sum(duration) as sum_duration FROM `stately-transit-383518.dbt_underseanate.all_bike_data` group by date) as b
WHERE a.date = b.date)
group by date, start_hour
order by date, start_hour
