### DBT

## Models

# Clean_bike_data_old_format.sql

This model is used to prepare the bigquery dataset based on the old file formats for the final dataset. Many of the variable names were renamed in this model to standardize names with the new file formats. In addition, this model uses a seed (which is a static file primarily used for data mapping) to add latitude and longitude data. Finally, this model also creates location variables from latitude and longitude, which can be used to create geo fields in Looker Studio later on. 

# Clean_bike_data_new_format.sql

This model is used to prepare the bigquery dataset based on the new file formats for the final dataset. This file is simplier compared to the old format because longitude and latitude were already included when these files were downloaded off the web.

# All_bike_data.sql

This model simply combines the previous two models to create a final dataset that will be used in Looker Studio.

This model also specifies that duration has to be greater than 0. Several observations had negative durations, which means that the end time was earlier than the start time. These observations were likely filed in error, and therefore were removed at this stage.

## Tests

There are also three tests which are used to determine if certain conditions are met in the final dataset. 

1. assert_duration_gt_zero.sql - this test simply determines if all observations had durations greater than 0.
3. assert_yr_lt_2019.sql - this test determines if all observations were collected during or after 2019, which is the start date of this project. 

## Seeds

There is one seed which is used to map station names in the clean_bike_data_old_format table with latitude and longtitude coordinates. 
