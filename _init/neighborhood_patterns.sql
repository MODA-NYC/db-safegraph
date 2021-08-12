CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.neighborhood_patterns_202107 (
`area` string,
`area_type` string,
`origin_area_type` string,
`date_range_start` string,
`date_range_end` string,
`day_counts` string,
`raw_stop_counts` int,
`raw_device_counts` int,
`stops_by_day` string,
`stops_by_each_hour` string,
`device_home_areas` string,
`weekday_device_home_areas` string,
`weekend_device_home_areas` string,
`breakfast_device_home_areas` string,
`lunch_device_home_areas` string,
`afternoon_tea_device_home_areas` string,
`dinner_device_home_areas` string,
`nightlife_device_home_areas` string,
`work_hours_device_home_areas` string,
`work_behavior_device_home_areas` string,
`device_daytime_areas` string,
`distance_from_home` string,
`distance_from_primary_daytime_location` int,
`median_dwell` double,
`top_same_day_brand` string,
`top_same_month_brand` string,
`popularity_by_each_hour` string,
`popularity_by_hour_monday` string,
`popularity_by_hour_tuesday` string,
`popularity_by_hour_wednesday` string,
`popularity_by_hour_thursday` string,
`popularity_by_hour_friday` string,
`popularity_by_hour_saturday` string,
`popularity_by_hour_sunday` string,
`device_type` string,
`iso_country_code` string,
`region` string,
`y` int,
`m` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/neighborhood_patterns_202107/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  "skip.header.line.count"="1"
)