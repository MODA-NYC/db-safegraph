CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.social_distancing (
  `origin_census_block_group` STRING,
  `date_range_start` STRING,
  `date_range_end` STRING,
  `device_count` STRING,
  `distance_traveled_from_home` STRING,
  `bucketed_distance_traveled` STRING,
  `median_dwell_at_bucketed_distance_traveled` STRING,
  `completely_home_device_count` STRING,
  `median_home_dwell_time` STRING,
  `bucketed_home_dwell_time` STRING,
  `at_home_by_each_hour` STRING,
  `part_time_work_behavior_devices` STRING,
  `full_time_work_behavior_devices` STRING,
  `destination_cbgs` STRING,
  `delivery_behavior_devices` STRING,
  `median_non_home_dwell_time` STRING,
  `candidate_device_count` STRING,
  `bucketed_away_from_home_time` STRING,
  `median_percentage_time_home` STRING,
  `bucketed_percentage_time_home` STRING
  )
PARTITIONED BY (dt DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/social_distancing/v2'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'compressionType'='gzip',
  "skip.header.line.count"="1"
)