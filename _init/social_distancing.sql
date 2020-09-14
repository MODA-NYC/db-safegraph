CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.social_distancing (
  `origin_census_block_group` STRING,
  `date_range_start` DATE,
  `date_range_end` DATE,
  `device_count` SMALLINT,
  `distance_traveled_from_home` SMALLINT,
  `bucketed_distance_traveled` STRING,
  `median_dwell_at_bucketed_distance_traveled` STRING,
  `completely_home_device_count` SMALLINT,
  `median_home_dwell_time` SMALLINT,
  `bucketed_home_dwell_time` STRING,
  `at_home_by_each_hour` ARRAY<SMALLINT>,
  `part_time_work_behavior_devices` SMALLINT,
  `full_time_work_behavior_devices` SMALLINT,
  `destination_cbgs` STRING,
  `delivery_behavior_devices` SMALLINT,
  `median_non_home_dwell_time` SMALLINT,
  `candidate_device_count` SMALLINT,
  `bucketed_away_from_home_time` STRING,
  `median_percentage_time_home` SMALLINT,
  `bucketed_percentage_time_home` STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"',
  'escapeChar'= '"'
) LOCATION 's3://recovery-data-partnership/social_distancing/v2'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'compressionType'='gzip',
  "skip.header.line.count"="1"
)