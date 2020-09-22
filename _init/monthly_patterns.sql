CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.monthly_patterns (
  `safegraph_place_id` string,
  `location_name` string,
  `street_address` string,
  `city` string,
  `region` string,
  `postal_code` string,
  `safegraph_brand_ids` string,
  `brands` string,
  `date_range_start` string,
  `date_range_end` string,
  `raw_visit_counts` int,
  `raw_visitor_counts` int,
  `visits_by_day` string,
  `poi_cbg` string,
  `visitor_home_cbgs` string,
  `visitor_daytime_cbgs` string,
  `visitor_work_cbgs` string,
  `visitor_country_of_origin` string,
  `distance_from_home` string,
  `median_dwell` string,
  `bucketed_dwell_times`string,
  `related_same_day_brand` string,
  `related_same_month_brand` string,
  `popularity_by_hour` string,
  `popularity_by_day` string,
  `device_type` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/monthly_patterns/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  "skip.header.line.count"="1"
)