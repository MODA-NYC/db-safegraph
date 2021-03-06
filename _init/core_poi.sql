CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.core_poi_latest
(
  `placekey` string,
  `parent_placekey` string,
  `location_name` string,
  `safegraph_brand_ids` string,
  `brands` string,
  `top_category` string,
  `sub_category` string,
  `naics_code` string,
  `latitude` DOUBLE,
  `longitude` DOUBLE,
  `street_address` string,
  `city` string,
  `region` string,
  `postal_code` string,
  `iso_country_code` string,
  `phone_number` string,
  `open_hours` string,
  `category_tags` string,
  `opened_on` string,
  `closed_on` string,
  `tracking_closed_since` string,
  `geometry_type` string,
  `year_month` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/core_poi_latest/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'compressionType'='gzip',
  "skip.header.line.count"="1"
)
