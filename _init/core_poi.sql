CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.core_poi (
  `safegraph_place_id` string,
  `parent_safegraph_place_id` string,
  `location_name` string,
  `safegraph_brand_ids` string,
  `brands` string,
  `top_category` string,
  `sub_category` string,
  `naics_code` string,
  `latitude` double,
  `longitude` double,
  `street_address` string,
  `city` string,
  `region` string,
  `postal_code` tinyint,
  `iso_country_code` string,
  `phone_number` string,
  `open_hours` string,
  `category_tags` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"',
  'escapeChar'= '"'
) LOCATION 's3://recovery-data-partnership/core_poi/poi/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"',
  'escapeChar'= '"'
);