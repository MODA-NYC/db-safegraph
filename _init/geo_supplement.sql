CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.geo_supplement (
    `safegraph_place_id` string,
    `location_name` string,
    `polygon_class` string,
    `is_synthetic` string,
    `includes_parking_lot` string,
    `iso_country_code` string,
    `area_square_feet` int
)
PARTITIONED BY (dt DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/geo_supplement/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  "skip.header.line.count"="1"
);