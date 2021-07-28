CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.neighborhood_home_panel_summary_202107 (
  `year` int,
  `month` int,
  `region` string,
  `iso_country_code` string,
  `census_block_group` string,
  `number_devices_residing` int,
  `number_devices_primary_daytime` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/neighborhood_home_panel_summary_202107/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  "skip.header.line.count"="1"
);