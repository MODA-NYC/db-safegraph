CREATE EXTERNAL TABLE IF NOT EXISTS safegraph.home_panel_summary (
    `date_range_start` string,
    `date_range_end` string,
    `state` string,
    `census_block_group` string,
    `number_devices_residing` int
)
PARTITIONED BY (dt DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://recovery-data-partnership/home_panel_summary/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  "skip.header.line.count"="1"
);