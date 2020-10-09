from _helper import aws

query = """
WITH dataset AS (
  SELECT date_start, origin_census_block_group, map_keys(a) as cbg, a from (
     SELECT origin_census_block_group,
            CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
            CAST(json_parse(destination_cbgs) AS  map<varchar, varchar>) as a 
     FROM social_distancing
     WHERE SUBSTR(origin_census_block_group, 1, 2) IN ('36', '34', '09', '42', '25', '44', '50', '33')
     LIMIT 30) b
 ) 
 SELECT 
     CAST(EXTRACT(year from date_start) AS VARCHAR)||'-'||LPAD(CAST(EXTRACT(week from date_start) AS VARCHAR),2,'0') as year_week,
     SUBSTR(origin_census_block_group, 1, 5) as fips_county_origin,
     SUBSTR(desti_cbgs, 1, 5) as fips_county_destination,
     SUM(CASE WHEN EXTRACT(dow from date_start) NOT IN (0, 6) THEN CAST(a[desti_cbgs] as SMALLINT) END) as weekday_trips,
     SUM(CASE WHEN EXTRACT(dow from date_start) IN (0, 6) THEN CAST(a[desti_cbgs] as SMALLINT) END) as weekend_trips,
     SUM(CAST(a[desti_cbgs] as SMALLINT)) as all_trips
 FROM dataset
 CROSS JOIN unnest(cbg) t(desti_cbgs)
 WHERE SUBSTR(desti_cbgs, 1, 2) IN ('36', '34', '09', '42', '25', '44', '50', '33')
 GROUP BY EXTRACT(year from date_start),
          EXTRACT(week from date_start),
          SUBSTR(origin_census_block_group, 1, 5),
          SUBSTR(desti_cbgs, 1, 5)
"""

aws.execute_query(
    query=query, 
    database="safegraph", 
    output="output/social_distancing/weekly_county_trips.csv"
)