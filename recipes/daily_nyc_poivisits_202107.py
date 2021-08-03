from _helper import aws
# from _helper.quarters import PastQs, get_quarter
# from _helper.poi import poi_latest_date
import sys

"""
DESCRIPTION:
   This script parses point-of-interest visit counts from the safegraph monthly patterns
   data to create a table containing the number of visits to individual POIs
   per day. It only includes POIs within NYC.

INPUTS:
    safegraph.weekly_patterns (
        placekey text,
        location_name text, 
        poi_cbg text,
        date_range_start date,
        date_range_end date,
        visits_by_day json
    )

    safegraph.core_poi (
        placekey text, 
        street_address text, 
        latitude numeric, 
        longitude numeric,
        naics_code varchar(6)
    )
    
OUTPUTS:
    outputs/daily_nyc_poivisits (
        date text,
        poi text,
        address text,
        poi_cbg text,
        naics_code varchar(6),
        visits int,
        latitude numeric,
        longitude numeric
    )
"""

query = """
WITH daily_visits AS(
SELECT placekey, 
        location_name, 
        poi_cbg, 
        date_add('day', (row_number() over(partition by placekey, date_start)) - 1, date_start) AS date_current, 
        CAST(visits AS INT) as visits
FROM (
  SELECT
     placekey,
     location_name,
     poi_cbg,
     CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
     CAST(SUBSTR(date_range_end, 1, 10) AS DATE) as date_end,
     cast(json_parse(visits_by_day) as array<varchar>) as a
  FROM safegraph.weekly_patterns_202107
  WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')
) b
CROSS JOIN UNNEST(a) as t(visits)
)

SELECT
   a.date_current as date,
   a.location_name as poi,
   b.street_address as address,
   a.poi_cbg,
   b.naics_code,
   a.visits as visits,
   b.latitude,
   b.longitude
FROM daily_visits a
LEFT JOIN (
      SELECT distinct 
          placekey, 
          naics_code, 
          street_address, 
          latitude, 
          longitude
      FROM "safegraph"."core_poi_new"
      WHERE region = 'NY' 
    ) b  
    ON a.placekey=b.placekey
ORDER BY date, poi_cbg
"""

aws.execute_query(
        query=query.format(), 
        database="safegraph", 
        output=f"output/dev/poi/daily_nyc_poivisits_202107/daily_nyc_poivisits.csv.zip"
    )