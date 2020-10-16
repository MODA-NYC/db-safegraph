from _helper import aws
from _helper.quarters import PastQs, get_quarter
from _helper.poi import poi_latest_date
import sys

"""
DESCRIPTION:
   This script parses point-of-interest visit counts from the safegraph monthly patterns
   data to create a table containing the number of visits to individual POIs
   per week. It only includes POIs within NYC.

INPUTS:
    safegraph.weekly_patterns (
        safegraph_place_id text,
        location_name text, 
        poi_cbg text,
        date_range_start date,
        date_range_end date,
        visits_by_day json
    )

    safegraph.core_poi (
        safegraph_place_id text, 
        street_address text, 
        latitude numeric, 
        longitude numeric,
        naics_code varchar(6), 
        top_category text, 
        sub_category text,
        region varchar(2)
    )
    
OUTPUTS:
    outputs/weekly_nyc_poivisits (
        year_week text,
        poi text,
        address text,
        poi_cbg text,
        naics_code varchar(6),
        visits_weekday int,
        visits_weekend int,
        visits_daily int,
        latitude numeric,
        longitude numeric
    )
"""

query = """
WITH daily_visits AS(
SELECT safegraph_place_id, location_name, poi_cbg, date_add('day', row_number() over(partition by safegraph_place_id), date_start) AS date_current, CAST(visits AS SMALLINT) as visits
FROM (
  SELECT
     safegraph_place_id,
     location_name,
     poi_cbg,
     CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
     CAST(SUBSTR(date_range_end, 1, 10) AS DATE) as date_end,
     cast(json_parse(visits_by_day) as array<varchar>) as a
  FROM safegraph.weekly_patterns
  WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')
  AND CAST('{0}' AS DATE) < dt
  AND CAST('{1}' AS DATE) > dt
) b
CROSS JOIN UNNEST(a) as t(visits)
)

SELECT
   CAST(EXTRACT(year from a.date_current) AS VARCHAR)||'-'||LPAD(CAST(EXTRACT(week from a.date_current) AS VARCHAR),2,'0') as year_week,
   a.location_name as poi,
   b.street_address as address,
   a.poi_cbg,
   b.naics_code,
   SUM(CASE WHEN EXTRACT(dow from a.date_current) NOT IN (0, 6) THEN visits END) as visits_weekday,
   SUM(CASE WHEN EXTRACT(dow from a.date_current) IN (0, 6) THEN visits END) as visits_weekend,
   SUM(a.visits) as visits_daily,
   b.latitude,
   b.longitude
FROM daily_visits a
LEFT JOIN (
      SELECT distinct safegraph_place_id, naics_code, street_address, latitude, longitude
      FROM "safegraph"."core_poi"
      WHERE region = 'NY' AND dt = CAST('{2}' AS DATE)
    ) b  
    ON a.safegraph_place_id=b.safegraph_place_id
GROUP BY EXTRACT(year from a.date_current), 
         EXTRACT(week from a.date_current), 
         a.location_name, 
         b.naics_code,
         a.poi_cbg,
         b.street_address,
         b.latitude,
         b.longitude
"""

# Load the current quarter
quarters = get_quarter()

# quarters = PastQs

for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end) 
    aws.execute_query(
        query=query.format(start, end, poi_latest_date), 
        database="safegraph", 
        output=f"output/poi/weekly_nyc_poivisits/weekly_nyc_poivisits_{year_qrtr}.csv.zip"
    )