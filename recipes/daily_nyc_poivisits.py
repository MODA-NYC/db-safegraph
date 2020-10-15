from _helper import aws
import sys
from math import ceil
import datetime

"""
DESCRIPTION:
   This script parses point-of-interest visit counts from the safegraph monthly patterns
   data to create a table containing the number of visits to individual POIs
   per day. It only includes POIs within NYC.

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
SELECT safegraph_place_id, location_name, poi_cbg, date_add('day', row_number() over(), date_start) AS date_current, CAST(visits AS SMALLINT) as visits
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
      SELECT distinct safegraph_place_id, naics_code, street_address, latitude, longitude
      FROM "safegraph"."core_poi"
      WHERE region = 'NY'
    ) b  
    ON a.safegraph_place_id=b.safegraph_place_id
"""
'''
# Load the current quarter
today = datetime.date.today()
year_qrtr = str(today.year) + 'Q' + str(ceil(today.month/3.))
start = datetime.date(today.year, 3*ceil(today.month/3.) - 2, 1)
quarters = {year_qrtr:(str(start), str(today))}

'''
quarters = {'2019Q1':('2019-01-01', '2019-03-31'),
            '2019Q2':('2019-04-01', '2019-06-30'), 
            '2019Q3':('2019-07-01', '2019-09-30'),
            '2019Q4':('2019-10-01', '2019-12-31'),
            '2020Q1':('2020-01-01', '2020-03-31'),
            '2020Q2':('2020-04-01', '2020-06-30'), 
            '2020Q3':('2020-07-01', '2020-09-30')}

for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end) 
    aws.execute_query(
        query=query.format(start, end), 
        database="safegraph", 
        output=f"output/poi/daily_nyc_poivisits/daily_nyc_poivisits_{year_qrtr}.csv"
    )