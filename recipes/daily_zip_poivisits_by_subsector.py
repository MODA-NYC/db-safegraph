from _helper import aws
from _helper.quarters import PastQs, get_quarter
from _helper.poi import poi_latest_date
import sys

"""
DESCRIPTION:
   This script parses point-of-interest visit counts from the safegraph monthly patterns
   data to create a table containing the number of visits to POIs in a given NAICS subsector
   per day. The data is aggregated to the zipcode level.

INPUTS:
    safegraph.weekly_patterns (
        safegraph_place_id text, 
        poi_cbg text,
        postal_code text,
        date_range_start date,
        date_range_end date,
        visits_by_day json
    )

    safegraph.core_poi (
        safegraph_place_id text, 
        naics_code varchar(6), 
        top_category text, 
        sub_category text,
        region varchar(2)
    )
    
OUTPUTS:
    outputs/daily_zip_poivisits_by_subsector (
        borough text, 
        borocode int,
        zipcode varchar(5),
        fips_county varchar(5),
        subsector varchar(3),
        total_visits int
    )
"""

query = """
WITH daily_visits AS(
SELECT safegraph_place_id, poi_cbg, postal_code, date_add('day', row_number() over(partition by safegraph_place_id), date_start) AS date_current, CAST(visits AS SMALLINT) as visits
FROM (
  SELECT
     safegraph_place_id,
     poi_cbg,
     postal_code,
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
   (CASE WHEN SUBSTR(a.poi_cbg,1,5) = '36005' THEN 'BX'
        WHEN SUBSTR(a.poi_cbg,1,5) = '36047' THEN 'BK'
        WHEN SUBSTR(a.poi_cbg,1,5) = '36061' THEN 'MN'
        WHEN SUBSTR(a.poi_cbg,1,5) = '36081' THEN 'QN'
        WHEN SUBSTR(a.poi_cbg,1,5) = '36085' THEN 'SI'
   END) as borough,
   (CASE WHEN SUBSTR(a.poi_cbg,1,5) = '36005' THEN 2
        WHEN SUBSTR(a.poi_cbg,1,5) = '36047' THEN 3
        WHEN SUBSTR(a.poi_cbg,1,5) = '36061' THEN 1
        WHEN SUBSTR(a.poi_cbg,1,5) = '36081' THEN 4
        WHEN SUBSTR(a.poi_cbg,1,5) = '36085' THEN 5
   END) as borocode,
   postal_code as zipcode,
   SUBSTR(a.poi_cbg,1,5) as fips_county,
   SUBSTR(b.naics_code,1,3) as subsector,
   SUM(a.visits) as total_visits
FROM daily_visits a
LEFT JOIN (
      SELECT distinct safegraph_place_id, naics_code, top_category, sub_category
      FROM "safegraph"."core_poi"
      WHERE region = 'NY' AND dt = CAST('{2}' AS DATE)
    ) b  
    ON a.safegraph_place_id=b.safegraph_place_id
GROUP BY a.date_current,
         SUBSTR(a.poi_cbg,1,5),
         a.postal_code,
         SUBSTR(b.naics_code,1,3)
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
        output=f"output/poi/daily_zip_poivisits_by_subsector/daily_zip_poivisits_by_subsector_{year_qrtr}.csv.zip"
    )
