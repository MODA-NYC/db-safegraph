from _helper import aws

"""
DESCRIPTION:
   This script parses social distancing trip data to create weekly counts of
   trips between NYC counties and all 50 states.

INPUTS:
    safegraph.social_distancing (
        origin_census_block_group text,
        destination_cbgs json, 
        date_range_start date
    )

OUTPUTS:
    outputs/weekly_county_trips (
        year_week text,
        state varchar(2),
        to_nyc int,
        from_nyc int,
        net_nyc int
    )
"""

query = """
WITH dataset AS (
  SELECT date_start, origin_census_block_group, map_keys(a) as cbg, a from (
     SELECT origin_census_block_group,
            CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
            CAST(json_parse(destination_cbgs) AS  map<varchar, varchar>) as a 
     FROM social_distancing) b
 ),
 
 pairs AS (
 SELECT 
     CAST(EXTRACT(year from date_start) AS VARCHAR)||'W'||LPAD(CAST(EXTRACT(week from date_start) AS VARCHAR),2,'0') as year_week,
     (CASE WHEN SUBSTR(origin_census_block_group, 1, 5) IN ('36085','36081','36061','36047','36005')
           THEN 'NYC'
      ELSE SUBSTR(origin_census_block_group, 1, 2)
      END) as origin,
     (CASE WHEN SUBSTR(desti_cbgs, 1, 5) IN ('36085','36081','36061','36047','36005')
           THEN 'NYC'
      ELSE SUBSTR(desti_cbgs, 1, 2)
      END) as destination,
      CAST(a[desti_cbgs] as SMALLINT) as trips
 FROM dataset
 CROSS JOIN unnest(cbg) t(desti_cbgs)
 WHERE SUBSTR(desti_cbgs, 1, 5) IN ('36085','36081','36061','36047','36005')
    OR SUBSTR(origin_census_block_group, 1, 5) IN ('36085','36081','36061','36047','36005')
 ),
          
 state_pairs AS (
 SELECT year_week, origin, destination, SUM(trips) as trips
 FROM pairs
 GROUP BY year_week, origin, destination)

SELECT
   a.year_week,
   a.origin as state,
   a.trips as to_nyc,
   b.trips as from_nyc,
   a.trips - b.trips as net_nyc
   FROM state_pairs a
   JOIN state_pairs b
   ON a.origin=b.destination
   WHERE a.origin <> 'NYC'
"""

aws.execute_query(
    query=query, 
    database="safegraph", 
    output="output/social_distancing/weekly_state_trips.csv"
)