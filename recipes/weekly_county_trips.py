from _helper import aws
from _helper.quarters import PastQs, get_quarter
import sys

"""
DESCRIPTION:
   This script parses social distancing trip data to create weekly counts of
   trips between counties for all counties in NY, NJ, PA, CT, RI, MA, VT, and NH

INPUTS:
    safegraph.social_distancing (
        origin_census_block_group text,
        destination_cbgs json, 
        date_range_start date
    )

OUTPUTS:
    outputs/weekly_county_trips (
        year_week text,
        fips_county_origin varchar(5),
        fips_county_destination varchar(5),
        weekday_trips int,
        weekend_trips int,
        all_trips int
    )
"""

query ="""
WITH dataset AS (
SELECT date_start, origin_census_block_group, map_keys(a) as cbg, a from (
    SELECT origin_census_block_group,
            CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
            CAST(json_parse(destination_cbgs) AS  map<varchar, varchar>) as a 
    FROM social_distancing
    WHERE SUBSTR(origin_census_block_group, 1, 2) IN ('36', '34', '09', '42', '25', '44', '50', '33')
    AND CAST('{0}' AS DATE) < dt
    AND CAST('{1}' AS DATE) > dt
    ) b
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

# Load the current quarter
quarters = get_quarter()

# quarters = PastQs

for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end)
    aws.execute_query(
        query=query.format(start, end), 
        database="safegraph", 
        output=f"output/social_distancing/weekly_county_trips/weekly_county_trips_{year_qrtr}.csv.zip"
    )

# Add/update device count table for counties in 8-state region
query ="""
    SELECT 
        CAST(EXTRACT(year from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR)||'-'||LPAD(CAST(EXTRACT(week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR),2,'0') as year_week,
        SUBSTR(origin_census_block_group, 1, 5) as origin_fips,
        SUM(CAST(device_count AS INTEGER)) as device_count,
        SUM(CAST(completely_home_device_count AS INTEGER)) as completely_home_device_count
    FROM social_distancing
    WHERE SUBSTR(origin_census_block_group, 1, 2) IN ('36', '34', '09', '42', '25', '44', '50', '33')
    GROUP BY date_range_start, SUBSTR(origin_census_block_group, 1, 5)
"""
aws.execute_query(
        query=query, 
        database="safegraph", 
        output="output/social_distancing/device_counts_by_county.csv.zip"
    )