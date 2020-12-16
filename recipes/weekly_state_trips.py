from _helper import aws
from _helper.quarters import PastQs, get_quarter
import sys

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
    outputs/weekly_state_trips (
        year_week text,
        origin varchar(3),
        destination varchar(3),
        weekday_trips int,
        weekend_trips int,
        all_trips int
    )
"""

query = """
WITH dataset AS (
    SELECT 
        date_start, 
        origin_census_block_group, 
        map_keys(a) as cbg, a 
    FROM (
        SELECT origin_census_block_group,
            CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
            CAST(json_parse(destination_cbgs) AS  map<varchar, varchar>) as a 
        FROM social_distancing
        WHERE CAST('{0}' AS DATE) < dt
        AND CAST('{1}' AS DATE) > dt
    ) b
 ),
draft as (
    SELECT 
        CAST(EXTRACT(year_of_week from date_start) AS VARCHAR)||'-'||
            LPAD(CAST(EXTRACT(week from date_start) AS VARCHAR),2,'0') as year_week,
        (CASE 
            WHEN SUBSTR(origin_census_block_group, 1, 5) 
                IN ('36085','36081','36061','36047','36005') THEN 'NYC'
            ELSE SUBSTR(origin_census_block_group, 1, 2)
        END) as origin,
        (CASE 
            WHEN SUBSTR(desti_cbgs, 1, 5) 
                IN ('36085','36081','36061','36047','36005') THEN 'NYC'
            ELSE SUBSTR(desti_cbgs, 1, 2)
        END) as destination,
        (CASE 
            WHEN EXTRACT(dow from date_start) NOT IN (6, 7) 
            THEN CAST(a[desti_cbgs] as SMALLINT) 
        END) as weekday_trips,
        (CASE 
            WHEN EXTRACT(dow from date_start) IN (6, 7) 
            THEN CAST(a[desti_cbgs] as SMALLINT) 
        END) as weekend_trips,
        CAST(a[desti_cbgs] as SMALLINT) as all_trips
    FROM dataset
    CROSS JOIN unnest(cbg) t(desti_cbgs)
    WHERE SUBSTR(desti_cbgs, 1, 5) IN ('36085','36081','36061','36047','36005')
        OR SUBSTR(origin_census_block_group, 1, 5) IN ('36085','36081','36061','36047','36005')
)
SELECT 
    year_week, origin, destination,
    SUM(weekday_trips) as weekday_trips,
    SUM(weekend_trips) as weekend_trips,
    SUM(all_trips) as all_trips
FROM draft
GROUP BY year_week, origin, destination
ORDER BY year_week, origin, destination
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
        output=f"output/production/social_distancing/weekly_state_trips/weekly_state_trips_{year_qrtr}.csv.zip"
    )

# Add/update device count table for states and NYC
query ="""
    SELECT 
        SUBSTR(date_range_start, 1, 10) as date,
        CAST(EXTRACT(year_of_week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR)||'-'||
            LPAD(CAST(EXTRACT(week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR),2,'0') as year_week,
        SUBSTR(origin_census_block_group, 1, 2) as state,
        SUM(CAST(device_count AS INTEGER)) as device_count,
        SUM(CAST(completely_home_device_count AS INTEGER)) as completely_home_device_count
    FROM social_distancing
    WHERE SUBSTR(origin_census_block_group, 1, 5) NOT IN ('36085','36081','36061','36047','36005')
    GROUP BY date_range_start, SUBSTR(origin_census_block_group, 1, 2)
    UNION
    SELECT 
        SUBSTR(date_range_start, 1, 10) as date,
        CAST(EXTRACT(year_of_week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR)||'-'||
            LPAD(CAST(EXTRACT(week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR),2,'0') as year_week,
        'NYC' as state,
        SUM(CAST(device_count AS INTEGER)) as device_count,
        SUM(CAST(completely_home_device_count AS INTEGER)) as completely_home_device_count
    FROM social_distancing
    WHERE SUBSTR(origin_census_block_group, 1, 5) IN ('36085','36081','36061','36047','36005')
    GROUP BY date_range_start
"""

aws.execute_query(
        query=query, 
        database="safegraph", 
        output="output/production/social_distancing/device_counts_by_state.csv.zip"
    )
