from _helper import aws
from _helper.quarters import PastQs, get_quarter
import sys

# inflow: origin (US counties) -> destination (NYC zipcodes)
query ="""
WITH dataset AS (
SELECT date_start, origin_census_block_group, map_keys(a) as cbg, a from (
    SELECT 
        origin_census_block_group,
        CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
        CAST(json_parse(destination_cbgs) AS  map<varchar, varchar>) as a 
    FROM social_distancing
    WHERE dt > CAST('2020-01-01' as DATE)
    ) b
)
SELECT 
    date, fips_county_origin, zipcode_destination, SUM(trips) as trips
FROM(
    SELECT 
        a.date, a.fips_county_origin, b.zcta as zipcode_destination, a.trips
    FROM (
        SELECT 
            date_start as date,
            SUBSTR(origin_census_block_group, 1, 5) as fips_county_origin,
            SUBSTR(desti_cbgs, 1, 11) as tract_destination,
            desti_cbgs,
            CAST(a[desti_cbgs] as SMALLINT) as trips
        FROM dataset
        CROSS JOIN unnest(cbg) t(desti_cbgs)
    ) a 
    LEFT JOIN zcta_tract b
    ON a.tract_destination = b.geoid
    WHERE SUBSTR(a.desti_cbgs, 1, 5) IN ('36085','36081','36061','36047','36005')
) a
GROUP BY date, fips_county_origin, zipcode_destination
ORDER BY date, fips_county_origin, zipcode_destination
"""

aws.execute_query(
    query=query, 
    database="safegraph", 
    output=f"output/omb/daily_zipcode_county_inflow/daily_zipcode_county_inflow_2020.csv.zip"
)

devices_query = """
    SELECT 
        SUBSTR(date_range_start, 1, 10) as date,
        SUBSTR(origin_census_block_group, 1, 5) as fips_county_origin,
        SUM(CAST(device_count AS INTEGER)) as device_count,
        SUM(CAST(candidate_device_count AS INTEGER)) as candidate_device_count,
        SUM(CAST(completely_home_device_count AS INTEGER)) as completely_home_device_count
    FROM social_distancing a
    GROUP BY date_range_start, SUBSTR(origin_census_block_group, 1, 5)
"""
aws.execute_query(
    query=devices_query, 
    database="safegraph", 
    output=f"output/omb/daily_county_devices/daily_county_devices.csv.zip"
)
