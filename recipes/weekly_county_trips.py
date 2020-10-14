from _helper import aws
import sys
from math import ceil
import datetime

'''
# Load historical quarters, beginning 2019-01-01
quarters = {'2019Q1':('2019-01-01', '2019-03-31'),
            '2019Q2':('2019-04-01', '2019-06-30'), 
            '2019Q3':('2019-07-01', '2019-09-30'),
            '2019Q4':('2019-10-01', '2019-12-31'),
            '2020Q1':('2020-01-01', '2020-03-31'),
            '2020Q2':('2020-04-01', '2020-06-30'), 
            '2020Q3':('2020-07-01', '2020-09-30')}
'''

# Load the current quarter
today = datetime.date.today()
year_qrtr = str(today.year) + 'Q' + str(ceil(today.month/3.))
start = datetime.date(today.year, 3*ceil(today.month/3.) - 2, 1)
quarters = {year_qrtr:(str(start), str(today))}

for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end)
    query = f"""
    WITH dataset AS (
    SELECT date_start, origin_census_block_group, map_keys(a) as cbg, a from (
        SELECT origin_census_block_group,
                CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
                CAST(json_parse(destination_cbgs) AS  map<varchar, varchar>) as a 
        FROM social_distancing
        WHERE SUBSTR(origin_census_block_group, 1, 2) IN ('36', '34', '09', '42', '25', '44', '50', '33')
        AND CAST('{start}' AS DATE) < dt
        AND CAST('{end}' AS DATE) > dt
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

    aws.execute_query(
        query=query, 
        database="safegraph", 
        output=f"output/social_distancing/weekly_county_trips_{year_qrtr}.csv"
    )