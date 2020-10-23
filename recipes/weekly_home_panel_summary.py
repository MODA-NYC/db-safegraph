from _helper import aws
import sys

query1 = """
SELECT 
    SUBSTR(date_range_start, 1, 10) as date,
    CAST(EXTRACT(year from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR)||'-'||
        LPAD(CAST(EXTRACT(week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR),2,'0') as year_week,
    SUM(number_devices_residing) as number_devices_residing
FROM home_panel_summary
WHERE SUBSTR(census_block_group, 1, 5) IN ('36085','36081','36061','36047','36005') 
    AND dt >= CAST('2019-01-01' as DATE)
GROUP BY SUBSTR(date_range_start, 1, 10)
ORDER BY SUBSTR(date_range_start, 1, 10)
"""
tablename1 = 'weekly_nyc_summary'
aws.execute_query(
    query=query1, 
    database="safegraph", 
    output=f"output/home_panel_summary/{tablename1}/{tablename1}.csv.zip"
)

query2 = """
SELECT 
    SUBSTR(date_range_start, 1, 10) as date,
    CAST(EXTRACT(year from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR)||'-'||
        LPAD(CAST(EXTRACT(week from CAST(SUBSTR(date_range_start, 1, 10) AS DATE)) AS VARCHAR),2,'0') as year_week,
    SUBSTR(census_block_group, 1, 5) as fips_county,
    (CASE WHEN SUBSTR(census_block_group, 1, 5) = '36005' THEN 'BX'
        WHEN SUBSTR(census_block_group, 1, 5) = '36047' THEN 'BK'
        WHEN SUBSTR(census_block_group, 1, 5) = '36061' THEN 'MN'
        WHEN SUBSTR(census_block_group, 1, 5) = '36081' THEN 'QN'
        WHEN SUBSTR(census_block_group, 1, 5) = '36085' THEN 'SI'
    END) as borough,
    (CASE WHEN SUBSTR(census_block_group, 1, 5) = '36005' THEN 2
            WHEN SUBSTR(census_block_group, 1, 5) = '36047' THEN 3
            WHEN SUBSTR(census_block_group, 1, 5) = '36061' THEN 1
            WHEN SUBSTR(census_block_group, 1, 5) = '36081' THEN 4
            WHEN SUBSTR(census_block_group, 1, 5) = '36085' THEN 5
    END) as borocode,
    SUM(number_devices_residing) as number_devices_residing
FROM home_panel_summary
WHERE SUBSTR(census_block_group, 1, 5) IN ('36085','36081','36061','36047','36005')
    AND dt >= CAST('2019-01-01' as DATE)
GROUP BY SUBSTR(date_range_start, 1, 10), SUBSTR(census_block_group, 1, 5)
ORDER BY SUBSTR(date_range_start, 1, 10), SUBSTR(census_block_group, 1, 5)
"""
tablename2 = 'weekly_nyc_borough_summary'
aws.execute_query(
    query=query2,
    database="safegraph", 
    output=f"output/home_panel_summary/{tablename2}/{tablename2}.csv.zip"
)