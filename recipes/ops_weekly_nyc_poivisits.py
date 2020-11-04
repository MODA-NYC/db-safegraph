from _helper import aws
from _helper.quarters import PastQs, get_quarter
from _helper.poi import poi_latest_date, geo_latest_date
import sys

query1 = """
WITH 
daily_visits AS(
    SELECT 
        safegraph_place_id, 
        location_name, poi_cbg, visits_total, visitors_total, 
        max_visits_per_day, min_visits_per_day, 
        max_visits_per_hour, min_visits_per_hour,
        date_add('day', row_number() over(partition by safegraph_place_id), date_start) AS date_current, 
        CAST(visits AS SMALLINT) as visits, median_dwell
    FROM (
    SELECT
        safegraph_place_id,
        location_name,
        poi_cbg,
        CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
        CAST(SUBSTR(date_range_end, 1, 10) AS DATE) as date_end,
        raw_visitor_counts as visitors_total,
        raw_visit_counts as visits_total,
        array_max(cast(json_parse(visits_by_day) as array<SMALLINT>)) as max_visits_per_day,
        array_min(cast(json_parse(visits_by_day) as array<SMALLINT>)) as min_visits_per_day,
        array_max(cast(json_parse(visits_by_each_hour) as array<SMALLINT>)) as max_visits_per_hour,
        array_min(cast(json_parse(visits_by_each_hour) as array<SMALLINT>)) as min_visits_per_hour,
        cast(json_parse(visits_by_day) as array<SMALLINT>) as a,
        median_dwell
    FROM safegraph.weekly_patterns
    WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')
    AND CAST('{0}' AS DATE) < dt
    AND CAST('{1}' AS DATE) > dt
    ) b
    CROSS JOIN UNNEST(a) as t(visits)
)
SELECT
    a.safegraph_place_id,
    CAST(EXTRACT(year_of_week from a.date_current) AS VARCHAR)||'-'||
        LPAD(CAST(EXTRACT(week from a.date_current) AS VARCHAR),2,'0') as year_week,
    a.location_name as poi,
    a.poi_cbg,
    SUM(CASE WHEN EXTRACT(dow from a.date_current) NOT IN (6, 7) THEN visits END) as visits_weekday,
    SUM(CASE WHEN EXTRACT(dow from a.date_current) IN (6, 7) THEN visits END) as visits_weekend,
    SUM(a.visits) as visits_all_days,
    a.visits_total,
    a.visitors_total,
    a.max_visits_per_day,
    a.min_visits_per_day,
    a.max_visits_per_hour,
    a.min_visits_per_hour,
    a.median_dwell
FROM daily_visits a
GROUP BY 
    a.safegraph_place_id, EXTRACT(year_of_week from a.date_current), 
    EXTRACT(week from a.date_current), a.location_name, a.poi_cbg, a.max_visits_per_day,
    a.min_visits_per_day, a.max_visits_per_hour, a.min_visits_per_hour, a.visits_total, a.visitors_total,
    a.median_dwell
ORDER BY year_week, poi_cbg
"""

query2 = """
WITH dataset AS(
    SELECT 
        date_start, safegraph_place_id, location_name, 
        poi_cbg, map_keys(a) as origins, a
    FROM (
        SELECT
            safegraph_place_id,
            location_name,
            poi_cbg,
            CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
            CAST(SUBSTR(date_range_end, 1, 10) AS DATE) as date_end,
            raw_visit_counts as total_weekly_visits,
            cast(json_parse(visitor_home_cbgs) as map<varchar, varchar>) as a
        FROM safegraph.weekly_patterns
        WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')
        AND CAST('{0}' AS DATE) < dt
        AND CAST('{1}' AS DATE) > dt
    ) b
)
SELECT 
    CAST(EXTRACT(year_of_week from date_start) AS VARCHAR)||'-'||
        LPAD(CAST(EXTRACT(week from date_start) AS VARCHAR),2,'0') as year_week,
    a.location_name as poi,
    a.poi_cbg,
    visitor_home_cbg,
    CAST(a[visitor_home_cbg] AS SMALLINT) as visits
FROM dataset a
CROSS JOIN unnest(origins) t(visitor_home_cbg)
"""


query3 = """
SELECT
    a.safegraph_place_id, a.naics_code, 
    a.top_category, a.sub_category,
    a.latitude, a.longitude,
    a.area_square_feet, a.includes_parking_lot,
    a.is_synthetic, b.zonename, b.zonecolor
FROM (
    SELECT 
        a.safegraph_place_id, a.naics_code, 
        a.top_category, a.sub_category,
        a.latitude, a.longitude, 
        a.street_address as address,
        b.area_square_feet, b.includes_parking_lot, b.is_synthetic
    FROM "safegraph"."core_poi" a
    LEFT JOIN "safegraph"."geo_supplement" b
    ON a.safegraph_place_id = b.safegraph_place_id
    WHERE a.region = 'NY'
        AND a.dt = CAST('{0}' AS DATE)
        AND b.dt = CAST('{1}' AS DATE)
) a
LEFT JOIN "safegraph"."zones" b
ON ST_WITHIN(ST_POINT(a.longitude, a.latitude), ST_POLYGON(b.wkt))
"""

# Load the current quarter
quarters = get_quarter()

# quarters = PastQs
tablename1 = 'weekly_nyc_poivisits'
tablename2 = 'weekly_nyc_poivisits_by_visitor_home_cbg'
tablename3 = 'poi_info'

for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end) 

    # weekly_nyc_poivisits
    aws.execute_query(
        query=query1.format(start, end), 
        database="safegraph", 
        output=f"output/ops/{tablename1}/{tablename1}_{year_qrtr}.csv.zip"
    )
    # weekly_nyc_poivisits_by_visitor_home_cbg
    aws.execute_query(
        query=query2.format(start, end), 
        database="safegraph", 
        output=f"output/ops/{tablename2}/{tablename2}_{year_qrtr}.csv.zip"
    )
    
# poi_info
aws.execute_query(
        query=query3.format(poi_latest_date, geo_latest_date),
        database="safegraph",
        output=f"output/ops/{tablename3}/{tablename3}.csv.zip"
    )