from _helper import aws
from _helper.quarters import PastQs, get_quarter
from _helper.poi import poi_latest_date
import sys

query = """
WITH 
daily_visits AS(
    SELECT 
        safegraph_place_id, 
        location_name, poi_cbg, 
        date_add('day', row_number() over(partition by safegraph_place_id), date_start) AS date_current, 
        CAST(visits AS SMALLINT) as visits
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
), 
weekly_visits as (
    SELECT
        a.safegraph_place_id,
        CAST(EXTRACT(year from a.date_current) AS VARCHAR)||'-'||
            LPAD(CAST(EXTRACT(week from a.date_current) AS VARCHAR),2,'0') as year_week,
        a.location_name as poi,
        a.poi_cbg,
        SUM(CASE WHEN EXTRACT(dow from a.date_current) NOT IN (0, 6) THEN visits END) as visits_weekday,
        SUM(CASE WHEN EXTRACT(dow from a.date_current) IN (0, 6) THEN visits END) as visits_weekend,
        SUM(a.visits) as visits_total
    FROM daily_visits a
    GROUP BY 
        a.safegraph_place_id, EXTRACT(year from a.date_current), 
        EXTRACT(week from a.date_current), a.location_name, a.poi_cbg
    ORDER BY year_week, poi_cbg
),
poi as (
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
            b.area_square_feet, b.includes_parking_lot, b.is_synthetic
        FROM "safegraph"."core_poi" a
        LEFT JOIN "safegraph"."geo_supplement" b
        ON a.safegraph_place_id = b.safegraph_place_id
        WHERE a.region = 'NY'
            AND a.dt = CAST('{2}' AS DATE)
            AND b.dt = CAST('2020-08-17' AS DATE)
    ) a
    LEFT JOIN "safegraph"."zones" b
    ON ST_WITHIN(ST_POINT(a.longitude, a.latitude), ST_POLYGON(b.wkt))
)
SELECT 
    a.safegraph_place_id, 
    b.year_week,
    b.poi,
    b.poi_cbg,
    b.visits_weekday,
    b.visits_weekend,
    b.visits_total,
    a.naics_code, 
    a.top_category, 
    a.sub_category,
    a.latitude, 
    a.longitude,
    a.area_square_feet, 
    a.includes_parking_lot,
    a.is_synthetic, 
    a.zonename, 
    a.zonecolor
FROM weekly_visits b
LEFT JOIN poi a
ON a.safegraph_place_id = b.safegraph_place_id;
"""

# Load the current quarter
quarters = get_quarter()

# quarters = PastQs
tablename = 'ops_weekly_nyc_poivisits'
for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end) 
    aws.execute_query(
        query=query.format(start, end, poi_latest_date), 
        database="safegraph", 
        output=f"output/ops/{tablename}/{tablename}_{year_qrtr}.csv.zip"
    )