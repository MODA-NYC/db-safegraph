from _helper import aws

query = """
WITH daily_visits AS(
SELECT safegraph_place_id, poi_cbg, date_add('day', row_number() over(), date_start) AS date_current, CAST(visits AS SMALLINT) as visits
FROM (
  SELECT
     safegraph_place_id,
     poi_cbg,
     CAST(SUBSTR(date_range_start, 1, 10) AS DATE) as date_start,
     CAST(SUBSTR(date_range_end, 1, 10) AS DATE) as date_end,
     cast(json_parse(visits_by_day) as array<varchar>) as a
  FROM safegraph.monthly_patterns
  WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')
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
   SUBSTR(a.poi_cbg,1,5) as fips_county,
   SUBSTR(b.naics_code,1,3) as subsector,
   SUM(a.visits) as total_visits
FROM daily_visits a
LEFT JOIN (
      SELECT distinct safegraph_place_id, naics_code, top_category, sub_category
      FROM "safegraph"."core_poi"
      WHERE region = 'NY'
    ) b  
    ON a.safegraph_place_id=b.safegraph_place_id
GROUP BY a.date_current,
         SUBSTR(a.poi_cbg,1,5),
         SUBSTR(b.naics_code,1,3)
"""

aws.execute_query(
    query=query, 
    database="safegraph", 
    output="output/poi/daily_borough_poivisits_by_subsector.csv"
)