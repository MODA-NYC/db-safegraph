CREATE TEMP TABLE tmp (
    safegraph_place_id text,
    location_name text,
    street_address text,
    city text,
    region VARCHAR(2),
    postal_code VARCHAR(5),
    iso_country_code VARCHAR(2),
    safegraph_brand_ids text,
    brands text,
    date_range_start timestamp,
    date_range_end timestamp,
    raw_visit_counts int,
    raw_visitor_counts int,
    visits_by_day json,
    visits_by_each_hour json,
    poi_cbg VARCHAR(12),
    visitor_home_cbgs json,
    visitor_daytime_cbgs json,
    visitor_country_of_origin json,
    distance_from_home text,
    median_dwell numeric,
    bucketed_dwell_times json,
    related_same_day_brand json,
    related_same_week_brand json,
    device_type json
);

\COPY tmp FROM pstdin WITH NULL AS '' DELIMITER ',' CSV HEADER;

CREATE SCHEMA IF NOT EXISTS weekly_patterns;
DROP TABLE IF EXISTS weekly_patterns.:"DATE";
SELECT 
    safegraph_place_id,
    location_name,
    street_address,
    city,
    region,
    postal_code,
    iso_country_code,
    safegraph_brand_ids,
    brands,
    date_range_start,
    date_range_end,
    raw_visit_counts,
    raw_visitor_counts,
    visits_by_day,
    visits_by_each_hour,
    poi_cbg,
    visitor_home_cbgs,
    visitor_daytime_cbgs,
    (CASE
        WHEN visitor_country_of_origin = '' THEN '{}'::json
        ELSE visitor_country_of_origin::json
    END) as visitor_country_of_origin,
    (CASE
        WHEN distance_from_home = '' THEN 0
        ELSE distance_from_home::int
    END) as distance_from_home,
    median_dwell,
    bucketed_dwell_times,
    related_same_day_brand,
    related_same_week_brand,
    device_type
INTO weekly_patterns.:"DATE" FROM tmp;