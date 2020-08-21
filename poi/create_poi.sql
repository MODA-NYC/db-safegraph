SET client_encoding = ‘UTF8’;
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
    poi_cbg VARCHAR(12),
    visitor_home_cbgs json,
    visitor_daytime_cbgs json,
    visitor_country_of_origin json,
    distance_from_home int,
    median_dwell numeric,
    bucketed_dwell_times json,
    related_same_day_brand json,
    related_same_week_brand json,
    --popularity_by_hour json,
    --popularity_by_day json,
    device_type json
    --carrier_name json
);

\COPY tmp FROM pstdin WITH NULL AS '' DELIMITER ',' CSV HEADER;

CREATE SCHEMA IF NOT EXISTS poi;
DROP TABLE IF EXISTS poi.:"DATE";
SELECT * INTO poi.:"DATE" FROM tmp;

CREATE INDEX :IDX ON poi.:"DATE" (safegraph_place_id);